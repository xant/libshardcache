#include <fbuf.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <errno.h>
#include <iomux.h>
#include <queue.h>
#include <linklist.h>
#include <bsd_queue.h>
#include <hashtable.h>
#include <inttypes.h>

#include "messaging.h"
#include "connections.h"
#include "shardcache.h"
#include "counters.h"

#include "serving.h"

#include "shardcache_internal.h" // for the replica memeber

#ifdef USE_PACKED_STRUCTURES
#pragma pack(push, 1)
#endif
typedef struct {
    pthread_t thread;
    queue_t *jobs;
    int leave;
    pthread_cond_t wakeup_cond;
    pthread_mutex_t wakeup_lock;
    shardcache_serving_t *serv;
    iomux_t *iomux;
    linked_list_t *prune;
    uint64_t numfds;
    int id;
    //uint64_t pruning;
} shardcache_worker_context_t;

struct _shardcache_serving_s {
    shardcache_t *cache;
    int sock;
    pthread_t io_thread;
    iomux_t *io_mux;
    int leave;
    unsigned int num_workers;
    unsigned int next_worker_index;
    linked_list_t *workers;
    uint64_t num_connections;
    uint64_t total_workers;
};

typedef struct _shardcache_connection_context_s shardcache_connection_context_t;

#define SHARDCACHE_REQUEST_RECORDS_MAX 5

typedef struct _shardcache_request_s {
    fbuf_t records[SHARDCACHE_REQUEST_RECORDS_MAX];
    int fd;
    shardcache_hdr_t hdr;
    shardcache_connection_context_t *ctx;
#ifdef __MACH__
    OSSpinLock output_lock;
#else
    pthread_spinlock_t output_lock;
#endif
    fbuf_t output;
    int error;
    int skipped;
    int copied;
    int done;
    fbuf_t fetch_accumulator;
    TAILQ_ENTRY(_shardcache_request_s) next;
} shardcache_request_t;

typedef struct {
    shardcache_request_t *req;
    int is_multi;
    union {
        struct {
            void *key;
            size_t klen;
        } single;
        struct {
            void **keys;
            size_t *klens;
            int num_keys;
            void **values;
            size_t *vlens;
            int num_values;
        } multi;
    };
} shardcache_get_async_ctx_t;

struct _shardcache_connection_context_s {
    shardcache_hdr_t hdr;

    TAILQ_HEAD (, _shardcache_request_s) requests;
    int num_requests;

    fbuf_t records[SHARDCACHE_REQUEST_RECORDS_MAX];

    shardcache_serving_t *serv;

    int fd;
    async_read_ctx_t *reader_ctx;
    int retries;
    struct timeval retry_timeout;
    shardcache_worker_context_t *worker;
    int closed;
    struct timeval in_prune_since;
};
#ifdef USE_PACKED_STRUCTURES
#pragma pack(pop)
#endif

static int
async_read_handler(void *data, size_t len, int idx, size_t total_len, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;


    if (idx >= 0 && idx < SHARDCACHE_REQUEST_RECORDS_MAX)
        fbuf_add_binary(&ctx->records[idx], data, len);

    // idx == -1 means that reading finished
    // idx == -2 means error
    // any idx >= 0 refers to the record index
    return (idx >= -1) ? 0 : -1;
}


static shardcache_connection_context_t *
shardcache_connection_context_create(shardcache_serving_t *serv, int fd)
{
    shardcache_connection_context_t *ctx =
        calloc(1, sizeof(shardcache_connection_context_t));

    ctx->serv = serv;
    ctx->fd = fd;
    ctx->reader_ctx = async_read_context_create(async_read_handler, ctx);
    TAILQ_INIT(&ctx->requests);

    int i;
    for (i = 0; i < SHARDCACHE_REQUEST_RECORDS_MAX; i++) {
        fbuf_minlen(&ctx->records[i], 64);
        fbuf_fastgrowsize(&ctx->records[i], 1024);
        fbuf_slowgrowsize(&ctx->records[i], 512);
    }
    ATOMIC_INCREMENT(serv->num_connections);
    return ctx;
}

static void
shardcache_request_destroy(shardcache_request_t *req)
{
    int i;
    for (i = 0; i < SHARDCACHE_REQUEST_RECORDS_MAX; i++) {
        fbuf_destroy(&req->records[i]);
    }
    SPIN_DESTROY(req->output_lock);
    fbuf_destroy(&req->output);
    fbuf_destroy(&req->fetch_accumulator);
    free(req);
}

static void
shardcache_connection_context_destroy(shardcache_connection_context_t *ctx)
{
    int i;
    for (i = 0; i < SHARDCACHE_REQUEST_RECORDS_MAX; i++) {
        fbuf_destroy(&ctx->records[i]);
    }
    shardcache_request_t *req = TAILQ_FIRST(&ctx->requests);
    while(req) {
        TAILQ_REMOVE(&ctx->requests, req, next);
        shardcache_request_destroy(req);
        ctx->num_requests--;
        req = TAILQ_FIRST(&ctx->requests);
    }
    async_read_context_destroy(ctx->reader_ctx);
    ATOMIC_DECREMENT(ctx->serv->num_connections);
    free(ctx);
}

static inline void
send_data(shardcache_request_t *req, fbuf_t *data)
{
    SPIN_LOCK(req->output_lock);
    fbuf_concat(&req->output, data);
    SPIN_UNLOCK(req->output_lock);
}

static void
write_statuses(shardcache_request_t *req, char mode, int num_items, ...)
{
    fbuf_t out = FBUF_STATIC_INITIALIZER;

    fbuf_t **items = malloc(sizeof(fbuf_t *) * num_items);
    va_list arg;
    va_start(arg, num_items);
    int i;
    for (i = 0; i < num_items; i++) {
        int rc = va_arg(arg, int);
        items[i] = fbuf_create(0);
        char st = rc_to_status(rc, mode); 
        fbuf_add_binary(items[i], &st, 1);
    }
    va_end(arg);

    array_to_record(num_items, items, &out);

    for (i = 0; i < num_items; i++)
        fbuf_free(items[i]);

    free(items);
}

static void
write_status(shardcache_request_t *req, char mode, int rc)
{
    // we are ensured that req exists until done is set to 1 and that
    // both req->output and req->ctx will never change, so we don't need a lock here
    char out[6] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    int no_data = 0;

    char version = async_read_context_protocol_version(req->ctx->reader_ctx);

    if (version < 2)
        out[1] = 1;
    else
        out[3] = 1;

    if (UNLIKELY(req->hdr == SHC_HDR_GET ||
                 req->hdr == SHC_HDR_GET_ASYNC ||
                 req->hdr == SHC_HDR_GET_OFFSET))
    {
        if (version < 2)
            out[1] = 0;
        else
            out[3] = 0;
        no_data = 1;
    } else {
        if (version < 2)
            out[2] = rc_to_status(rc, mode);
        else
            out[4] = rc_to_status(rc, mode);
    }

    // NOTE: we will respond using the same protocol version used by the client
    uint32_t magic = htonl((SHC_MAGIC & 0xFFFFFF00) | version);
    fbuf_t output = FBUF_STATIC_INITIALIZER;
    fbuf_minlen(&output, 64);
    fbuf_fastgrowsize(&output, 1024);
    fbuf_slowgrowsize(&output, 512);

    fbuf_add_binary(&output, (char *)&magic, sizeof(magic));

    unsigned char hdr = SHC_HDR_RESPONSE;

    fbuf_add_binary(&output, (char *)&hdr, 1);

    int delta = (version < 1) ? 3 : 1;
    fbuf_add_binary(&output, out, sizeof(out) - (no_data ? delta : 0));

    send_data(req, &output);
    fbuf_destroy(&output);

    ATOMIC_INCREMENT(req->done);
}

static inline int
send_async_data_response_preamble(shardcache_request_t *req, uint32_t total_size)
{
    shardcache_hdr_t hdr = SHC_HDR_RESPONSE;

    char version = async_read_context_protocol_version(req->ctx->reader_ctx);
    uint32_t magic = htonl((SHC_MAGIC & 0xFFFFFF00) | version);

    fbuf_t output = FBUF_STATIC_INITIALIZER;
    fbuf_minlen(&output, 64);
    fbuf_fastgrowsize(&output, 1024);
    fbuf_slowgrowsize(&output, 512);
    fbuf_add_binary(&output, (char *)&magic, sizeof(magic));

    fbuf_add_binary(&output, (void *)&hdr, 1);

    if (version > 1) {
        uint32_t size = htonl(total_size);
        fbuf_add_binary(&output, (char *)&size, sizeof(uint32_t));
    }

    send_data(req, &output);
    fbuf_destroy(&output);

    return 0;
}

static inline int
send_async_data_response_epilogue(shardcache_request_t *req, char status)
{
    uint16_t eor = 0;
    char eom = SHARDCACHE_EOM;
    char version = async_read_context_protocol_version(req->ctx->reader_ctx);
    // NOTE: From protocol version 2 responses to get/offset commands are terminated
    //       with a third record containing a status code (so allowing to distinguish
    //       between not-found/empty-data and underlying errors happening at the
    //       cache/storage level.
    //       In protocol version1 no status code was available because the response
    //       contained exactly one record holding the data.
    char rsep = version == 1 ? eom : SHARDCACHE_RSEP;
    fbuf_t output = FBUF_STATIC_INITIALIZER;
    fbuf_minlen(&output, 64);
    fbuf_fastgrowsize(&output, 1024);
    fbuf_slowgrowsize(&output, 512);

    if (version < 2)
        fbuf_add_binary(&output, (void *)&eor, 2);

    fbuf_add_binary(&output, &rsep, 1);

    // read above about the third record in get/offset responses
    // introduced from protocol version 2
    if (version > 1) {
        uint32_t status_size = htonl(1);
        fbuf_add_binary(&output, (void *)&status_size, sizeof(status_size));
        fbuf_add_binary(&output, (void *)&status, 1);
        fbuf_add_binary(&output, &eom, 1);
    }

    send_data(req, &output);
    fbuf_destroy(&output);

    ATOMIC_INCREMENT(req->done);
    return 0;
}

static int
send_async_multi_data_response(shardcache_get_async_ctx_t *ctx)
{
    int i;
    fbuf_t *items[ctx->multi.num_keys];

    for (i = 0; i < ctx->multi.num_keys; i++) {
        items[i] = fbuf_create(0);
        fbuf_attach(items[i], ctx->multi.values[i], ctx->multi.vlens[i], ctx->multi.vlens[i]);
    }

    fbuf_t out = FBUF_STATIC_INITIALIZER;

    array_to_record(ctx->multi.num_keys, items, &out);

    return 0;
}


static inline shardcache_get_async_ctx_t *
get_async_ctx_create(shardcache_request_t *req, void **keys, size_t *klens, int num_keys)
{
    shardcache_get_async_ctx_t *ctx = malloc(sizeof(shardcache_get_async_ctx_t));
    ctx->req = req;

    if (num_keys > 1) {
        ctx->is_multi = 1;
        ctx->multi.keys = (void **)keys;
        ctx->multi.klens = klens;
        ctx->multi.num_keys = num_keys;
        ctx->multi.values = malloc(sizeof(void *) * num_keys);
        ctx->multi.vlens = malloc(sizeof(size_t) * num_keys);
        ctx->multi.num_values = 0;
    } else {
        ctx->is_multi = 0;
        ctx->single.key = malloc(klens[0]);
        memcpy(ctx->single.key, keys[0], klens[0]);
        ctx->single.klen = klens[0];
    }
    return ctx;
}

static inline void
get_async_ctx_destroy(shardcache_get_async_ctx_t *ctx)
{
    if (ctx->is_multi) {
        int i;
        for (i = 0; i < ctx->multi.num_keys; i++) {
            if (ctx->multi.keys[i])
                free(ctx->multi.keys[i]);
        }
        free(ctx->multi.keys);
        free(ctx->multi.klens);
        for (i = 0; i < ctx->multi.num_keys; i++) {
            if (ctx->multi.values[i])
                free(ctx->multi.values[i]);
        }
        free(ctx->multi.values);
        free(ctx->multi.vlens);
    } else {
        free(ctx->single.key);
    }
    free(ctx);
}

static int
get_async_multi_data_handler(void *key,
                             size_t klen,
                             void *data,
                             size_t dlen,
                             size_t total_size,
                             struct timeval *timestamp,
                             void *priv)
{
    shardcache_get_async_ctx_t *ctx = (shardcache_get_async_ctx_t *)priv;

    shardcache_request_t *req = (shardcache_request_t *)ctx->req;

    if (dlen == 0 && total_size == 0) {
        if (!timestamp) { // Error
            ATOMIC_INCREMENT(req->error);
            get_async_ctx_destroy(ctx);
            return -1;
        }

        ctx->multi.num_values++;

        if (ctx->multi.num_values == ctx->multi.num_keys) {
            if (send_async_multi_data_response(ctx) != 0) {
                ATOMIC_INCREMENT(req->error);
                get_async_ctx_destroy(ctx);
                return -1;
            }
            get_async_ctx_destroy(ctx);
        }
    }

    if (UNLIKELY(!dlen))
        return 0;

    int i;
    for (i = 0; i < ctx->multi.num_keys; i++) {
        if (klen == ctx->multi.klens[i] &&
            memcmp(key, ctx->multi.keys[i], klen) == 0)
        {
            ctx->multi.values[i] = realloc(ctx->multi.values[i], ctx->multi.vlens[i] + dlen);
            memcpy(ctx->multi.values[i] + ctx->multi.vlens[i], data, dlen);
            ctx->multi.vlens[i] += dlen;
        }
    }

    return 0;

}

static int
get_async_data_handler(void *key,
                       size_t klen,
                       void *data,
                       size_t dlen,
                       size_t total_size,
                       struct timeval *timestamp,
                       void *priv)
{

    shardcache_get_async_ctx_t *ctx = (shardcache_get_async_ctx_t *)priv;

    shardcache_request_t *req = (shardcache_request_t *)ctx->req;

    if (req->skipped == 0 && req->copied == 0) {
        uint32_t record_size = total_size;
        if (req->hdr == SHC_HDR_GET_OFFSET) {
            memcpy(&record_size, fbuf_data(&req->records[2]), sizeof(uint32_t));
            record_size = ntohl(record_size);
        }


        if (send_async_data_response_preamble(req, record_size) != 0) {
            ATOMIC_INCREMENT(req->error);
            get_async_ctx_destroy(ctx);
            return -1;
        }
    }

    if (dlen == 0 && total_size == 0) {
        char status = SHC_RES_OK;
        if (!timestamp && (req->skipped || req->copied)) {
            // if there is no timestamp here it means there was an
            // error (and not just an empty item)
            SHC_ERROR("Error notified to the get_async_data callback");
            status = SHC_RES_ERR;
        }

        get_async_ctx_destroy(ctx);

        if (send_async_data_response_epilogue(req, status) != 0) {
            ATOMIC_INCREMENT(req->error);
            return -1;
        }

        return !timestamp ? -1 : 0;
    }

    char version = async_read_context_protocol_version(req->ctx->reader_ctx);

    uint16_t accumulated_size = fbuf_used(&req->fetch_accumulator);

    if (version < 2) {
        static int max_chunk_size = (1<<16)-1;

        size_t to_process = accumulated_size + dlen;
        size_t data_offset = 0;
        while(to_process >= max_chunk_size) {
            fbuf_t output = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 64, 1024, 512);
            size_t copy_size = max_chunk_size;

            uint16_t clen = htons((uint16_t)copy_size);

            fbuf_add_binary(&output, (void *)&clen, sizeof(clen));

            if (accumulated_size) {
                int copied = fbuf_concat(&output, &req->fetch_accumulator);
                copy_size -= copied;
                accumulated_size -= copied;
                fbuf_remove(&req->fetch_accumulator, copied);
            }
            if (dlen - data_offset >= copy_size) {
                fbuf_add_binary(&output, data + data_offset, copy_size);
                data_offset += copy_size;
                req->copied += copy_size;
            }

            if (fbuf_used(&output))
                send_data(req, &output);

            fbuf_destroy(&output);
            to_process = accumulated_size + (dlen - data_offset);
        }

        if (dlen > data_offset) {
            int remainder = dlen - data_offset;
            if (remainder) {
                fbuf_add_binary(&req->fetch_accumulator, data + data_offset, remainder);
                accumulated_size = remainder;
                req->copied += remainder;
            }
        }
    } else {
            fbuf_t output = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 64, 1024, 512);
            fbuf_add_binary(&output, data, dlen);
            if (fbuf_used(&output))
                send_data(req, &output);
            fbuf_destroy(&output);
            req->copied += dlen;
    }

    if (total_size > 0 && timestamp) {
        if (accumulated_size) { // NOTE : if accumulated_size is positive we know it's protocol v1
            fbuf_t output = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 64, 1024, 512);
            // flush what we have left in the accumulator
            uint16_t clen = htons(accumulated_size);
            fbuf_add_binary(&output, (void *)&clen, sizeof(clen));
            int copied = fbuf_concat(&output, &req->fetch_accumulator);
            if (copied)
                fbuf_remove(&req->fetch_accumulator, copied);

            send_data(req, &output);
            fbuf_destroy(&output);
        }

        if (req->hdr == SHC_HDR_GET_OFFSET) {
            fbuf_t output = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 6, 6, 6);
            // flush what we have left in the accumulator
            char rsep = SHARDCACHE_RSEP;
            fbuf_add_binary(&output, &rsep, 1);
            uint32_t rsize = htonl(sizeof(uint32_t));
            fbuf_add_binary(&output, (void *)&rsize, sizeof(rsize));
            uint32_t rlen = htonl(total_size - req->copied);
            fbuf_add_binary(&output, (void *)&rlen, sizeof(rlen));
            send_data(req, &output);
            fbuf_destroy(&output);
        }

        if (send_async_data_response_epilogue(req, SHC_RES_OK) != 0) {
            ATOMIC_INCREMENT(req->error);
            get_async_ctx_destroy(ctx);
            return -1;
        }

        ATOMIC_INCREMENT(req->done);
    }

    return 0;
}

static int
get_async_data(shardcache_t *cache,
               void *key,
               size_t klen,
               shardcache_get_async_callback_t cb,
               shardcache_request_t *req)
{
    int rc;
    req->copied = 0;
    req->skipped = 0;

    shardcache_get_async_ctx_t *ctx = get_async_ctx_create(req, &key, &klen, 1);

    if (req->hdr == SHC_HDR_GET_OFFSET) {
        uint32_t offset = ntohl(*((uint32_t *)fbuf_data(&req->records[1])));
        uint32_t length = ntohl(*((uint32_t *)fbuf_data(&req->records[2])));
        rc = shardcache_get_offset(cache, key, klen, offset, length, cb, ctx);
    } else {
        rc = shardcache_get(cache, key, klen, cb, ctx);
    }
    if (rc != 0) {
        SHC_ERROR("shardcache_get_async returned error");
        // XXX - at the moment the protocol doesn't allow to distinguish
        //       between an empty value and an error, so for now we choose
        //       to return a valid response for an empty value instead of
        //       silently shutdown the connection.
        //ATOMIC_INCREMENT(req->error);
        get_async_ctx_destroy(ctx);
        send_async_data_response_preamble(req, 0);
        if (send_async_data_response_epilogue(req, SHC_RES_ERR) != 0) {
            ATOMIC_INCREMENT(req->error);
            return -1;
        }
    }

    return rc;
}

static void
shardcache_async_command_response(void *key, size_t klen, int64_t ret, void *priv)
{
    shardcache_request_t *req = (shardcache_request_t *)priv;

    char version = async_read_context_protocol_version(req->ctx->reader_ctx);
    if (req->hdr == SHC_HDR_INCREMENT || req->hdr == SHC_HDR_DECREMENT) {
        // build the response to the increment/decrement command

        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        fbuf_printf(&buf, "%"PRIi64, ret);
        fbuf_t out = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 64, 1024, 512);
        shardcache_record_t record = {
            .v = fbuf_data(&buf),
            .l = fbuf_used(&buf)
        };
        if (build_message(SHC_HDR_RESPONSE,
                          &record, 1, &out, version) == 0)
        {
            send_data(req, &out);
            ATOMIC_INCREMENT(req->done);
        } else {
            SHC_ERROR("Can't build the INCR/DECR response");
            write_status(req, WRITE_STATUS_MODE_SIMPLE, 0);
        }
        fbuf_destroy(&buf);
        fbuf_destroy(&out);
    } else {
        int mode = (req->hdr == SHC_HDR_ADD)
                 ? WRITE_STATUS_MODE_EXISTS
                 : (req->hdr == SHC_HDR_EXISTS)
                    ? WRITE_STATUS_MODE_BOOLEAN
                    : WRITE_STATUS_MODE_SIMPLE;

        write_status(req, mode, ret);
    }
}

static void
process_request(shardcache_request_t *req)
{

    shardcache_t *cache = req->ctx->serv->cache; //XXX

    int rc = 0;
    void *key = fbuf_data(&req->records[0]);
    size_t klen = fbuf_used(&req->records[0]);

    char version = async_read_context_protocol_version(req->ctx->reader_ctx);

    switch(req->hdr) {
        case SHC_HDR_GET:
        case SHC_HDR_GET_ASYNC:
        case SHC_HDR_GET_OFFSET:
        {
            if (req->hdr == SHC_HDR_GET_OFFSET) {
                if (fbuf_used(&req->records[1]) != sizeof(uint32_t) ||
                    fbuf_used(&req->records[2]) != sizeof(uint32_t))
                {
                    SHC_WARNING("Bad record format for message GET_OFFSET");
                    send_async_data_response_preamble(req, 0);
                    if (send_async_data_response_epilogue(req, SHC_RES_ERR) != 0)
                        ATOMIC_INCREMENT(req->error);

                    break;
                }
            }

            get_async_data(cache, key, klen, get_async_data_handler, req);
            break;
        }
        case SHC_HDR_ADD:
        case SHC_HDR_SET:
        {
            uint32_t expire = 0;
            uint32_t cexpire = 0;
            if (fbuf_used(&req->records[2]) == sizeof(uint32_t)) {
                memcpy(&expire, fbuf_data(&req->records[2]), sizeof(uint32_t));
                expire = ntohl(expire);
            }
            if (fbuf_used(&req->records[3]) == sizeof(uint32_t)) {
                memcpy(&cexpire, fbuf_data(&req->records[2]), sizeof(uint32_t));
                cexpire = ntohl(cexpire);
            }
            shardcache_set(cache, key, klen,
                           fbuf_data(&req->records[1]),
                           fbuf_used(&req->records[1]),
                           expire,
                           cexpire,
                           req->hdr == SHC_HDR_SET ? 0 : 1,
                           shardcache_async_command_response,
                           req);
            break;
        }
        case SHC_HDR_EXISTS:
        {
            shardcache_exists(cache, key, klen, shardcache_async_command_response, req);
            break;
        }
        case SHC_HDR_TOUCH:
        {
            rc = shardcache_touch(cache, key, klen);
            write_status(req, WRITE_STATUS_MODE_SIMPLE, rc);
            break;
        }
        case SHC_HDR_DELETE:
        {
            shardcache_del(cache, key, klen, shardcache_async_command_response, req);
            break;
        }
        case SHC_HDR_EVICT:
        {
            shardcache_evict(cache, key, klen);
            write_status(req, WRITE_STATUS_MODE_SIMPLE, 0);
            break;
        }
        case SHC_HDR_EVICT_MULTI:
        {
            uint32_t num_items = *((uint32_t *)key);
            char *p = ((char *)key) + sizeof(uint32_t);
            while (num_items-- > 0) {
                uint32_t ks = *((uint32_t *)p);
                p += ks;
                shardcache_evict(cache, (void *)p, ks);
            }
            write_status(req, WRITE_STATUS_MODE_SIMPLE, 0);
            break;
        }
        case SHC_HDR_CAS:
        {
            if (!fbuf_used(&req->records[2])) {
                // CAS command requires at least 3 arguments (key, old_value, new_value)
                // TODO - maybe a NULL new value could mean 'unset if equals' ?
                SHC_WARNING("CAS command didn't contain enough records");
                write_status(req, WRITE_STATUS_MODE_SIMPLE, -1);
                break;
            }
            uint32_t expire = 0;
            uint32_t cexpire = 0;
            if (fbuf_used(&req->records[3]) == sizeof(uint32_t)) {
                memcpy(&expire, fbuf_data(&req->records[2]), sizeof(uint32_t));
                expire = ntohl(expire);
            }
            if (fbuf_used(&req->records[4]) == sizeof(uint32_t)) {
                memcpy(&cexpire, fbuf_data(&req->records[2]), sizeof(uint32_t));
                cexpire = ntohl(cexpire);
            }
 
            int rc = shardcache_cas(cache, key, klen,
                                    fbuf_data(&req->records[1]),
                                    fbuf_used(&req->records[1]),
                                    fbuf_data(&req->records[2]),
                                    fbuf_used(&req->records[2]),
                                    expire,
                                    cexpire,
                                    shardcache_async_command_response,
                                    req);
 
            write_status(req, WRITE_STATUS_MODE_EXISTS, rc);
            break;
        }
        case SHC_HDR_INCREMENT:
        case SHC_HDR_DECREMENT:
        {
            // convert the amounts from the decimal string representation to the int64_t used internally
            int64_t amount = 0;
            int64_t initial = 0;
            if (fbuf_used(&req->records[1]))
                amount = strtoll(fbuf_data(&req->records[1]), NULL, 10);
            if (fbuf_used(&req->records[2]))
                initial = strtoll(fbuf_data(&req->records[2]), NULL, 10);

            uint32_t expire = 0;
            uint32_t cexpire = 0;
            if (fbuf_used(&req->records[3]) == sizeof(uint32_t)) {
                memcpy(&expire, fbuf_data(&req->records[2]), sizeof(uint32_t));
                expire = ntohl(expire);
            }
            if (fbuf_used(&req->records[4]) == sizeof(uint32_t)) {
                memcpy(&cexpire, fbuf_data(&req->records[2]), sizeof(uint32_t));
                cexpire = ntohl(cexpire);
            }

            if (req->hdr == SHC_HDR_INCREMENT)
                shardcache_increment(cache, key, klen, amount, initial, expire, cexpire, NULL, shardcache_async_command_response, req);
            else
                shardcache_decrement(cache, key, klen, amount, initial, expire, cexpire, NULL, shardcache_async_command_response, req);

            break;
        }
        case SHC_HDR_GET_MULTI:
        {
            char **keys = NULL;
            size_t *lens = NULL;
            uint32_t num_keys = record_to_array(&req->records[0], &keys, &lens);
            

            shardcache_get_async_ctx_t *ctx = get_async_ctx_create(req, (void **)keys, lens, num_keys);
                
            int rc = shardcache_get_multi(cache,
                                          (void **)keys,
                                          lens,
                                          num_keys,
                                          get_async_multi_data_handler,
                                          ctx);

            if (rc != 0) {
                get_async_ctx_destroy(ctx);
                fbuf_t out = FBUF_STATIC_INITIALIZER;
                array_to_record(0, NULL, &out);
                write_statuses(req, WRITE_STATUS_MODE_SIMPLE, 1, -1);
            }
            break;
        }
        case SHC_HDR_SET_MULTI:
        {
            // TODO - IMPLEMENT
            write_statuses(req, WRITE_STATUS_MODE_SIMPLE, 1, -1);
            break;
        }
        case SHC_HDR_DELETE_MULTI:
        {
            // TODO - IMPLEMENT
            write_statuses(req, 1, WRITE_STATUS_MODE_SIMPLE, -1);
            break;
        }
        case SHC_HDR_MIGRATION_BEGIN:
        {
            int num_shards = 0;
            shardcache_node_t **nodes = NULL;
            char *s = (char *)fbuf_data(&req->records[0]);
            while (s && *s) {
                char *tok = strsep(&s, ",");
                if(tok) {
                    char *label = strsep(&tok, ":");
                    char *addr = tok;
                    size_t size = (num_shards + 1) * sizeof(shardcache_node_t *);
                    nodes = realloc(nodes, size);
                    shardcache_node_t *node = shardcache_node_create(label, &addr, 1);
                    nodes[num_shards++] = node;
                }
            }
            rc = shardcache_migration_begin(cache, nodes, num_shards, 0);
            if (rc != 0)
                SHC_WARNING("Can't begin the migration");
            int i;
            for (i = 0; i < num_shards; i++)
                shardcache_node_destroy(nodes[i]);
            free(nodes);
            write_status(req, WRITE_STATUS_MODE_SIMPLE, 0);
            break;
        }
        case SHC_HDR_MIGRATION_ABORT:
        {
            rc = shardcache_migration_abort(cache);
            if (rc != 0)
                SHC_WARNING("Can't abort the migration");
            write_status(req, WRITE_STATUS_MODE_SIMPLE, rc);
            break;
        }
        case SHC_HDR_MIGRATION_END:
        {
            rc = shardcache_migration_end(cache);
            if (rc != 0)
                SHC_WARNING("Can't end the migration");
            write_status(req, WRITE_STATUS_MODE_SIMPLE, rc);
            break;
        }
        case SHC_HDR_CHECK:
        {
            // TODO - HEALTH CHECK
            write_status(req, WRITE_STATUS_MODE_SIMPLE, 0);
            break;
        }
        case SHC_HDR_STATS:
        {
            fbuf_t buf = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 64, 1024, 512);

            fbuf_printf(&buf, "libshardcache_version;%s\r\n", LIBSHARDCACHE_VERSION);
            fbuf_printf(&buf, "libshardcache_build_info;%s\r\n", LIBSHARDCACHE_BUILD_INFO);

            shardcache_counter_t *counters = NULL;
            int i, num_nodes;

            shardcache_node_t **nodes = shardcache_get_nodes(cache, &num_nodes);
            if (nodes) {
                fbuf_printf(&buf, "num_nodes;%d\r\nnodes;", num_nodes);
                for (i = 0; i < num_nodes; i++) {
                    if (i > 0)
                        fbuf_add(&buf, ",");
                    fbuf_printf(&buf, "%s", shardcache_node_get_string(nodes[i]));
                }
                fbuf_add(&buf, "\r\n");
                shardcache_free_nodes(nodes, num_nodes);
            }

            int ncounters = shardcache_get_counters(cache, &counters);
            if (counters) {
                for (i = 0; i < ncounters; i++) {
                    fbuf_printf(&buf, "%s;%llu\r\n",
                                counters[i].name, counters[i].value);
                }

                fbuf_t out = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 64, 1024, 512);
                shardcache_record_t record = {
                    .v = fbuf_data(&buf),
                    .l = fbuf_used(&buf)
                };
                if (build_message(SHC_HDR_RESPONSE,
                                  &record, 1, &out, version) == 0)
                {
                    send_data(req, &out);
                    ATOMIC_INCREMENT(req->done);
                } else {
                    SHC_ERROR("Can't build the STATS response");
                    write_status(req, WRITE_STATUS_MODE_SIMPLE, -1);
                }
                fbuf_destroy(&out);
                free(counters);
            }
            fbuf_destroy(&buf);
            break;
        }
        case SHC_HDR_GET_INDEX:
        {
            fbuf_t buf = FBUF_STATIC_INITIALIZER;
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            SHC_DEBUG("Fetching index");
            shardcache_storage_index_t *index = shardcache_get_index(cache);
            SHC_DEBUG("Index got");
            if (index) {
                int i;
                for (i = 0; i < index->size; i++) {
                    uint32_t klen = (uint32_t)index->items[i].klen;
                    uint32_t vlen = (uint32_t)index->items[i].vlen;
                    void *key = index->items[i].key;
                    uint32_t nklen = htonl(klen);
                    uint32_t nvlen = htonl(vlen);
                    fbuf_add_binary(&buf, (char *)&nklen, sizeof(nklen));
                    fbuf_add_binary(&buf, key, klen);
                    fbuf_add_binary(&buf, (char *)&nvlen, sizeof(nvlen));
                }
                size_t zero = 0;
                // no klen terminates the list
                fbuf_add_binary(&buf, (char *)&zero, sizeof(zero));

                shardcache_free_index(index);
            }

            // chunkize the data and build an actual message
            shardcache_record_t record = {
                .v = fbuf_data(&buf),
                .l = fbuf_used(&buf)
            };
            if (build_message(SHC_HDR_INDEX_RESPONSE, &record, 1, &out, version) == 0)
            {
                // destroy it early ... since we still need one more copy
                SHC_DEBUG("Index response sent (%d)", fbuf_used(&out));
                send_data(req, &out);
                ATOMIC_INCREMENT(req->done);
            } else {
                write_status(req, WRITE_STATUS_MODE_SIMPLE, -1);
                SHC_ERROR("Can't build the index response");
            }
            fbuf_destroy(&out);
            fbuf_destroy(&buf);
            break;
        }
        case SHC_HDR_REPLICA_COMMAND:
        case SHC_HDR_REPLICA_PING:
        {
            void *response = NULL;
            size_t response_len = 0;
            if (!cache->replica) {
                write_status(req, WRITE_STATUS_MODE_SIMPLE, -1);
                break;
            }

            shardcache_hdr_t rhdr =
                shardcache_replica_received_command(cache->replica,
                                                    req->hdr,
                                                    fbuf_data(&req->records[0]),
                                                    fbuf_used(&req->records[0]),
                                                    &response,
                                                    &response_len);
            if (response_len) {
                fbuf_t out = FBUF_STATIC_INITIALIZER_PARAMS(FBUF_MAXLEN_NONE, 64, 1024, 512);

                shardcache_record_t record = {
                    .v = response,
                    .l = response_len
                };
                if (build_message(rhdr, &record, 1, &out, version) == 0)
                {
                    // destroy it early ... since we still need one more copy
                    free(response);
                    send_data(req, &out);
                    ATOMIC_INCREMENT(req->done);
                } else {
                    free(response);
                    SHC_ERROR("Can't build the REPLICA command response");
                    write_status(req, WRITE_STATUS_MODE_SIMPLE, -1);
                }
                fbuf_destroy(&out);
            } else {
                write_status(req, WRITE_STATUS_MODE_SIMPLE, rc);
            }
            break;
        }
        default:
            fprintf(stderr, "Unsupported command: 0x%02x\n", (char)req->hdr);
            write_status(req, WRITE_STATUS_MODE_SIMPLE, -1);
            break;
    }
}


static void * worker(void *priv);

static shardcache_worker_context_t *
shardcache_select_worker(shardcache_serving_t *serv)
{
    if (ATOMIC_READ(serv->leave))
        return NULL;

    shardcache_worker_context_t *wrk = list_pick_value(serv->workers,
            __sync_fetch_and_add(&serv->next_worker_index, 1)%list_count(serv->workers));

    return wrk;
}

shardcache_request_t *
shardcache_request_create(shardcache_connection_context_t *ctx)
{
    shardcache_request_t *req = calloc(1, sizeof(shardcache_request_t));
    req->hdr = async_read_context_hdr(ctx->reader_ctx);
    req->ctx = ctx;
    SPIN_INIT(req->output_lock);

    int i;
    for (i = 0; i < SHARDCACHE_REQUEST_RECORDS_MAX; i++) {
        FBUF_STATIC_INITIALIZER_POINTER(&req->records[i], FBUF_MAXLEN_NONE, 64, 1024, 512);
        char *buf = NULL;
        int len = 0;
        int used = fbuf_detach(&ctx->records[i], &buf, &len);
        if (buf)
            fbuf_attach(&req->records[i], buf, len, used);
    }

    FBUF_STATIC_INITIALIZER_POINTER(&req->fetch_accumulator, FBUF_MAXLEN_NONE, 64, 1024, 512);
    FBUF_STATIC_INITIALIZER_POINTER(&req->output, FBUF_MAXLEN_NONE, 64, 1024, 512);

    return req;
}

static int shardcache_output_handler(iomux_t *iomux, int fd, unsigned char **out, int *len, void *priv);

static inline int
shardcache_check_context_state(iomux_t *iomux,
                               int fd,
                               shardcache_connection_context_t *ctx,
                               async_read_context_state_t state)
{
    if (state == SHC_STATE_READING_DONE) {
        // create a new request
        ctx->retries = 0;
        shardcache_request_t *req = shardcache_request_create(ctx);
        TAILQ_INSERT_TAIL(&ctx->requests, req, next);
        ctx->num_requests++;
        process_request(req);
        iomux_set_output_callback(iomux, fd, shardcache_output_handler);
    }
    else if (UNLIKELY(state == SHC_STATE_READING_ERR))
    {
        // if the asynchronous reader is in error state we want
        // to close the connection, probably an unauthorized or a
        // badly formatted message has been sent by the client
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);
        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
        SHC_WARNING("Bad message %02x from %s (%d)",
                    ctx->hdr, inet_ntoa(saddr.sin_addr), state);
        return -1;
    }
    return 0;
}


static int
shardcache_output_handler(iomux_t *iomux, int fd, unsigned char **out, int *len, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    *len = 0;

    shardcache_request_t *req = TAILQ_FIRST(&ctx->requests);

    if (req) {
        if (UNLIKELY(ATOMIC_READ(req->error))) {
            // abort the request and close the connection
            // if there was an error while fetching a remote object
            if (!iomux_close(iomux, fd)) {
                close(fd);
                shardcache_connection_context_destroy(ctx);
            }
            return IOMUX_OUTPUT_MODE_NONE;
        }

        int done = ATOMIC_READ(req->done);

        SPIN_LOCK(req->output_lock);
        if (fbuf_used(&req->output))
            *len = fbuf_detach(&req->output, (char **)out, NULL);
        SPIN_UNLOCK(req->output_lock);

        if (done) {
            TAILQ_REMOVE(&ctx->requests, req, next);
            ctx->num_requests--;
            shardcache_request_destroy(req);
            // if we have pending input data this is time
            // to process it and move to the next request
            int state = async_read_context_update(ctx->reader_ctx);
            if (shardcache_check_context_state(iomux, fd, ctx, state) != 0) {
                iomux_close(iomux, fd);
                *len = 0;
            }
        }
    } else {
        iomux_unset_output_callback(iomux, fd);
    }
    return IOMUX_OUTPUT_MODE_FREE;
}

static int
shardcache_input_handler(iomux_t *iomux,
                         int fd,
                         unsigned char *data,
                         int len,
                         void *priv)
{
    int processed = 0;

    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;


    if (ctx) {
        if (ctx->num_requests > ctx->serv->cache->serving_look_ahead) {
            SHC_DEBUG2("Too many pipelined requests, waiting");
            return 0;
        }

        async_read_context_state_t state =
            async_read_context_input_data(ctx->reader_ctx, data, len, &processed);

        // updating the context state might eventually push a new requeset
        // (if entirely dowloaded) to a worker
        if (shardcache_check_context_state(iomux, fd, ctx, state) != 0) {
            iomux_close(iomux, fd);
        }
    }

    return processed;
}

static void
shardcache_eof_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    close(fd);

    if (ctx) {
        if (TAILQ_FIRST(&ctx->requests) != NULL) {
            ctx->closed = 1;
            gettimeofday(&ctx->in_prune_since, NULL);
            list_push_value(ctx->worker->prune, ctx);
            //ATOMIC_INCREMENT(ctx->worker->pruning);
            return;
        }
        shardcache_connection_context_destroy(ctx);
    }
}

static void
shardcache_connection_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_serving_t *serv = (shardcache_serving_t *)priv;

    if (!ATOMIC_READ(serv->leave)) {
        shardcache_worker_context_t *wrkctx = shardcache_select_worker(serv);
        if (wrkctx) {
            shardcache_connection_context_t *ctx =
                shardcache_connection_context_create(serv, fd);

            ctx->worker = wrkctx;
            if (queue_push_right(wrkctx->jobs, ctx) != 0) {
                close(fd);
                SHC_WARNING("Can't push the new job to the worker queue");
                return;
            }
        } else {
            close(fd);
            SHC_WARNING("Can't find any usable worker to handle the new connection");
        }
    }
}


static void *
worker(void *priv)
{
    shardcache_worker_context_t *wrkctx = (shardcache_worker_context_t *)priv;
    queue_t *jobs = wrkctx->jobs;

    shardcache_thread_init(wrkctx->serv->cache);

    while (ATOMIC_READ(wrkctx->leave) == 0) {
        shardcache_connection_context_t *ctx = queue_pop_left(jobs);
        while(ctx) {
            iomux_callbacks_t connection_callbacks = {
                .mux_connection = NULL,
                .mux_input = shardcache_input_handler,
                .mux_output = NULL,
                .mux_eof = shardcache_eof_handler,
                .priv = ctx
            };
            if (!iomux_add(wrkctx->iomux, ctx->fd, &connection_callbacks)) {
                close(ctx->fd);
                shardcache_connection_context_destroy(ctx);
            }
            ctx = queue_pop_left(jobs);
        }


        int timeout = ATOMIC_READ(wrkctx->serv->cache->iomux_run_timeout_low);
        struct timeval tv = { timeout/1e6, timeout%(int)1e6 };
        iomux_run(wrkctx->iomux, &tv);

        int to_check = list_count(wrkctx->prune);
        while (to_check--) {
            shardcache_connection_context_t *to_prune = list_shift_value(wrkctx->prune);
            //ATOMIC_DECREMENT(wrkctx->pruning);
            shardcache_request_t *req = TAILQ_FIRST(&to_prune->requests);
            int done = 0;
            if (req) {
                if (ATOMIC_READ(req->done)) {
                    // the request is served, we can destroy it
                    TAILQ_REMOVE(&to_prune->requests, req, next);
                    shardcache_request_destroy(req);
                    done = (TAILQ_FIRST(&to_prune->requests) == NULL);
                }
            }
            struct timeval quarantine = { 60, 0 };
            struct timeval now, diff;
            gettimeofday(&now, NULL);
            timersub(&now, &to_prune->in_prune_since, &diff);
            if (done || timercmp(&diff, &quarantine, >)) {
                shardcache_connection_context_destroy(to_prune);
            } else {
                list_push_value(wrkctx->prune, to_prune);
                //ATOMIC_INCREMENT(wrkctx->pruning);
            }
        }

        ATOMIC_SET(wrkctx->numfds, iomux_num_fds(wrkctx->iomux));

        if (iomux_isempty(wrkctx->iomux)) {
            // we don't have any filedescriptor to handle in the mux,
            // let's sit for 1 second waiting for the listener thread to wake
            // us up if new filedescriptors arrive
            struct timespec abstime;
            struct timeval now;

            int rc = gettimeofday(&now, NULL);
            if (rc == 0) {
                struct timeval wait_time = { 0, 250000 };
                timeradd(&now, &wait_time, &wait_time);
                abstime.tv_sec = wait_time.tv_sec;
                abstime.tv_nsec = (wait_time.tv_usec * 1000);
                CONDITION_TIMEDWAIT(wrkctx->wakeup_cond,
                                    wrkctx->wakeup_lock,
                                    &abstime);

            }
        }
    }

    shardcache_thread_end(wrkctx->serv->cache);
    return NULL;
}

void *
serve_cache(void *priv)
{
    shardcache_serving_t *serv = (shardcache_serving_t *)priv;

    SHC_NOTICE("Listening on %s (num_workers: %d)",
               serv->cache->addr, serv->num_workers);

    if (listen(serv->sock, -1) != 0) {
        SHC_ERROR("Error listening on fd %d: %s",
                  serv->sock, strerror(errno));
        return NULL;
    }

    iomux_callbacks_t connection_callbacks = {
        .mux_connection = shardcache_connection_handler,
        .mux_input = NULL,
        .mux_eof = NULL,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = serv
    };

    if (!iomux_add(serv->io_mux, serv->sock, &connection_callbacks)) {
        SHC_ERROR("Can't add the listening socket to the mux");
        return NULL;
    }
    iomux_listen(serv->io_mux, serv->sock);

    while (!ATOMIC_READ(serv->leave)) {
        int timeout = ATOMIC_READ(serv->cache->iomux_run_timeout_high);
        struct timeval tv = { timeout/1e6, timeout%(int)1e6 };
        iomux_run(serv->io_mux, &tv);
    }

    return NULL;
}

static inline void
create_serving_worker(shardcache_serving_t *s, int id)
{
    shardcache_worker_context_t *wrk = calloc(1, sizeof(shardcache_worker_context_t));
    wrk->id = id;
    wrk->serv = s;
    wrk->jobs = queue_create();
    queue_set_free_value_callback(wrk->jobs,
            (queue_free_value_callback_t)shardcache_connection_context_destroy);
    wrk->prune = list_create();
    list_set_free_value_callback(wrk->prune, (free_value_callback_t)shardcache_connection_context_destroy);

    char label[64];
    snprintf(label, sizeof(label), "worker[%d].numfds", id);
    shardcache_counter_add(s->cache->counters, label, &wrk->numfds);

    MUTEX_INIT(wrk->wakeup_lock);
    CONDITION_INIT(wrk->wakeup_cond);
    wrk->iomux = iomux_create(1<<13, 0);
    pthread_create(&wrk->thread, NULL, worker, wrk);
    list_push_value(s->workers, wrk);
    ATOMIC_INCREMENT(s->total_workers);
}

shardcache_serving_t *
start_serving(shardcache_t *cache, unsigned int num_workers)
{
    shardcache_serving_t *s = calloc(1, sizeof(shardcache_serving_t));
    s->cache = cache;
    s->num_workers = num_workers;

    // open the listening socket
    char *brkt = NULL;
    char *addr = strdup(cache->addr); // we need a temporary copy to be used by strtok
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : SHARDCACHE_PORT_DEFAULT;

    s->sock = open_socket(host, port);
    if (s->sock == -1) {
        fprintf(stderr, "Can't open listening socket %s:%d : %s\n",
                host, port, strerror(errno));
        free(addr);
        free(s);
        return NULL;
    }

    free(addr); // we don't need it anymore

    // create the workers' pool
    s->workers = list_create();

    if (cache->counters) {
        shardcache_counter_add(cache->counters, "connections", &s->num_connections);
        shardcache_counter_add(cache->counters, "num_workers", &s->total_workers);
    }

    int i;
    for (i = 0; i < num_workers; i++)
        create_serving_worker(s, i);

    s->io_mux = iomux_create(0, 0);

    // and start a background thread to handle incoming connections
    int rc = pthread_create(&s->io_thread, NULL, serve_cache, s);
    if (rc != 0) {
        fprintf(stderr, "Can't create new thread: %s\n", strerror(errno));
        stop_serving(s);
        return NULL;
    }

    return s;
}

static inline void
destroy_serving_worker(shardcache_worker_context_t *wrk)
{
    ATOMIC_INCREMENT(wrk->leave);

    // wake up the worker if it is slacking
    CONDITION_SIGNAL(wrk->wakeup_cond, wrk->wakeup_lock);

    pthread_join(wrk->thread, NULL);

    queue_destroy(wrk->jobs);

    MUTEX_DESTROY(wrk->wakeup_lock);
    CONDITION_DESTROY(wrk->wakeup_cond);
    SHC_DEBUG3("Worker thread %p exited", wrk);

    shardcache_connection_context_t *ctx = list_shift_value(wrk->prune);
    while (ctx) {
        shardcache_request_t *req = TAILQ_FIRST(&ctx->requests);
        while (req) {
            TAILQ_REMOVE(&ctx->requests, req, next);
            ctx->num_requests--;
            shardcache_request_destroy(req);
            req = TAILQ_FIRST(&ctx->requests);
        }
        shardcache_connection_context_destroy(ctx);
        ctx = list_shift_value(wrk->prune);
    }

    iomux_destroy(wrk->iomux);

    list_destroy(wrk->prune);

    char label[64];
    snprintf(label, sizeof(label), "worker[%d].numfds", wrk->id);
    shardcache_counter_remove(wrk->serv->cache->counters, label);

    free(wrk);
}

int
configure_serving_workers(shardcache_serving_t *s, unsigned int num_workers)
{
    static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    MUTEX_LOCK(lock);
    if (s->num_workers == num_workers) {
        MUTEX_UNLOCK(lock);
        return 0;
    }

    if (num_workers > s->num_workers) {
        int i;
        for (i = s->num_workers; i < num_workers; i++)
            create_serving_worker(s, i);
    } else {
        while (list_count(s->workers) > num_workers) {
            shardcache_worker_context_t *wrk = list_shift_value(s->workers);
            // NOTE : this will close all connections currently handled by the
            // worker being destroyed
            destroy_serving_worker(wrk);
            ATOMIC_DECREMENT(s->total_workers);
        }
    }

    int ret = num_workers - s->num_workers;
    s->num_workers = num_workers;

    MUTEX_UNLOCK(lock);
    return ret;
}

static void
clear_workers_list(linked_list_t *list)
{
    shardcache_worker_context_t *wrk = list_shift_value(list);
    while (wrk) {
        destroy_serving_worker(wrk);
        wrk = list_shift_value(list);
    }
}

void
stop_serving(shardcache_serving_t *s)
{
    ATOMIC_INCREMENT(s->leave);

    iomux_remove(s->io_mux, s->sock);
    close(s->sock);

    // now the workers
    SHC_NOTICE("Collecting worker threads (might have to wait until i/o is finished)");
    clear_workers_list(s->workers);
    SHC_DEBUG2("All worker threads have been collected");

    // unregister our counters if we did at creation time
    if (s->cache->counters) {
        shardcache_counter_remove(s->cache->counters, "connections");
        shardcache_counter_remove(s->cache->counters, "num_workers");
    }

    pthread_join(s->io_thread, NULL);

    iomux_destroy(s->io_mux);
    list_destroy(s->workers);

    free(s);
}

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
