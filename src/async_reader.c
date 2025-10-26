#include <stdlib.h>
#include <strings.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "shardcache.h"
#include "async_reader.h"
#include "messaging.h"

#ifdef USE_PACKED_STRUCTURES
#define PACK_IF_NECESSARY __attribute__((packed))
#else
#define PACK_IF_NECESSARY
#endif

struct _async_read_ctx_s {
    async_read_callback_t cb;
    shardcache_hdr_t hdr;
    void *cb_priv;
    rbuf_t *buf;
    char chunk[65536];
    uint16_t clen;
    uint16_t coff;
    uint32_t rlen;
    int rnum;
    char state;
    char magic[4];
    char version;
    int moff;
    struct timeval last_update;
} PACK_IF_NECESSARY;

int
async_read_context_state(async_read_ctx_t *ctx)
{
    return ctx->state;
}

shardcache_hdr_t
async_read_context_hdr(async_read_ctx_t *ctx)
{
    return ctx->hdr;
}

char
async_read_context_protocol_version(async_read_ctx_t *ctx)
{
    return ctx->version;
}

static inline int
async_read_move_to_next_record(async_read_ctx_t *ctx)
{
    if (rbuf_used(ctx->buf) < 1) {
        // TRUNCATED - we need more data
        ctx->state = SHC_STATE_READING_RSEP;
        return 1;
    }

    u_char bsep = 0;
    rbuf_read(ctx->buf, &bsep, 1);

    if (bsep == SHARDCACHE_RSEP) {
        ctx->state = SHC_STATE_READING_RECORD;
        if (ctx->cb && ctx->cb(NULL, 0, ctx->rnum, ctx->rlen, ctx->cb_priv) != 0)
        {
            ctx->state = SHC_STATE_READING_ERR;
            if (ctx->cb)
                ctx->cb(NULL, 0, -2, ctx->rlen, ctx->cb_priv);
            return -1;
        }
        ctx->rnum++;
        ctx->rlen = 0;
    } else if (bsep == 0) {
        ctx->state = SHC_STATE_READING_DONE;
        return 1;
    } else {
        ctx->state = SHC_STATE_READING_ERR;
        if (ctx->cb)
            ctx->cb(NULL, 0, -2, ctx->rlen, ctx->cb_priv);
        return -1;
    }
    return 0;
}

static inline void
async_read_parse_protocol_v1(async_read_ctx_t *ctx)
{
    for (;;) {

        if (ctx->coff == ctx->clen && ctx->state == SHC_STATE_READING_RECORD) {
            if (rbuf_used(ctx->buf) < 2)
                break;

            // let's call the read_async callback
            if (ctx->clen > 0 && ctx->cb && ctx->cb(ctx->chunk, ctx->clen, ctx->rnum, ctx->clen, ctx->cb_priv) != 0)
            {
                ctx->state = SHC_STATE_READING_ERR;
                if (ctx->cb)
                    ctx->cb(NULL, 0, -2, ctx->clen, ctx->cb_priv);
                return;
            } 

            uint16_t nlen = 0;
            rbuf_read(ctx->buf, (u_char *)&nlen, 2);
            ctx->clen = ntohs(nlen);
            ctx->rlen += ctx->clen;
            ctx->coff = 0;
        }
        if (ctx->clen > ctx->coff) {
            int rb = rbuf_read(ctx->buf, (u_char *)ctx->chunk + ctx->coff, ctx->clen - ctx->coff);
            ctx->coff += rb;
            if (!rbuf_used(ctx->buf))
                break; // TRUNCATED - we need more data
        } else {
            if (async_read_move_to_next_record(ctx) != 0)
                break;

        }
    }
}

static inline void
async_read_parse_protocol_v2(async_read_ctx_t *ctx)
{
    for (;;) {

        if (ctx->coff == ctx->rlen && ctx->state == SHC_STATE_READING_RECORD) {
            if (ctx->rlen > 0 && async_read_move_to_next_record(ctx) != 0)
                break;

            if (rbuf_used(ctx->buf) < 4)
                break;

            uint32_t rlen = 0;
            rbuf_read(ctx->buf, (u_char *)&rlen, 4);

            // if this is an empty record, move directly to the next
            if (rlen == 0 && async_read_move_to_next_record(ctx) != 0)
                break;

            ctx->rlen = ntohl(rlen);
            ctx->clen = (uint16_t)ctx->rlen; // XXX
            ctx->coff = 0;
        }
        if (ctx->rlen > ctx->coff) {
            int rb = rbuf_read(ctx->buf, (u_char *)ctx->chunk + ctx->coff, ctx->rlen - ctx->coff);
            // let's call the read_async callback
            if (ctx->cb && ctx->cb(ctx->chunk + ctx->coff, rb, ctx->rnum, ctx->rlen, ctx->cb_priv) != 0)
            {
                ctx->state = SHC_STATE_READING_ERR;
                if (ctx->cb)
                    ctx->cb(NULL, 0, -2, ctx->rlen, ctx->cb_priv);
                break;
            } 
            ctx->coff += rb;

            if (!rbuf_used(ctx->buf))
                break; // TRUNCATED - we need more data
        }
    }
}

async_read_context_state_t
async_read_context_update(async_read_ctx_t *ctx)
{
    gettimeofday(&ctx->last_update, NULL);

    if (__builtin_expect(ctx->state == SHC_STATE_READING_DONE, 0))
    {
        ctx->state = SHC_STATE_READING_NONE;
        ctx->rnum = 0;
        ctx->rlen = 0;
        ctx->moff = 0;
        ctx->version = 0;
        ctx->clen = 0;
        ctx->coff = 0;
        memset(ctx->magic, 0, sizeof(ctx->magic));
    }

    if (!rbuf_used(ctx->buf))
        return ctx->state;

    if (ctx->state == SHC_STATE_READING_NONE)
    {
        ctx->hdr = 0;
        unsigned char byte;
        rbuf_read(ctx->buf, &byte, 1);
        while (byte == SHC_HDR_NOOP && rbuf_used(ctx->buf) > 0)
            rbuf_read(ctx->buf, &byte, 1); // skip

        if (byte == SHC_HDR_NOOP && !rbuf_used(ctx->buf))
            return ctx->state;

        ctx->magic[0] = byte;
        ctx->state = SHC_STATE_READING_MAGIC;
        ctx->moff = 1;
    }

    if (ctx->state == SHC_STATE_READING_MAGIC) {
        if (rbuf_used(ctx->buf) < sizeof(uint32_t) - ctx->moff) {
            return ctx->state;
        }

        rbuf_read(ctx->buf, (u_char *)&ctx->magic[ctx->moff], sizeof(uint32_t) - ctx->moff);
        uint32_t rmagic;
        memcpy((char *)&rmagic, ctx->magic, sizeof(uint32_t));
        if ((ntohl(rmagic)&0xFFFFFF00) != (SHC_MAGIC&0xFFFFFF00)) {
            ctx->state = SHC_STATE_READING_ERR;
            if (ctx->cb)
                ctx->cb(NULL, 0, -2, ctx->clen, ctx->cb_priv);
            return ctx->state;
        }
        ctx->version = ctx->magic[3];
        if (ctx->version > SHC_PROTOCOL_VERSION) {
            SHC_WARNING("Unsupported protocol version %02x", ctx->version);
            ctx->state = SHC_STATE_READING_ERR;
            if (ctx->cb)
                ctx->cb(NULL, 0, -2, ctx->clen, ctx->cb_priv);
            return ctx->state;
        }

        ctx->state = SHC_STATE_READING_HDR;
    }

    if (ctx->state == SHC_STATE_READING_HDR)
    {
        if (rbuf_used(ctx->buf) < 1)
            return ctx->state;
        rbuf_read(ctx->buf, (unsigned char *)&ctx->hdr, 1);

        ctx->state = SHC_STATE_READING_RECORD;
    }

    if (ctx->state == SHC_STATE_READING_RECORD) {
        if (ctx->version < 2)
            async_read_parse_protocol_v1(ctx);
        else
            async_read_parse_protocol_v2(ctx);
    }


    if (ctx->state == SHC_STATE_READING_DONE) {
        if (ctx->cb && ctx->cb(NULL, 0, -1, ctx->clen, ctx->cb_priv) != 0) {
            ctx->state = SHC_STATE_READING_ERR;
            if (ctx->cb)
                ctx->cb(NULL, 0, -2, ctx->clen, ctx->cb_priv);
            return ctx->state;
        }
    }
    return ctx->state;
}

async_read_context_state_t
async_read_context_consume_data(async_read_ctx_t *ctx, rbuf_t *in)
{
    int used_bytes = rbuf_move(in, ctx->buf, rbuf_used(in));
    if (used_bytes)
        return async_read_context_update(ctx);
    return ctx->state;
}

async_read_context_state_t
async_read_context_input_data(async_read_ctx_t *ctx, void *data, int len, int *processed)
{
    int used_bytes = rbuf_write(ctx->buf, data, len);
    if (used_bytes)
        async_read_context_update(ctx);
    if (processed)
        *processed = used_bytes;
    return ctx->state;
}

async_read_ctx_t *
async_read_context_create(async_read_callback_t cb,
                          void *priv)
{
    async_read_ctx_t *ctx = calloc(1, sizeof(async_read_ctx_t));
    ctx->buf = rbuf_create(1<<16);
    ctx->cb = cb;
    ctx->cb_priv = priv;
    gettimeofday(&ctx->last_update, NULL);
    return ctx;
}

void
async_read_context_destroy(async_read_ctx_t *ctx)
{
    rbuf_destroy(ctx->buf);
    free(ctx);
}

int
read_async_input_data(iomux_t *iomux, int fd, unsigned char *data, int len, void *priv)
{
    async_read_ctx_t *ctx = (async_read_ctx_t *)priv;
    int processed = 0;
    async_read_context_state_t state = async_read_context_input_data(ctx, data, len, &processed);

    int close = (state == SHC_STATE_READING_DONE ||
                 state == SHC_STATE_READING_NONE ||
                 state == SHC_STATE_READING_ERR);

    if (state == SHC_STATE_READING_ERR) {
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);
        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
        fprintf(stderr, "Bad message %02x from %s\n", ctx->hdr, inet_ntoa(saddr.sin_addr));
    }

    if (close)
        iomux_close(iomux, fd);

    return len;
}

void
read_async_input_eof(iomux_t *iomux, int fd, void *priv)
{
    async_read_ctx_t *ctx = (async_read_ctx_t *)priv;

    if (ctx->state != SHC_STATE_READING_DONE)
        ctx->cb(NULL, 0, -2, ctx->clen, ctx->cb_priv);

    ctx->cb(NULL, 0, -3, ctx->clen, ctx->cb_priv);

    async_read_context_destroy(ctx);
}

void
read_async_timeout(iomux_t *iomux, int fd, void *priv)
{
    async_read_ctx_t *ctx = (async_read_ctx_t *)priv;
    int tcp_timeout = global_tcp_timeout(-1);
    struct timeval maxwait = { tcp_timeout / 1000, (tcp_timeout % 1000) * 1000 };
    struct timeval now, diff;
    gettimeofday(&now, NULL);
    timersub(&now, &ctx->last_update, &diff);
    if (timercmp(&diff, &maxwait, >)) {
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);
        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
        SHC_WARNING("Timeout while waiting for data from %s (timeout: %d milliseconds)",
                    inet_ntoa(saddr.sin_addr), tcp_timeout);
        iomux_close(iomux, fd);
    } else { 
        iomux_set_timeout(iomux, fd, &maxwait);
    }
}

