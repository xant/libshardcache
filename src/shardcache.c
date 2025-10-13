#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <signal.h>
#include <fcntl.h>
#include <limits.h>
#include <dlfcn.h>
#include <ctype.h>
#include <inttypes.h>

#include "shardcache.h"
#include "shardcache_internal.h"
#include "arc_ops.h"
#include "connections.h"
#include "messaging.h"
#include "shardcache_replica.h"

#ifndef BUILD_INFO
#define BUILD_INFO
#endif

#define STRINGIFY(x)   #x
#define X_STRINGIFY(x) STRINGIFY(x)

const char *LIBSHARDCACHE_VERSION = "1.1";
const char *LIBSHARDCACHE_BUILD_INFO = X_STRINGIFY(BUILD_INFO);

extern int shardcache_log_initialized;
extern unsigned int shardcache_loglevel;


static int
shardcache_test_ownership_internal(shardcache_t *cache,
                                   void *key,
                                   size_t klen,
                                   char *owner,
                                   size_t *len,
                                   int  migration)
{
    const char *node_name;
    size_t name_len = 0;

    if (len && *len == 0)
        return -1;

    if (cache->num_shards == 1)
        return 1;

    SPIN_LOCK(cache->migration_lock);

    chash_t *continuum = NULL;
    if (cache->migration && cache->migration_done) { 
        shardcache_migration_end(cache);
    } 

    if (migration) {
        if (cache->migration) {
            continuum = cache->migration;
        } else {
            SPIN_UNLOCK(cache->migration_lock);
            return -1;
        }
    } else {
        continuum = cache->chash;
    }

    chash_lookup(continuum, key, klen, &node_name, &name_len);
    if (owner) {
        if (len && name_len + 1 > *len)
            name_len = *len - 1;
        memcpy(owner, node_name, name_len);
        owner[name_len] = 0;
    }
    if (len)
        *len = name_len;

    SPIN_UNLOCK(cache->migration_lock);
    return (strcmp(owner, cache->me) == 0);
}

int
shardcache_test_migration_ownership(shardcache_t *cache,
                                    void *key,
                                    size_t klen,
                                    char *owner,
                                    size_t *len)
{
    int ret = shardcache_test_ownership_internal(cache, key, klen, owner, len, 1);
    return ret;
}

int
shardcache_test_ownership(shardcache_t *cache,
                          void *key,
                          size_t klen,
                          char *owner,
                          size_t *len)
{
    return shardcache_test_ownership_internal(cache, key, klen, owner, len, 0);
}

int
shardcache_get_connection_for_peer(shardcache_t *cache, char *peer)
{
    if (!ATOMIC_READ(cache->use_persistent_connections))
        return connect_to_peer(peer, cache->tcp_timeout);

    // this will reuse an available filedescriptor already connected to peer
    // or create a new connection if there isn't any available
    return connections_pool_get(cache->connections_pool, peer);
}

void
shardcache_release_connection_for_peer(shardcache_t *cache, char *peer, int fd)
{
    if (fd < 0)
        return;

    if (!ATOMIC_READ(cache->use_persistent_connections)) {
        close(fd);
        return;
    }
    // put back the fildescriptor into the connection cache
    connections_pool_add(cache->connections_pool, peer, fd);
}

static void
shardcache_do_nothing(int sig)
{
    // do_nothing
}

typedef struct {
    void *key;
    size_t klen;
} shardcache_key_t;

typedef struct {
    shardcache_t *cache;
    shardcache_key_t item;
    int is_volatile;
} expire_key_ctx_t;

typedef shardcache_key_t shardcache_evictor_job_t;

static void
destroy_evictor_job(shardcache_evictor_job_t *job)
{
    free(job->key);
    free(job);
}

static
shardcache_evictor_job_t *create_evictor_job(void *key, size_t klen)
{
    shardcache_evictor_job_t *job = malloc(sizeof(shardcache_evictor_job_t)); 
    job->key = malloc(klen);
    memcpy(job->key, key, klen);
    job->klen = klen; 
    return job;
}

static int
evict_key(hashtable_t *table, void *value, size_t vlen, void *user)
{
    shardcache_evictor_job_t **job = (shardcache_evictor_job_t **)user;
    *job = (shardcache_evictor_job_t *)value;
    // remove the value from the table and stop the iteration 
    // and since there is no free value callback the job won't
    // be released on removal 
    return -2;
}

static inline void
shardcache_update_size_counters(shardcache_t *cache)
{
    ATOMIC_CAS(cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
               ATOMIC_READ(cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value),
               ATOMIC_READ(*cache->arc_lists_size[0]) +
               ATOMIC_READ(*cache->arc_lists_size[1]) +
               ATOMIC_READ(*cache->arc_lists_size[2]) +
               ATOMIC_READ(*cache->arc_lists_size[3]));
}


static void *
evictor(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    hashtable_t *jobs = cache->evictor_jobs;

    connections_pool_t *connections = connections_pool_create(ATOMIC_READ(cache->tcp_timeout),
                                                              SHARDCACHE_CONNECTION_EXPIRE_DEFAULT,
                                                              1);

    while (!ATOMIC_READ(cache->quit))
    {

        // this will extract only the first value
        shardcache_evictor_job_t *job = NULL;
        ht_foreach_value(jobs, evict_key, &job);
        if (job) {

            SHC_DEBUG2("Eviction job for key '%.*s' started", job->klen, job->key);

            int i;
            for (i = 0; i < cache->num_shards; i++) {
                char *peer = shardcache_node_get_label(cache->shards[i]);
                if (strcmp(peer, cache->me) != 0) {
                    SHC_DEBUG3("Sending Eviction command to %s", peer);
                    int rindex = random()%shardcache_node_num_addresses(cache->shards[i]);
                    char *addr = shardcache_node_get_address_at_index(cache->shards[i], rindex);
                    int fd = connections_pool_get(connections, addr);
                    if (fd < 0)
                        break;

                    int retries = 0;
                    for (;;) {
                        int flags = fcntl(fd, F_GETFL, 0);
                        if (flags == -1) {
                            close(fd);
                            fd = connections_pool_get(connections, addr);
                            if (fd < 0)
                                break;
                            continue;
                        }

                        flags |= O_NONBLOCK;
                        fcntl(fd, F_SETFL, flags);

                        int rb = 0;
                        do {
                            char discard[1<<16];
                            rb = read(fd, &discard, sizeof(discard));
                        } while (rb > 0);

                        if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                            close(fd);
                            fd = connections_pool_get(connections, addr);
                            if (fd < 0)
                                break;
                            if (retries++ < 5)
                                continue;
                        }
                        flags &= ~O_NONBLOCK;
                        fcntl(fd, F_SETFL, flags);
                        break;
                    }

                    int rc = evict_from_peer(addr, job->key, job->klen, fd, 0);
                    if (rc == 0) {
                        connections_pool_add(connections, addr, fd);
                    } else {
                        SHC_WARNING("evict_from_peer return %d for peer %s", rc, peer);
                        close(fd);
                    }
                }
            }

            SHC_DEBUG2("Eviction job for key '%.*s' completed", job->klen, job->key);
            destroy_evictor_job(job);
        }

        if (!ht_count(jobs)) {
            // if we have no more jobs to handle let's sleep a bit
            struct timeval now;
            int rc = 0;
            rc = gettimeofday(&now, NULL);
            if (rc == 0) {
                struct timespec abstime = { now.tv_sec + 1, now.tv_usec * 1000 };
                MUTEX_LOCK(cache->evictor_lock);
                pthread_cond_timedwait(&cache->evictor_cond, &cache->evictor_lock, &abstime);
                MUTEX_UNLOCK(cache->evictor_lock);
            } else {
                // TODO - Error messsages
            }
        }
        shardcache_update_size_counters(cache);
    }
    connections_pool_destroy(connections);
    return NULL;
}

static void
destroy_volatile(volatile_object_t *obj)
{
    free(obj->data);
    free(obj);
}

static void
shardcache_expire_key_cb(iomux_t *iomux, void *priv)
{
    expire_key_ctx_t *ctx = (expire_key_ctx_t *)priv;
    void *ptr = NULL;
    if (ctx->is_volatile) {
        ht_delete(ctx->cache->volatile_timeouts, ctx->item.key, ctx->item.klen, &ptr, NULL);
        if (!ptr)
            return;

        free(ptr);
        ht_delete(ctx->cache->volatile_storage, ctx->item.key, ctx->item.klen, &ptr, NULL);
        if (ptr) {
            volatile_object_t *prev = (volatile_object_t *)ptr;
            ATOMIC_DECREASE(ctx->cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                            prev->dlen);
            destroy_volatile(prev);
        }
    } else {
        ht_delete(ctx->cache->cache_timeouts, ctx->item.key, ctx->item.klen, &ptr, NULL);
        if (!ptr)
            return;
        free(ptr);
    }
    ATOMIC_INCREMENT(ctx->cache->cnt[SHARDCACHE_COUNTER_EXPIRES].value);
    arc_remove(ctx->cache->arc, (const void *)ctx->item.key, ctx->item.klen);
}

typedef struct {
    int cmd;
#define SHARDACHE_EXPIRE_SCHEDULE 1
#define SHARDACHE_EXPIRE_UNSCHEDULE 2
    void *key;
    size_t klen;
    time_t expire;
    int is_volatile;
} shardcache_expire_job_t;

static void
shardcache_expire_context_destroy(expire_key_ctx_t *ctx)
{
    free(ctx->item.key);
    free(ctx);
}

void *
shardcache_expire_keys(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    while (!ATOMIC_READ(cache->quit))
    {
        shardcache_expire_job_t *job = queue_pop_left(cache->expirer_queue);
        while (job) {
            if (job->cmd == SHARDACHE_EXPIRE_UNSCHEDULE) {
                void *ptr = NULL;
                hashtable_t *table = job->is_volatile ? cache->volatile_timeouts : cache->cache_timeouts;

                ht_delete(table, job->key, job->klen, &ptr, NULL);
                if (ptr) {
                    iomux_timeout_id_t *tid = (iomux_timeout_id_t *)ptr;
                    iomux_unschedule(cache->expirer_mux, *tid);
                    free(tid);
                } else {
                    // TODO - Error messages ?
                }
                free(job->key);
                free(job);
                job = queue_pop_left(cache->expirer_queue);
                continue;
            }

            expire_key_ctx_t *ctx = calloc(1, sizeof(expire_key_ctx_t));
            ctx->item.key = job->key;
            ctx->item.klen = job->klen;
            ctx->cache = cache;
            ctx->is_volatile = job->is_volatile;
            hashtable_t *table = job->is_volatile ? cache->volatile_timeouts : cache->cache_timeouts;
            struct timeval timeout = { job->expire, (int)random()%(int)1e6 };
            iomux_timeout_id_t *tid_ptr = NULL;
            void *prev = NULL;
            iomux_timeout_id_t tid = 0;
            ht_delete(table, job->key, job->klen, &prev, NULL);

            if (prev) {
                tid_ptr = (iomux_timeout_id_t *)prev;
                tid = iomux_reschedule(cache->expirer_mux,
                                       *tid_ptr,
                                       &timeout,
                                       shardcache_expire_key_cb,
                                       ctx,
                                       (iomux_timeout_free_context_cb)shardcache_expire_context_destroy);
                *tid_ptr = tid;
            } else {
                tid = iomux_schedule(cache->expirer_mux,
                                     &timeout, shardcache_expire_key_cb,
                                     ctx,
                                     (iomux_timeout_free_context_cb)shardcache_expire_context_destroy);
                tid_ptr = malloc(sizeof(iomux_timeout_id_t));
                *tid_ptr = tid;
            }
            if (tid && tid_ptr) {
                ht_set(table, job->key, job->klen, tid_ptr, sizeof(iomux_timeout_id_t));
            } else {
                free(tid_ptr);
                shardcache_expire_context_destroy(ctx);
                // TODO - Erro message
            }

            free(job);
            job = queue_pop_left(cache->expirer_queue);
        }

        struct timeval tv = { 1, 0 };
        iomux_run(cache->expirer_mux, &tv);
        shardcache_update_size_counters(cache);
    }
    return NULL;
}

void
shardcache_queue_async_read_wrk(shardcache_t *cache, async_read_wrk_t *wrk)
{
    queue_push_right(cache->async_context[ATOMIC_INCREASE(cache->async_index, 1) % cache->num_async].queue, wrk);
}

typedef struct {
    shardcache_t *cache;
    int index;
} shardcache_run_async_arg_t;

void *
shardcache_run_async(void *priv)
{
    shardcache_run_async_arg_t *arg = (shardcache_run_async_arg_t *)priv;
    shardcache_t *cache = arg->cache;
    iomux_t *async_mux = arg->cache->async_context[arg->index % cache->num_async].mux;
    queue_t *async_queue = arg->cache->async_context[arg->index % cache->num_async].queue;
    shardcache_thread_init(cache);
    while (!ATOMIC_READ(cache->async_quit)) {
        int timeout = ATOMIC_READ(cache->iomux_run_timeout_low);
        struct timeval tv = { timeout/1e6, timeout%(int)1e6 };
        iomux_run(async_mux, &tv);
        async_read_wrk_t *wrk = queue_pop_left(async_queue);
        while (wrk) {
            if (wrk->fd < 0 || !iomux_add(async_mux, wrk->fd, &wrk->cbs)) {
                 if (wrk->fd >= 0)
                     wrk->cbs.mux_eof(async_mux, wrk->fd, wrk->cbs.priv);
                 else
                     async_read_context_destroy(wrk->ctx);
            }
            int tcp_timeout = global_tcp_timeout(-1);
            struct timeval maxwait = { tcp_timeout / 1000, (tcp_timeout % 1000) * 1000 };
            iomux_set_timeout(async_mux, wrk->fd, &maxwait);
            free(wrk);
            wrk = queue_pop_left(async_queue);
        }
    }
    free(arg);
    shardcache_thread_end(cache);
    return NULL;
}

shardcache_t *
shardcache_create(char *me,
                  shardcache_node_t **nodes,
                  int nnodes,
                  shardcache_storage_t *st,
                  int num_workers,
                  int num_async,
                  size_t cache_size)
{
    int i, n;
    size_t shard_lens[nnodes];
    char *shard_names[nnodes];

    shardcache_t *cache = calloc(1, sizeof(shardcache_t));

    cache->evict_on_delete = 1;
    cache->use_persistent_connections = 1;
    cache->tcp_timeout = SHARDCACHE_TCP_TIMEOUT_DEFAULT;
    cache->expire_time = SHARDCACHE_EXPIRE_TIME_DEFAULT;
    cache->serving_look_ahead = SHARDCACHE_SERVING_LOOK_AHEAD_DEFAULT;
    cache->iomux_run_timeout_low = SHARDCACHE_IOMUX_RUN_TIMEOUT_LOW;
    cache->iomux_run_timeout_high = SHARDCACHE_IOMUX_RUN_TIMEOUT_HIGH;
    if (num_async > 0)
        cache->num_async = num_async;
    else if (num_async < 0)
        cache->num_async = ((num_workers > 1 ? num_workers -1 : 1) / 10) + 1; // 1 async thread for every 10 workers
    else
        cache->num_async = SHARDCACHE_ASYNC_THREADS_NUM_DEFAULT;

    SPIN_INIT(cache->migration_lock);

    if (st) {
        if (st->version != SHARDCACHE_STORAGE_API_VERSION) {
            SHC_ERROR("Storage module version mismatch: %u != %u", st->version, SHARDCACHE_STORAGE_API_VERSION);
            shardcache_destroy(cache);
            return NULL;
        }
        memcpy(&cache->storage, st, sizeof(cache->storage));
        cache->use_persistent_storage = 1;
    } else {
        SHC_NOTICE("No storage callbacks provided,"
                   "using only the internal volatile storage");
        cache->use_persistent_storage = 0;
    }


    cache->me = strdup(me);

    cache->ops.init    = arc_ops_init;
    cache->ops.fetch   = arc_ops_fetch;
    cache->ops.evict   = arc_ops_evict;
    cache->ops.store   = arc_ops_store;

    cache->ops.priv = cache;
    cache->shards = malloc(sizeof(shardcache_node_t *) * nnodes);
    int me_found = 0;
    int my_index = -1;
    for (i = 0; i < nnodes; i++) {
        shard_names[i] = shardcache_node_get_label(nodes[i]);
        shard_lens[i] = strlen(shard_names[i]);
        int num_replicas = shardcache_node_num_addresses(nodes[i]);
        char *replicas[num_replicas];
        shardcache_node_get_all_addresses(nodes[i], replicas, num_replicas);
        cache->shards[i] = shardcache_node_create(shard_names[i], replicas, num_replicas);
        char *label = shardcache_node_get_label(nodes[i]);
        if (strcmp(label, me) == 0) {
            me_found = 1;
            for (n = 0; n < num_replicas; n++) {
                char *address = shardcache_node_get_address_at_index(nodes[i], n);
                int fd = open_socket(address, 0);
                if (fd >= 0) {
                    SHC_DEBUG("Using address %s", address);
                    cache->addr = strdup(address);
                    my_index = n;
                    close(fd);
                    break;
                } else {
                    SHC_DEBUG("Skipping address %s", address);
                }
            }
        }
    }

    if (!cache->addr) {
        if (me_found) {
            SHC_ERROR("Can't bind any address configured for node %s", me);
        } else {
            SHC_ERROR("Can't find my address (%s) among the configured nodes", cache->me);
        }
        shardcache_destroy(cache);
        return NULL;
    }

    cache->num_shards = nnodes;

    cache->chash = chash_create((const char **)shard_names, shard_lens, cache->num_shards, 200);

    // we need to tell the arc subsystem how big are the cached objects (well ... at least the container struct
    // which is attached to each cached object to encapsulate its actual data and extra flags/members
    cache->arc = arc_create(&cache->ops, cache_size, sizeof(cached_object_t), cache->arc_lists_size, cache->arc_mode);
    cache->arc_size = cache_size;

    // check if there is already signal handler registered on SIGPIPE
    struct sigaction sa;
    if (sigaction(SIGPIPE, NULL, &sa) != 0) {
        SHC_ERROR("Can't check signal handlers: %s", strerror(errno)); 
        shardcache_destroy(cache);
        return NULL;
    }

    // if not we need to register one to handle writes/reads to disconnected sockets
    if (sa.sa_handler == NULL)
        signal(SIGPIPE, shardcache_do_nothing);

    const char *counters_names[SHARDCACHE_NUM_COUNTERS] = SHARDCACHE_COUNTER_LABELS_ARRAY;

    cache->counters = shardcache_init_counters();

    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        cache->cnt[i].name = counters_names[i];
        shardcache_counter_add(cache->counters, cache->cnt[i].name, &cache->cnt[i].value); 
    }

    shardcache_counter_add(cache->counters, "mru_size", (uint64_t *)cache->arc_lists_size[0]);
    shardcache_counter_add(cache->counters, "mfu_size", (uint64_t *)cache->arc_lists_size[1]);
    shardcache_counter_add(cache->counters, "mrug_size", (uint64_t *)cache->arc_lists_size[2]);
    shardcache_counter_add(cache->counters, "mfug_size", (uint64_t *)cache->arc_lists_size[3]);

    if (ATOMIC_READ(cache->evict_on_delete)) {
        MUTEX_INIT(cache->evictor_lock);
        CONDITION_INIT(cache->evictor_cond);
        cache->evictor_jobs = ht_create(128, 8192, NULL);
        pthread_create(&cache->evictor_th, NULL, evictor, cache);
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    srandom((unsigned)tv.tv_usec);

    cache->volatile_storage = ht_create(1<<16, 1<<20, (ht_free_item_callback_t)destroy_volatile);

    cache->connections_pool = connections_pool_create(cache->tcp_timeout,
                                                      SHARDCACHE_CONNECTION_EXPIRE_DEFAULT,
                                                      (num_workers/2)+ 1);

    global_tcp_timeout(ATOMIC_READ(cache->tcp_timeout));

    cache->async_context = calloc(1, sizeof(shardcache_async_io_context_t) * cache->num_async);

    for (i = 0; i < cache->num_async; i++) {
        cache ->async_context[i].queue = queue_create();
        queue_set_bpool_size(cache->async_context[i].queue, num_workers * 1024);
        cache->async_context[i].mux = iomux_create(1<<13, 0);
        shardcache_run_async_arg_t *arg = malloc(sizeof(shardcache_run_async_arg_t));
        arg->cache = cache;
        arg->index = i;
        if (pthread_create(&cache->async_context[i].io_th, NULL, shardcache_run_async, arg) != 0) {
            SHC_ERROR("Can't create the async i/o thread: %s", strerror(errno));
            shardcache_destroy(cache);
            return NULL;
        }
    }

    if (!shardcache_log_initialized)
        shardcache_log_init("libshardcache", LOG_WARNING);

    cache->cache_timeouts = ht_create(1<<16, 1<<20, (ht_free_item_callback_t)free);
    cache->volatile_timeouts = ht_create(1<<16, 1<<20, (ht_free_item_callback_t)free);
    cache->expirer_mux = iomux_create(0, 1);
    cache->expirer_queue = queue_create();
    pthread_create(&cache->expirer_th, NULL, shardcache_expire_keys, cache);

    // start the replica subsystem now
    // NOTE: this needs to happen after the cache has been fully initialized
    for (i = 0; i < nnodes; i++) {
        if (shardcache_node_num_addresses(nodes[i]) > 1 && my_index >= 0)
            cache->replica = shardcache_replica_create(cache, cache->shards[i], my_index, NULL);
    }

    cache->serv = start_serving(cache, num_workers);

    if (!cache->serv) {
        SHC_ERROR("Can't start the communication engine");
        shardcache_destroy(cache);
        return NULL;
    }

    return cache;
}

void
shardcache_destroy(shardcache_t *cache)
{
    int i;

    ATOMIC_INCREMENT(cache->quit);

    ATOMIC_INCREMENT(cache->async_quit);
    if (cache->async_context) { 
        for (i = 0; i < cache->num_async; i ++) {
            if (cache->async_context[i].mux) {
                SHC_DEBUG2("Stopping the async i/o thread");
                pthread_join(cache->async_context[i].io_th, NULL);
                SHC_DEBUG2("Async i/o thread stopped");
                iomux_clear(cache->async_context[i].mux);
            }
        }
    }

    if (cache->serv)
        stop_serving(cache->serv);

    if (cache->async_context) {
        // NOTE : should be destroyed only after
        //        the serving subsystem has been stopped
        for (i = 0; i < cache->num_async; i ++) {
            if (cache->async_context[i].queue) {
                async_read_wrk_t *wrk = queue_pop_left(cache->async_context[i].queue);
                while(wrk) {
                     if (wrk->fd >= 0)
                         wrk->cbs.mux_eof(cache->async_context[i].mux, wrk->fd, wrk->cbs.priv);
                     else
                         async_read_context_destroy(wrk->ctx);
                    free(wrk);
                    wrk = queue_pop_left(cache->async_context[i].queue);
                }
                queue_destroy(cache->async_context[i].queue);
            }
            if (cache->async_context[i].mux)
                iomux_destroy(cache->async_context[i].mux);
        }
        free(cache->async_context);
    }

    if (ATOMIC_READ(cache->evict_on_delete) && cache->evictor_jobs)
    {
        SHC_DEBUG2("Stopping evictor thread");
        pthread_join(cache->evictor_th, NULL);
        MUTEX_DESTROY(cache->evictor_lock);
        CONDITION_DESTROY(cache->evictor_cond);
        ht_set_free_item_callback(cache->evictor_jobs,
                (ht_free_item_callback_t)destroy_evictor_job);
        ht_destroy(cache->evictor_jobs);
        SHC_DEBUG2("Evictor thread stopped");
    }

    SPIN_LOCK(cache->migration_lock);
    if (cache->migration) {
        shardcache_migration_abort(cache);    
    }
    SPIN_UNLOCK(cache->migration_lock);
    SPIN_DESTROY(cache->migration_lock);

    if (cache->expirer_th) {
        SHC_DEBUG2("Stopping expirer thread");
        pthread_join(cache->expirer_th, NULL);
        SHC_DEBUG2("Expirer thread stopped");
    }

    if (cache->replica)
        shardcache_replica_destroy(cache->replica);

    if (cache->counters) {
        for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
            shardcache_counter_remove(cache->counters, cache->cnt[i].name);
        }
        shardcache_counter_remove(cache->counters, "mru_size");
        shardcache_counter_remove(cache->counters, "mfu_size");
        shardcache_counter_remove(cache->counters, "mrug_size");
        shardcache_counter_remove(cache->counters, "mfug_size");
        shardcache_release_counters(cache->counters);
    }

    if (cache->volatile_storage)
        ht_destroy(cache->volatile_storage);

    if (cache->arc)
        arc_destroy(cache->arc);

    if (cache->chash)
        chash_free(cache->chash);

    if (cache->expirer_mux)
        iomux_destroy(cache->expirer_mux);

    if (cache->expirer_queue) {
        shardcache_expire_job_t *job = queue_pop_left(cache->expirer_queue);
        while(job) {
            free(job->key);
            free(job);
            job = queue_pop_left(cache->expirer_queue);
        }
        queue_destroy(cache->expirer_queue);
    }

    if (cache->cache_timeouts)
        ht_destroy(cache->cache_timeouts);

    if (cache->volatile_timeouts)
        ht_destroy(cache->volatile_timeouts);

    free(cache->me);

    free(cache->addr);

    if (cache->shards)
        shardcache_free_nodes(cache->shards, cache->num_shards);

    if (cache->connections_pool)
        connections_pool_destroy(cache->connections_pool);

    free(cache);
    SHC_DEBUG("Shardcache node stopped");
}

void
shardcache_clear(shardcache_t *cache)
{
    arc_clear(cache->arc);
}

void
shardcache_set_size(shardcache_t *cache, size_t new_size)
{
    arc_set_size(cache->arc, new_size);
}

int
shardcache_set_workers_num(shardcache_t *cache, unsigned int num_workers)
{
    return configure_serving_workers(cache->serv, num_workers);
}

typedef struct {
    int stat;
    size_t dlen;
    size_t offset;
    size_t len;
    size_t sent;
    shardcache_t *cache;
    arc_resource_t res;
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_async_helper_arg_t;

static int
shardcache_get_async_helper(void *key,
                            size_t klen,
                            void *data,
                            size_t dlen,
                            size_t total_size,
                            struct timeval *timestamp,
                            void *priv)
{
    shardcache_get_async_helper_arg_t *arg = (shardcache_get_async_helper_arg_t *)priv;

    arc_t *arc = arg->cache->arc;

    if (ATOMIC_READ(arg->cache->async_quit)) {
        arc_release_resource(arc, arg->res);
        free(arg);
        return -1;
    }

    int rc = 0;
    if (!dlen) {
        rc = arg->cb(key, klen, NULL, 0, total_size, timestamp, arg->priv);
    } else if ((arg->dlen + dlen) > arg->offset) {
        if (arg->dlen < arg->offset) {
            int diff = arg->offset - arg->dlen;
            rc = arg->cb(key, klen, data + diff, dlen - diff, total_size, timestamp, arg->priv);
            arg->sent += (dlen - diff);
        } else {
            if (arg->len && (arg->sent + dlen) > arg->len) {
                int diff = arg->len - arg->sent;
                rc = arg->cb(key, klen, data, diff, total_size, timestamp, arg->priv);
                arg->sent += diff;
            } else {
                rc = arg->cb(key, klen, data, dlen, total_size, timestamp, arg->priv);
                arg->sent += dlen;
            }
        }
    } 

    if (rc != 0) { // the callback raised an error
        ATOMIC_SET(arg->stat, -1);
        arc_release_resource(arc, arg->res);
        free(arg);
        return -1;
    }

    arg->dlen += dlen;

    if (!dlen) {
        // we are done
        arc_release_resource(arc, arg->res);
        free(arg);
    }

    return 0;
}

int
shardcache_get_offset(shardcache_t *cache,
                      void *key,
                      size_t klen,
                      size_t offset,
                      size_t length,
                      shardcache_get_async_callback_t cb,
                      void *priv)
{
    if (!key) {
        return -1;
    }

    if (offset == 0)
        ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_GETS].value);



    void *obj_ptr = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 1, cache->expire_time);
    if (!res) {
        return -1;
    }

    if (!obj_ptr) {
        arc_release_resource(cache->arc, res);
        return -1;
    }

    cached_object_t *obj = (cached_object_t *)obj_ptr;
    MUTEX_LOCK(obj->lock);
    if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_EVICTED)) {
        // if marked for eviction we don't want to return this object
        MUTEX_UNLOCK(obj->lock);
        arc_release_resource(cache->arc, res);
        // but we will try to fetch it again
        SHC_DEBUG("The retreived object has been already evicted, try fetching it again (offset)");
        return shardcache_get_offset(cache, key, klen, offset, length, cb, priv);
    } else if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_COMPLETE)) {
        size_t dlen = obj->dlen;
        void *data = NULL;
        if (offset) {
            if (offset < dlen) {
                dlen -= offset;
            } else {
                cb(key, klen, NULL, 0, 0, &obj->ts, priv);
                MUTEX_UNLOCK(obj->lock);
                arc_release_resource(cache->arc, res);
                free(data);
                return 0;
            }
        }
        if (dlen > length)
            dlen = length;
        if (dlen && obj->data) {
            data = malloc(dlen);
            memcpy(data, obj->data + offset, dlen);
        }

        time_t obj_expiration = obj->ts.tv_sec + cache->expire_time;

        if (UNLIKELY(cache->lazy_expiration && obj_expiration &&
            cache->expire_time > 0 && obj_expiration < time(NULL)))
        {
            MUTEX_UNLOCK(obj->lock);
            arc_drop_resource(cache->arc, res);
            free(data);
            ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_EXPIRES].value);
            return shardcache_get_offset(cache, key, klen, offset, length, cb, priv);
        } else {
            cb(key, klen, data, dlen, obj->dlen, &obj->ts, priv);
            MUTEX_UNLOCK(obj->lock);
            arc_release_resource(cache->arc, res);
            free(data);
            return 0;
        }
    } else {
        if (obj->dlen) {
            // check if we have enough so far
            if (obj->dlen > offset) {

                size_t dlen = obj->dlen;
                void *data = NULL;
                if (offset)
                    dlen -= offset;
                if (dlen > length)
                    dlen = length;
                if (dlen && obj->data) {
                    data = malloc(dlen);
                    memcpy(data, obj->data + offset, dlen);
                }
                cb(key, klen, data, dlen, obj->dlen, NULL, priv); // XXX - obj->dlen is not complete yet
                if (data)
                    free(data);
            }
        }

        shardcache_get_async_helper_arg_t *arg = calloc(1, sizeof(shardcache_get_async_helper_arg_t));
        arg->cb = cb;
        arg->priv = priv;
        arg->cache = cache;
        arg->res = res;
        arg->offset = offset;
        arg->len = length;

        shardcache_get_listener_t *listener = malloc(sizeof(shardcache_get_listener_t));
        listener->cb = shardcache_get_async_helper;
        listener->priv = arg;
        list_push_value(obj->listeners, listener);
        MUTEX_UNLOCK(obj->lock);
    }

    arc_release_resource(cache->arc, res);
    return 0;
}

size_t
shardcache_get_offset_sync(shardcache_t *cache,
                           void *key,
                           size_t klen,
                           void *data,
                           size_t *dlen,
                           size_t offset,
                           struct timeval *timestamp)
{
    size_t vlen = 0;
    size_t copied = 0;
    if (!key)
        return 0;

    if (offset == 0)
        ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_GETS].value);

    void *obj_ptr = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 0, cache->expire_time);
    if (!res)
        return 0;

    if (obj_ptr) {
        cached_object_t *obj = (cached_object_t *)obj_ptr;
        MUTEX_LOCK(obj->lock);
        if (obj->data) {
            if (dlen && data) {
                if (offset < obj->dlen) {
                    int size = obj->dlen - offset;
                    copied = size < *dlen ? size : *dlen;
                    memcpy(data, obj->data + offset, copied);
                    *dlen = copied;
                }
            }
            if (timestamp)
                memcpy(timestamp, &obj->ts, sizeof(struct timeval));
        }
        vlen = obj->dlen;
        MUTEX_UNLOCK(obj->lock);
    }
    arc_release_resource(cache->arc, res);
    return (offset < vlen + copied) ? (vlen - offset - copied) : 0;
}

int
shardcache_get(shardcache_t *cache,
               void *key,
               size_t klen,
               shardcache_get_async_callback_t cb,
               void *priv)
{
    if (!key)
        return -1;

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_GETS].value);

    SHC_DEBUG4("Getting value for key: %.*s", klen, key);

    void *obj_ptr = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 1, cache->expire_time);
    if ((!res || !obj_ptr) && cache->storage.fetch_local != NULL) {
        if (res) { // should never happen, maybe an assertion would make more sense
            arc_release_resource(cache->arc, res);
            return -1;
        }

        int fd = -1;
        size_t vlen = 0;
        if (cache->storage.fetch_local(key, klen, &fd, &vlen, cache->storage.priv) == 0) {
            fbuf_t buf = FBUF_STATIC_INITIALIZER;
            size_t sent = 0;
            struct timeval tv;
            int read_size = 16 * 1024;
            int rb = fbuf_read(&buf, fd, read_size);
            while(rb > 0) {
                gettimeofday(&tv, NULL);
                sent += rb;
                if (sent == vlen) { 
                    cb(key, klen, fbuf_data(&buf), rb, vlen, &tv, priv);
                }
                cb(key, klen, fbuf_data(&buf), rb, 0, &tv, priv);
                fbuf_clear(&buf);
                rb = fbuf_read(&buf, fd, read_size);
            }
            return 0;
        }
    }

    res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 1, cache->expire_time);
    if (!res)
        return -1;

    if (!obj_ptr) {
        arc_release_resource(cache->arc, res);
        return -1;
    }

    cached_object_t *obj = (cached_object_t *)obj_ptr;
    MUTEX_LOCK(obj->lock);

    uint32_t retry_timeout = 1<<7;
    while (UNLIKELY(COBJ_CHECK_FLAGS(obj, COBJ_FLAG_EVICTED))) {
        // if marked for eviction we don't want to return this object
        // but we will try to fetch it again
        MUTEX_UNLOCK(obj->lock);
        arc_release_resource(cache->arc, res);

        if (retry_timeout > 1<<11) {
            SHC_DEBUG("The retreived object has been already evicted, try fetching it again");
            return -1;
        }

        SHC_DEBUG("Got an evicted object, retrying in %u microseconds", retry_timeout);
        usleep(retry_timeout);
        retry_timeout <<= 1;

        obj_ptr = NULL;
        res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 1, cache->expire_time);
        if (!res)
            return -1;

        if (!obj_ptr) {
            arc_release_resource(cache->arc, res);
            return -1;
        }

        obj = (cached_object_t *)obj_ptr;
        MUTEX_LOCK(obj->lock);
    }

    if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_COMPLETE)) {
        time_t obj_expiration = obj->ts.tv_sec + cache->expire_time;
        if (UNLIKELY(cache->lazy_expiration && obj_expiration &&
                     cache->expire_time > 0 && obj_expiration < time(NULL)))
        {
            MUTEX_UNLOCK(obj->lock);
            arc_drop_resource(cache->arc, res);
            ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_EXPIRES].value);
            return shardcache_get(cache, key, klen, cb, priv);

        } else {
            cb(key, klen, obj->data, obj->dlen, obj->dlen, &obj->ts, priv);
            MUTEX_UNLOCK(obj->lock);
            arc_release_resource(cache->arc, res);
        }
    } else {
        if (obj->dlen) // let's send what we have so far
            cb(key, klen, obj->data, obj->dlen, 0, NULL, priv);

        shardcache_get_async_helper_arg_t *arg = calloc(1, sizeof(shardcache_get_async_helper_arg_t));
        arg->cb = cb;
        arg->priv = priv;
        arg->cache = cache;
        arg->res = res;

        shardcache_get_listener_t *listener = malloc(sizeof(shardcache_get_listener_t));
        listener->cb = shardcache_get_async_helper;
        listener->priv = arg;
        list_push_value(obj->listeners, listener);
        MUTEX_UNLOCK(obj->lock);
    }

    return 0;
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    fbuf_t data;
    int stat;
    struct timeval ts;
    int free;
    int complete;
} shardcache_get_helper_arg_t;

static int
shardcache_get_helper(void *key,
                      size_t klen,
                      void *data,
                      size_t dlen,
                      size_t total_size,
                      struct timeval *timestamp,
                      void *priv)
{
    shardcache_get_helper_arg_t *arg = (shardcache_get_helper_arg_t *)priv;
    if (dlen) {
        fbuf_add_binary(&arg->data, data, dlen);
    } else if (!total_size) {
        // error notified (dlen == 0 && total_size == 0)
        ATOMIC_SET(arg->complete, 1);
        ATOMIC_SET(arg->stat, -1);
        pthread_cond_signal(&arg->cond);
        return -1;
    }

    if (timestamp) {
        ATOMIC_SET(arg->complete, 1);
        if (timestamp)
            memcpy(&arg->ts, timestamp, sizeof(struct timeval));
        if (total_size != fbuf_used(&arg->data)) {
            ATOMIC_SET(arg->stat, -1);
        }
        MUTEX_LOCK(arg->lock);
        pthread_cond_signal(&arg->cond);
        MUTEX_UNLOCK(arg->lock);
    }


    return 0;
}

int
shardcache_get_sync(shardcache_t *cache,
                    void *key,
                    size_t klen,
                    void **value,
                    size_t *vlen,
                    struct timeval *timestamp)
{
    if (!key)
        return -1;

    shardcache_get_helper_arg_t *arg = calloc(1, sizeof(shardcache_get_helper_arg_t));

    MUTEX_INIT(arg->lock);
    CONDITION_INIT(arg->cond);
    arg->stat = 0;
    memset(&arg->ts, 0, sizeof(arg->ts));
    memset(&arg->data, 0, sizeof(arg->data));
    arg->complete = 0;

    int rc = shardcache_get(cache, key, klen, shardcache_get_helper, arg);

    if (rc == 0) {
        while (!ATOMIC_READ(arg->complete) && ATOMIC_READ(arg->stat) == 0) {
            struct timeval now;
            gettimeofday(&now, NULL);
            struct timeval waiting_time = { 0, 500000 };
            struct timeval sum;
            timeradd(&now, &waiting_time, &sum);
            struct timespec abstime = { sum.tv_sec, sum.tv_usec * 1000 };
            MUTEX_LOCK(arg->lock);
            pthread_cond_timedwait(&arg->cond, &arg->lock, &abstime); 
            if (ATOMIC_READ(cache->async_quit)) {
                ATOMIC_SET(arg->stat, 1);
                break;
            }
        }

        char *data = fbuf_data(&arg->data);
        uint32_t len = fbuf_used(&arg->data);
        int stat = ATOMIC_READ(arg->stat);

        if (timestamp)
            memcpy(timestamp, &arg->ts, sizeof(struct timeval));

        MUTEX_DESTROY(arg->lock);
        CONDITION_DESTROY(arg->cond);
        free(arg);

        if (stat != 0 && !ATOMIC_READ(cache->async_quit)) {
            SHC_ERROR("Error trying to get key: %.*s", klen, key);
            if (data)
                free(data);
            return -1;
        }

        if (vlen)
            *vlen = len;

        if (!data)
            SHC_DEBUG("No value for key: %.*s", klen, key);

        if (value)
            *value = data;

        return 0;
    }
    fbuf_destroy(&arg->data);
    MUTEX_DESTROY(arg->lock);
    CONDITION_DESTROY(arg->cond);
    free(arg);
    return rc;
}

size_t
shardcache_head(shardcache_t *cache,
                void *key,
                size_t len,
                void *head,
                size_t hlen,
                struct timeval *timestamp)
{
    if (!key)
        return 0;

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_HEADS].value);

    size_t rlen = hlen;
    size_t remainder =  shardcache_get_offset_sync(cache, key, len, head, &rlen, 0, timestamp);
    return remainder + rlen;
}

int shardcache_get_multi(shardcache_t *cache,
                         void **keys,
                         size_t *lens,
                         int num_keys,
                         shardcache_get_async_callback_t cb,
                         void *priv)
{

    arc_resource_t *resources = malloc(sizeof(arc_resource_t) * num_keys);
    int rc = arc_lookup_multi(cache->arc, keys, lens, resources, num_keys, cache->expire_time);
        
    if (rc != 0) {
        free(resources);
        return -1;
    }

    int i;
    for (i = 0; i < num_keys; i++) {
        arc_resource_t res = resources[i];
        if (res) {
            cached_object_t *obj = (cached_object_t *)arc_get_resource_ptr(res);
            MUTEX_LOCK(obj->lock);

            int complete = COBJ_CHECK_FLAGS(obj, COBJ_FLAG_COMPLETE);
            if (complete) {
                time_t obj_expiration = obj->ts.tv_sec + cache->expire_time;
                if (UNLIKELY(cache->lazy_expiration && obj_expiration &&
                             cache->expire_time > 0 && obj_expiration < time(NULL)))
                {
                    MUTEX_UNLOCK(obj->lock);
                    arc_drop_resource(cache->arc, res);
                    cb(keys[i], lens[i], NULL, 0, 0, NULL, priv);
                    continue;
                }
                cb(obj->key, obj->klen, obj->data, obj->dlen, obj->dlen, &obj->ts, priv);
                arc_release_resource(cache->arc, res);
            } else {
                // send what we have so far
                if (obj->dlen)
                    cb(obj->key, obj->klen, obj->data, obj->dlen, 0, NULL, priv);

                // and then register the listener
                shardcache_get_async_helper_arg_t *arg = calloc(1, sizeof(shardcache_get_async_helper_arg_t));
                arg->cb = cb;
                arg->priv = priv;
                arg->cache = cache;
                arg->res = res;

                shardcache_get_listener_t *listener = malloc(sizeof(shardcache_get_listener_t));
                listener->cb = shardcache_get_async_helper;
                listener->priv = arg;
                list_push_value(obj->listeners, listener);
            }

            MUTEX_UNLOCK(obj->lock);
        } else {
            cb(keys[i], lens[i], NULL, 0, 0, NULL, priv);
        }
    }

    free(resources);

    return 0;
}

typedef struct {
    void *key;
    size_t klen;
    shardcache_async_response_callback_t cb;
    void *priv;
    int done;
    char *addr;
    int fd;
    shardcache_hdr_t hdr;
    shardcache_t *cache;
    int error;
} shardcache_async_command_helper_arg_t;

static int shardcache_async_command_helper(void *data,
                                           size_t len,
                                           int  idx,
                                           size_t total_len,
                                           void *priv)
{
    shardcache_async_command_helper_arg_t *arg =
        (shardcache_async_command_helper_arg_t *)priv;

    // idx == -1 means that reading finished 
    // idx == -2 means error
    // idx == -3 means the async connection can been closed
    // any idx >= 0 refers to the record index

    // XXX - works only for the first record
    //       needs refactoring if we will started
    //       including multiple reports in the responses
    if (idx == 0) {
        int rc = -1;
        if (data && len == 1) {
            rc = (int)*((char *)data);
            // mangle the return code to conform
            // to the specific command (if necessary)
            if (arg->hdr == SHC_HDR_EXISTS) {
               switch(rc) {
                    case SHC_RES_YES:
                        rc = 1;
                        break;
                    case SHC_RES_NO:
                        rc = 0;
                        break;
                    default:
                        rc = -1;
                        break;
               }
            }
        }
        arg->cb(arg->key, arg->klen, rc, arg->priv);
        arg->done = 1;
    } else if (idx == -1) {
        if (!arg->done)
            arg->cb(arg->key, arg->klen, -1, arg->priv);
    } else if (idx == -2) {
        arg->error = 1;
    } else if (idx == -3) {
        if (arg->fd >= 0) {
            if (arg->error)
                close(arg->fd);
            else
                shardcache_release_connection_for_peer(arg->cache, arg->addr, arg->fd);
        }
        free(arg->key);
        free(arg);
    }
    return 0;
}

static inline int
shardcache_fetch_async_response(shardcache_t *cache,
                                void *key,
                                size_t klen,
                                shardcache_hdr_t hdr,
                                char *addr,
                                int fd,
                                shardcache_async_response_callback_t cb,
                                void *priv)
{
    shardcache_async_command_helper_arg_t *arg = calloc(1, sizeof(shardcache_async_command_helper_arg_t));
    arg->key = malloc(klen);
    memcpy(arg->key, key, klen);
    arg->klen = klen;
    arg->cb = cb;
    arg->priv = priv;
    arg->cache = cache;
    arg->addr = addr;
    arg->fd = fd;
    arg->hdr = hdr;
    async_read_wrk_t *wrk = NULL;
    int rc = read_message_async(fd, shardcache_async_command_helper, arg, &wrk);
    if (rc == 0 && wrk) {
        shardcache_queue_async_read_wrk(cache, wrk);
    } else {
        free(arg->key);
        free(arg);
        cb(key, klen, -1, priv);
    }
    return rc;
}

int
shardcache_exists(shardcache_t *cache,
                  void *key,
                  size_t klen,
                  shardcache_async_response_callback_t cb,
                  void *priv)
{
    if (!key || !klen)
        return -1;

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    int rc = -1;

    if (is_mine == 1)
    {
        // TODO - clean this bunch of nested conditions
        if (!ht_exists(cache->volatile_storage, key, klen)) {
            if (cache->use_persistent_storage && cache->storage.exist) {
                if (!cache->storage.exist(key, klen, cache->storage.priv)) {
                    rc = 0;
                } else {
                    rc = 1;
                }
            } else {
                rc = 0;
            }
        } else {
            rc = 1;
        }

        if (cb)
            cb(key, klen, rc, priv);

    } else {
        shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name); 
        if (!peer) {
            SHC_ERROR("Can't find address for node %s", peer);
            if (cb)
                cb(key, klen, -1, priv);
            return -1;
        }
        char *addr = shardcache_node_get_address(peer);
        int fd = shardcache_get_connection_for_peer(cache, addr);
        if (cb) {
            rc = exists_on_peer(addr, key, klen, fd, 0);
            if (rc == 0)
                rc = shardcache_fetch_async_response(cache, key, klen, SHC_HDR_EXISTS, addr, fd, cb, priv);
        } else {
            rc = exists_on_peer(addr, key, klen, fd, 1);
            shardcache_release_connection_for_peer(cache, addr, fd);
        }
    }

    return rc;
}

int
shardcache_touch(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !klen)
        return -1;

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        void *obj_ptr = NULL;
        arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 0, cache->expire_time);
        if (res) {
            cached_object_t *obj = (cached_object_t *)obj_ptr;
            MUTEX_LOCK(obj->lock);
            gettimeofday(&obj->ts, NULL);
            MUTEX_UNLOCK(obj->lock);
            arc_release_resource(cache->arc, res);
            return obj ? 0 : -1;
        }
    } else {
        shardcache_node_t *peer = shardcache_node_select(cache, node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s", peer);
            return -1;
        }
        char *addr = shardcache_node_get_address(peer);
        int fd = shardcache_get_connection_for_peer(cache, addr);
        int rc = touch_on_peer(addr, key, klen, fd);
        shardcache_release_connection_for_peer(cache, addr, fd);
        return rc;
    }

    return -1;
}

static void
shardcache_commence_eviction(shardcache_t *cache, void *key, size_t klen)
{
    shardcache_evictor_job_t *job = create_evictor_job(key, klen);

    SHC_DEBUG2("Adding evictor job for key %.*s", klen, key);

    int rc = ht_set_if_not_exists(cache->evictor_jobs, key, klen, job, sizeof(shardcache_evictor_job_t));

    if (rc != 0) {
        destroy_evictor_job(job);
        return;
    }

    MUTEX_LOCK(cache->evictor_lock);
    pthread_cond_signal(&cache->evictor_cond);
    MUTEX_UNLOCK(cache->evictor_lock);
}

static inline int
shardcache_queue_expiration_job(shardcache_t *cache, void *key, size_t klen, int expire, int is_volatile, int cmd)
{
    shardcache_expire_job_t *job = calloc(1, sizeof(shardcache_expire_job_t));
    job->cmd = cmd;

    job->key = malloc(klen);
    job->klen = klen;
    memcpy(job->key, key, klen);
    job->expire = expire;
    job->is_volatile = is_volatile;

    int rc = queue_push_right(cache->expirer_queue, job);
    if (rc != 0) {
        free(job->key);
        free(job);
    }

    return rc;
}

int
shardcache_unschedule_expiration(shardcache_t *cache, void *key, size_t klen, int is_volatile)
{
    return shardcache_queue_expiration_job(cache, key, klen, 0, is_volatile, SHARDACHE_EXPIRE_UNSCHEDULE);
}

int
shardcache_schedule_expiration(shardcache_t *cache,
                               void *key,
                               size_t klen,
                               time_t expire,
                               int is_volatile)
{

    return shardcache_queue_expiration_job(cache, key, klen, expire, is_volatile, SHARDACHE_EXPIRE_SCHEDULE);
}

typedef struct {
    void *new_value;
    size_t new_len;
    void *prev_value;
    size_t prev_len;
    int matched;
} shardcache_cas_volatile_cb_arg_t;

static void *
shardcache_cas_volatile_cb(void *ptr, size_t len, void *user)
{
    shardcache_cas_volatile_cb_arg_t *arg = (shardcache_cas_volatile_cb_arg_t *)user;
    volatile_object_t *item = (volatile_object_t *)ptr;
    if (item->dlen == arg->prev_len && memcmp(item->data, arg->prev_value, item->dlen) == 0) {
        item->data = realloc(item->data, arg->new_len);
        memcpy(item->data, arg->new_value, arg->new_len);
        arg->matched = 1;
    }

    return item;
}

static inline int
shardcache_store_volatile(shardcache_t *cache,
                          void *key,
                          size_t klen,
                          void *value,
                          size_t vlen,
                          void *prev_value,
                          size_t prev_len,
                          time_t expire,
                          time_t cexpire,
                          int mode, // 0 = SET, 1 == ADD, 2 == CAS
                          int replica)
{
    volatile_object_t *obj = NULL;
    volatile_object_t *prev = NULL;
    // ensure removing this key from the persistent storage (if present)
    // since it's now going to be a volatile item
    if (cache->use_persistent_storage && cache->storage.remove)
        cache->storage.remove(key, klen, cache->storage.priv);

    if (mode == 2 && prev_value == NULL) // if CAS and prev_value is NULL we really want ADD
        mode = 1;

    if (mode == 1 && ht_exists(cache->volatile_storage, key, klen)) {
        SHC_DEBUG("A volatile value already exists for key %.*s", klen, key);
        return 1;
    } else if (mode == 2) {
        shardcache_cas_volatile_cb_arg_t arg = {
            .new_value = value,
            .new_len = vlen,
            .prev_value = prev_value,
            .prev_len = prev_len,
            .matched = 0
        };
        obj = (volatile_object_t *)ht_get_deep_copy(cache->volatile_storage,
                                                    key,
                                                    klen,
                                                    NULL,
                                                    shardcache_cas_volatile_cb,
                                                    &arg);
        if (obj)
            return !arg.matched;
    }

    obj = malloc(sizeof(volatile_object_t));
    obj->data = malloc(vlen);
    memcpy(obj->data, value, vlen);
    obj->dlen = vlen;
    time_t now = time(NULL);
    time_t real_expire = expire ? time(NULL) + expire : 0;
    obj->expire = real_expire;

    SHC_DEBUG2("Setting volatile item %.*s to expire %d (now: %d)", 
        klen, key, obj->expire, (int)now);

    void *prev_ptr = NULL;
    if (mode == 1) {
        int rc = ht_set_if_not_exists(cache->volatile_storage, key, klen,
                                  obj, sizeof(volatile_object_t));
        if (rc == 0) {
            ATOMIC_INCREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value, obj->dlen);
        } else {
            prev_ptr = obj;
            obj = NULL;
        }

    } else { // mode == 2 (CAS) is already handled above
        ht_get_and_set(cache->volatile_storage, key, klen,
                       obj, sizeof(volatile_object_t),
                       &prev_ptr, NULL);
    }

    if (prev_ptr) {
        prev = (volatile_object_t *)prev_ptr;
        if (vlen > prev->dlen) {
            ATOMIC_INCREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                            vlen - prev->dlen);
        } else {
            ATOMIC_DECREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                            prev->dlen - vlen);
        }
        destroy_volatile(prev); 
        if (cache->cache_on_set)
            arc_load(cache->arc, (const void *)key, klen, value, vlen, cexpire);
        else
            arc_remove(cache->arc, (const void *)key, klen);

        if (!replica)
            shardcache_commence_eviction(cache, key, klen);

    } else {
        ATOMIC_INCREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value, vlen);
    }

    if (obj && obj->expire)
        shardcache_schedule_expiration(cache, key, klen, expire, 1);

    return 0;
}

static inline int
shardcache_store(shardcache_t *cache,
                 void *key,
                 size_t klen,
                 void *value,
                 size_t vlen,
                 void *prev_value,
                 size_t prev_vlen,
                 time_t expire,
                 time_t cexpire,
                 int mode, // 0 = SET, 1 == ADD, 2 == CAS
                 int replica)

{
    int rc = -1;
    if (mode == 2) {
        if (prev_value == NULL) {
            // if CAS and prev_value is NULL we really want ADD
            mode = 1;
        } else if (cache->use_persistent_storage) {
            if (cache->storage.cas) {
                rc = cache->storage.cas(key,
                                        klen,
                                        prev_value,
                                        prev_vlen,
                                        value,
                                        vlen,
                                        cache->storage.priv);
                if (rc == 0 && !replica)
                    shardcache_commence_eviction(cache, key, klen);
            } else {
                SHC_WARNING("CAS command not supported from the underlying storage");
                return -1;
            }
            return rc;
        }
    }

    // if the storage is readonly we will store a volatile object instead
    if (!cache->use_persistent_storage || !cache->storage.store || expire)
        return shardcache_store_volatile(cache,
                                         key,
                                         klen,
                                         value,
                                         vlen,
                                         prev_value,
                                         prev_vlen,
                                         expire,
                                         cexpire,
                                         mode,
                                         replica);

    rc = cache->storage.store(key, klen, value, vlen, mode, cache->storage.priv);

    if (cache->cache_on_set)
        arc_load(cache->arc, (const void *)key, klen, value, vlen, cexpire);
    else
        arc_remove(cache->arc, (const void *)key, klen);

    if (!replica)
        shardcache_commence_eviction(cache, key, klen);

    return rc;
}

// TODO - cleanup
// this routine should be splitted in at least 2 or 3 different
// functions, depending if we are the owner, if we need to forward to a peer
// if we fallback to a global storage because the peer is not available
int
shardcache_set_internal(shardcache_t *cache,
                        void *key,
                        size_t klen,
                        void *prev_value,
                        size_t prev_vlen,
                        void *value,
                        size_t vlen,
                        time_t expire,
                        time_t cexpire,
                        int mode, // 0 = SET, 1 == ADD, 2 == CAS
                        int replica,
                        shardcache_async_response_callback_t cb,
                        void *priv)
{
    int rc = -1;
    int async = 0;

    if (klen == 0 || vlen == 0 || !key || !value) {
        if (cb)
            cb(key, klen, -1, priv);
        return rc;
    }

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_SETS].value);

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    
    // first check if we are the owner for this key
    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        SHC_DEBUG2("Storing value %s (%d) for key %.*s",
                   shardcache_hex_escape(value, vlen, DEBUG_DUMP_MAXSIZE, 0),
                   (int)vlen, klen, key);

        rc = shardcache_store(cache, key, klen, value, vlen, prev_value, prev_vlen, expire, cexpire, mode == 1 ? 1 : 0, replica);
    }
    else if (node_len)
    {
        // if we are not the owner try propagating the command to the responsible peer
    
        SHC_DEBUG2("Forwarding set command %.*s => %s (%d) to %s",
                klen, key, shardcache_hex_escape(value, vlen, DEBUG_DUMP_MAXSIZE, 0),
                (int)vlen, node_name);

        shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s", peer);
            if (cache->use_persistent_storage && cache->storage.global)
                rc = shardcache_store(cache, key, klen, value, vlen, prev_value, prev_vlen, expire, cexpire, mode == 1 ? 1 : 0, replica);
            
            if (cb)
                cb(key, klen, rc, priv);

            return rc;
        }

        char *addr = shardcache_node_get_address(peer);
        int fd = shardcache_get_connection_for_peer(cache, addr);
        unsigned char hdr;
        switch(mode) {
            case 0:
                hdr = SHC_HDR_SET;
                rc = send_to_peer(addr, key, klen, value, vlen, expire, cexpire, fd, cb ? 0 : 1);
                break;
            case 1:
                hdr = SHC_HDR_ADD;
                rc = add_to_peer(addr, key, klen, value, vlen, expire, cexpire, fd, cb ? 0 : 1);
                break;
            case 2:
                hdr = SHC_HDR_CAS;
                rc = cas_on_peer(addr, key, klen, prev_value, prev_vlen, value, vlen, expire, cexpire, fd, cb ? 0 : 1);
                break;
            default:
                // TODO - Error Messages
                return -1;
        }

        if (cb) {
            if (rc == 0) {
                rc = shardcache_fetch_async_response(cache, key, klen, hdr, addr, fd, cb, priv);
                async = 1;
            } else {
                close(fd);
                if (cache->use_persistent_storage && cache->storage.global)
                    rc = shardcache_store(cache, key, klen, value, vlen, prev_value, prev_vlen, expire, cexpire, mode == 1 ? 1 : 0, replica);
            }
        } else {
            if (rc == 0) {
                shardcache_release_connection_for_peer(cache, addr, fd);
            } else {
                close(fd);
                if (cache->use_persistent_storage && cache->storage.global)
                    rc = shardcache_store(cache, key, klen, value, vlen, prev_value, prev_vlen, expire, cexpire, mode == 1 ? 1 : 0, replica);
            }
        }

        if (rc == 0) {
            if (cache->cache_on_set)
                arc_load(cache->arc, (const void *)key, klen, value, vlen, cexpire);
            else
                arc_remove(cache->arc, (const void *)key, klen);
        }

    }

    if (cb && !async)
        cb(key, klen, rc, priv);

    if (rc == 0 && cexpire) {
    }

    return rc;
}

int
shardcache_add(shardcache_t *cache,
               void *key,
               size_t klen,
               void *value,
               size_t vlen,
               time_t expire,
               time_t cexpire,
               shardcache_async_response_callback_t cb,
               void *priv)
{
    if (!key || !klen)
        return -1;

    if (cache->replica && (!cache->use_persistent_storage || !cache->storage.shared)) {
        int rc = shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_ADD, key, klen, value, vlen, NULL, 0, expire, cexpire);
        if (cb)
            cb(key, klen, rc, priv);
    }

    return shardcache_set_internal(cache, key, klen, NULL, 0, value, vlen, expire, cexpire, 1, 0, cb, priv);
}

int shardcache_set(shardcache_t *cache,
                   void  *key,
                   size_t klen,
                   void  *value,
                   size_t vlen,
                   time_t expire,
                   time_t cexpire,
                   int    if_not_exists,
                   shardcache_async_response_callback_t cb,
                   void *priv)
{
    if (!key || !klen)
        return -1;

    if (cache->replica && (!cache->use_persistent_storage || !cache->storage.shared)) {
        int rc = shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_SET, key, klen, value, vlen, NULL, 0, expire, cexpire);
        if (cb)
            cb(key, klen, rc, priv);
        return rc;
    }

    return shardcache_set_internal(cache, key, klen, NULL, 0, value, vlen, expire, cexpire, if_not_exists ? 1 : 0, 0, cb, priv);
}

int shardcache_set_local(shardcache_t *cache,
                         void *key,
                         size_t klen,
                         char *local_path,
                         size_t vlen,
                         int if_not_exists) {
    if (!key || !klen || !cache->storage.store_local)
        return -1;
    return cache->storage.store_local(key, klen, local_path, vlen, if_not_exists, cache->storage.priv);
}

int
shardcache_set_multi(shardcache_t *cache,
                     void **keys,
                     size_t *klens,
                     void **values,
                     size_t *vlens,
                     int nkeys,
                     time_t expire,
                     time_t cexpire,
                     int if_not_exists,
                     shardcache_async_response_callback_t cb,
                     void *priv)
{
    int i;

    if (!nkeys || !keys || !klens || ! values || !vlens)
        return -1;

    // TODO - create different buckets grouping keys by owner so that we can 
    // call shardcache_set() to set local keys only, and forward the rest to the
    // proper peer using a SET_MULTI command
    for (i = 0; i < nkeys; i++) {
        shardcache_set(cache,
                       keys[i],
                       klens[i],
                       values[i],
                       vlens[i],
                       expire,
                       cexpire,
                       if_not_exists,
                       cb,
                       priv);
    }

    return 0;
}

int
shardcache_cas(shardcache_t *cache,
               void *key,
               size_t klen,
               void *prev_value,
               size_t prev_len,
               void *new_value,
               size_t new_len,
               time_t expire,
               time_t cexpire,
               shardcache_async_response_callback_t cb,
               void *priv)
{
    if (cache->replica) {
        int rc = shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_CAS, key, klen, prev_value, prev_len, new_value, new_len, expire, cexpire);
        if (cb)
            cb(key, klen, rc, priv);
        return rc;
    }


    return shardcache_set_internal(cache, key, klen, prev_value, prev_len, new_value, new_len, expire, cexpire, 2, 0, cb, priv);
}

#define xisspace(c) isspace((unsigned char)c)

static int safe_strtoll(char *str, size_t len, int64_t *out) {
    static __thread char buf[1024];

    errno = 0;
    *out = 0;
    char *endptr;

    char *check = str + len;
    while (check != str) {
        if (*check-- == 0)
            break;
    }

    if (check == str) {
        if (len >= sizeof(buf)) {
            // TODO - Error Messages
            return 0;
        }
        memcpy(buf, str, len);
        buf[len] = 0;
        str = buf;
    }

    long long ll = strtoll(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return 0;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = ll;
        return 1;
    }
    return 0;
}

static void *
shardcache_increment_volatile_cb(void *ptr, size_t len, void *user)
{
    static __thread int64_t ret = 0;

    int64_t *amount = (int64_t *)user;
    volatile_object_t *item = (volatile_object_t *)ptr;
    if (!safe_strtoll((char *)item->data, item->dlen, &ret)) {
        SHC_ERROR("Volatile object was expected to be an integer in shardcache_increment_volatile_cb()");
        return NULL;
    }
    ret += *amount;
    fbuf_t new = FBUF_STATIC_INITIALIZER_PARAMS(0, 10, 10, 1);
    fbuf_nprintf(&new, 20, "%"PRIi64, ret);
    free(item->data);
    int blen = 0;
    item->dlen = fbuf_detach(&new, (char **)&item->data, &blen);

    return &ret;
}

int
shardcache_increment_internal(shardcache_t *cache,
                              void *key,
                              size_t klen,
                              int64_t amount,
                              int64_t initial,
                              time_t expire,
                              time_t cexpire,
                              int64_t *out,
                              shardcache_async_response_callback_t cb,
                              void *priv)
{
    int rc = 0;
    int64_t value = 0;

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        void *v = ht_get_deep_copy(cache->volatile_storage,
                                  key,
                                  klen,
                                  NULL,
                                  shardcache_increment_volatile_cb,
                                  &amount);
        if (!v) {
            if (cache->use_persistent_storage && !expire) {
                int64_t real_amount;
                int (*real_function)(void *, size_t, int64_t, int64_t, int64_t *, void *);
                if (amount >= 0) {
                    if (cache->storage.increment) {
                        real_function = cache->storage.increment;
                        real_amount = amount;
                    } else {
                        real_function = cache->storage.decrement;
                        real_amount = -amount;
                    }
                } else {
                    if (cache->storage.decrement) {
                        real_function = cache->storage.decrement;
                        real_amount = amount;
                    } else {
                        real_function = cache->storage.increment;
                        real_amount = -amount;
                    }
                }

                if (real_function) {
                    rc = real_function(key, klen, real_amount, initial, &value, cache->storage.priv);
                    v = (void *)&value;
                }
            }

            if (!v) {
                // if we are here .... there is no persistent storage support for increment/decrement
                char data[32];
                value = initial + amount;
                size_t dsize = snprintf(data, sizeof(data), "%"PRIi64, value);
                rc = shardcache_store_volatile(cache, key, klen, data, dsize, NULL, 0, expire, cexpire, 1, 0);
                if (rc != 0) {
                    v = ht_get_deep_copy(cache->volatile_storage,
                                         key,
                                         klen,
                                         NULL,
                                         shardcache_increment_volatile_cb,
                                         &amount);
                } else {
                    v = (void *)&value;
                }
            }
        }

        if (v) {
            arc_remove(cache->arc, (const void *)key, klen);
            shardcache_commence_eviction(cache, key, klen);
            int64_t output_value = *((int64_t *)v);
            if (out)
                *out = output_value;

            if (cb)
                cb(key, klen, output_value, priv);

        }
    } else {
        SHC_DEBUG2("Forwarding increment command %.*s => %"PRIi64" to %s",
                klen, key, amount, node_name);

        shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s", peer);
            if (cache->use_persistent_storage && cache->storage.global) {
                int64_t real_amount;
                int (*real_function)(void *, size_t, int64_t, int64_t, int64_t *, void *);
                if (amount >= 0) {
                    if (cache->storage.increment) {
                        real_function = cache->storage.increment;
                        real_amount = amount;
                    } else {
                        real_function = cache->storage.decrement;
                        real_amount = -amount;
                    }
                } else {
                    if (cache->storage.decrement) {
                        real_function = cache->storage.decrement;
                        real_amount = amount;
                    } else {
                        real_function = cache->storage.increment;
                        real_amount = -amount;
                    }
                }

                if (real_function) {
                    rc = real_function(key, klen, real_amount, initial, &value, cache->storage.priv);
                    if (rc == 0 )//&& !replica)
                        shardcache_commence_eviction(cache, key, klen);

                } else {
                    char data[32];
                    size_t dsize = snprintf(data, sizeof(data), "%"PRIi64, initial + amount);
                    rc = shardcache_store_volatile(cache, key, klen, data, dsize, NULL, 0, expire, cexpire, 1, 0);
                    if (rc == 1) {
                        void *v = ht_get_deep_copy(cache->volatile_storage,
                                                   key,
                                                   klen,
                                                   NULL,
                                                   shardcache_increment_volatile_cb,
                                                   &amount);
                        if (v) {
                            value = *((int *)v);
                            free(v);
                        } else {
                            rc = -1;
                        }
                    }
                }
            }

            if (out)
                *out = value;
            
            if (cb)
                cb(key, klen, value, priv);

        } else {
            char *addr = shardcache_node_get_address(peer);
            int fd = shardcache_get_connection_for_peer(cache, addr);
            if (amount >= 0)
                rc = increment_on_peer(addr, key, klen, amount, initial, expire, cexpire, &value, fd, cb ? 0 : 1);
            else
                rc = decrement_on_peer(addr, key, klen, -amount, initial, expire, cexpire, &value, fd, cb ? 0 : 1);

            if (cb) {
                if (rc == 0) {
                    rc = shardcache_fetch_async_response(cache, key, klen,
                                                         amount >= 0 ? SHC_HDR_INCREMENT : SHC_HDR_DECREMENT,
                                                         addr, fd, cb, priv);
                } else {
                    close(fd);
                    cb(key, klen, 0, priv);
                }
            } else {
                if (rc == 0) {
                    shardcache_release_connection_for_peer(cache, addr, fd);
                } else {
                    close(fd);
                }
            }

            if (rc == 0) {
                if (out)
                    *out = value;
                arc_remove(cache->arc, (const void *)key, klen);
            }
        }
    }

    return rc;
}

int
shardcache_increment(shardcache_t *cache,
                     void *key,
                     size_t klen,
                     int64_t amount,
                     int64_t initial,
                     time_t expire,
                     time_t cexpire,
                     int64_t *out,
                     shardcache_async_response_callback_t cb,
                     void *priv)
{
    return shardcache_increment_internal(cache, key, klen, amount, initial,
                                         expire, cexpire, out, cb, priv);
}

int
shardcache_decrement(shardcache_t *cache,
                     void *key,
                     size_t klen,
                     int64_t amount,
                     int64_t initial,
                     time_t expire,
                     time_t cexpire,
                     int64_t *out,
                     shardcache_async_response_callback_t cb,
                     void *priv)

{
    return shardcache_increment_internal(cache, key, klen, -amount, initial,
                                         expire, cexpire, out, cb, priv);
}

int
shardcache_del_internal(shardcache_t *cache,
                        void *key,
                        size_t klen,
                        int replica, 
                        shardcache_async_response_callback_t cb,
                        void *priv)
{
    int rc = -1;

    if (!key) {
        if (cb)
            cb(key, klen, -1, priv);
        return rc;
    }

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_DELS].value);

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        void *prev_ptr;
        rc = ht_delete(cache->volatile_storage, key, klen, &prev_ptr, NULL);

        if (rc != 0) {
            if (cache->use_persistent_storage) {
                if (cache->storage.remove) {
                    rc = cache->storage.remove(key, klen, cache->storage.priv);
                } else {
                    // if there is a readonly persistent storage
                    // we want to return a 'success' return code,
                    // since eviction is going to be commenced anyway
                    // which is what expected when sending a delete command
                    // to a node using a readonly persistent storage
                    rc = 0;
                }
            }
        } else if (prev_ptr) {
            shardcache_unschedule_expiration(cache, key, klen, 1);
            volatile_object_t *prev_item = (volatile_object_t *)prev_ptr;
            ATOMIC_DECREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                            prev_item->dlen);
            destroy_volatile(prev_item);
        }

        if (ATOMIC_READ(cache->evict_on_delete))
        {
            arc_remove(cache->arc, (const void *)key, klen);

            if (!replica)
                shardcache_commence_eviction(cache, key, klen);
        }

        if (cb)
            cb(key, klen, rc, priv);

    } else if (!replica) {
        shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s", peer);
            if (cb)
                cb(key, klen, -1, priv);
            return -1;
        }
        char *addr = shardcache_node_get_address(peer);
        int fd = shardcache_get_connection_for_peer(cache, addr);
        if (cb) {
            rc = delete_from_peer(addr, key, klen, fd, 0);
            if (rc == 0) {
                rc = shardcache_fetch_async_response(cache, key, klen, SHC_HDR_DELETE, addr, fd, cb, priv);
            } else {
                cb(key, klen, -1, priv);
            }
        } else {
            rc = delete_from_peer(addr, key, klen, fd, 1);
            if (rc == 0)
                shardcache_release_connection_for_peer(cache, addr, fd);
            else
                close(fd);
        }
    }

    return rc;
}

int
shardcache_del(shardcache_t *cache,
               void *key,
               size_t klen,
               shardcache_async_response_callback_t cb,
               void *priv)
{
    if (!key || !key)
        return -1;

    if (cache->replica) {
        int rc = shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_DELETE, key, klen, NULL, 0, NULL, 0, 0, 0);
        if (cb)
            cb(key, klen, rc, priv);
        return rc;
    }

    return shardcache_del_internal(cache, key, klen, 0, cb, priv);
}

int
shardcache_del_multi(shardcache_t *cache,
                     void **keys,
                     size_t *klens,
                     int nkeys,
                     shardcache_async_response_callback_t cb,
                     void *priv)
{
    int i;

    if (!nkeys || !keys || !klens)
        return -1;

    // TODO - create different buckets grouping keys by owner so that we can 
    // call shardcache_del() to del local keys only, and forward the rest to the
    // proper peer using a DEL_MULTI command
    for (i = 0; i < nkeys; i++) {
        shardcache_del(cache,
                       keys[i],
                       klens[i],
                       cb,
                       priv);
    }

    return 0;
}

int
shardcache_evict(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !klen)
        return -1;

    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_EVICT, key, klen, NULL, 0, NULL, 0, 0, 0);

    arc_remove(cache->arc, (const void *)key, klen);

    return 0;
}

int
shardcache_evict_multi(shardcache_t *cache,
                     void **keys,
                     size_t *klens,
                     int nkeys,
                     shardcache_async_response_callback_t cb,
                     void *priv)
{
    int i;

    if (!nkeys || !keys || !klens)
        return -1;

    for (i = 0; i < nkeys; i++)
        shardcache_evict(cache, keys[i], klens[i]);

    return 0;
}

shardcache_node_t **
shardcache_get_nodes(shardcache_t *cache, int *num_nodes)
{
    int i;
    int num = 0;
    SPIN_LOCK(cache->migration_lock);
    num = cache->num_shards;
    if (num_nodes)
        *num_nodes = num;
    shardcache_node_t **list = malloc(sizeof(shardcache_node_t *) * num);
    for (i = 0; i < num; i++) {
        shardcache_node_t *orig = cache->shards[i];
        char *label = shardcache_node_get_label(orig);
        int num_replicas = shardcache_node_num_addresses(orig);
        char *addresses[num_replicas];
        shardcache_node_get_all_addresses(orig, addresses, num_replicas);
        list[i] = shardcache_node_create(label, addresses, num_replicas);
    }
    SPIN_UNLOCK(cache->migration_lock);
    return list;
}

void
shardcache_free_nodes(shardcache_node_t **nodes, int num_nodes)
{
    int i;
    for (i = 0; i < num_nodes; i++)
        shardcache_node_destroy(nodes[i]);
    free(nodes);
}

int
shardcache_get_counters(shardcache_t *cache, shardcache_counter_t **counters)
{
    return shardcache_get_all_counters(cache->counters, counters); 
}

void
shardcache_clear_counters(shardcache_t *cache)
{
    int i;
    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i++)
        ATOMIC_SET(cache->cnt[i].value, 0);
}

shardcache_storage_index_t *
shardcache_get_index(shardcache_t *cache)
{
    shardcache_storage_index_t *index = NULL;

    if (cache->use_persistent_storage) {
        size_t isize = 65535;
        if (cache->storage.count)
            isize = cache->storage.count(cache->storage.priv);

        ssize_t count = 0;
        shardcache_storage_index_item_t *items = NULL;
        if (cache->storage.index) {
            items = calloc(sizeof(shardcache_storage_index_item_t), isize);
            count = cache->storage.index(items, isize, cache->storage.priv);
        }
        index = calloc(1, sizeof(shardcache_storage_index_t));
        index->items = items;
        index->size = count; 
    }
    return index;
}

void
shardcache_free_index(shardcache_storage_index_t *index)
{
    if (index->items) {
        int i;
        for (i = 0; i < index->size; i++)
            free(index->items[i].key);
        free(index->items);
    }
    free(index);
}

static int
expire_migrated(hashtable_t *table, void *key, size_t klen, void *value, size_t vlen, void *user)
{
    shardcache_t *cache = (shardcache_t *)user;
    volatile_object_t *v = (volatile_object_t *)value;

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1) {
        SHC_WARNING("expire_migrated running while no migration continuum present ... aborting");
        return 0;
    } else if (!is_mine) {
        SHC_DEBUG("Forcing Key %.*s to expire because not owned anymore", klen, key);

        v->expire = 0;
    }
    return 1;
}


void *
migrate(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;

    shardcache_storage_index_t *index = shardcache_get_index(cache);
    int aborted = 0;
    linked_list_t *to_delete = list_create();

    uint64_t migrated_items = 0;
    uint64_t scanned_items = 0;
    uint64_t errors = 0;
    uint64_t total_items = 0;

    shardcache_thread_init(cache);

    if (index) {
        total_items = index->size;

        shardcache_counter_add(cache->counters, "migrated_items", &migrated_items);
        shardcache_counter_add(cache->counters, "scanned_items", &scanned_items);
        shardcache_counter_add(cache->counters, "total_items", &total_items);
        shardcache_counter_add(cache->counters, "migration_errors", &errors);

        SHC_INFO("Migrator starting (%d items to precess)", total_items);

        int i;
        for (i = 0; i < index->size; i++) {
            size_t klen = index->items[i].klen;
            void *key = index->items[i].key;

            char node_name[1024];
            size_t node_len = sizeof(node_name);
            memset(node_name, 0, node_len);

            SHC_DEBUG("Migrator processign key %.*s", klen, key);

            int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);

            int rc = 0;
            
            if (is_mine == -1) {
                SHC_WARNING("Migrator running while no migration continuum present ... aborting");
                ATOMIC_INCREMENT(errors);
                aborted = 1;
                break;
            } else if (!is_mine) {
                // if we are not the owner try asking our peer responsible for this data
                void *value = NULL;
                size_t vlen = 0;
                if (cache->storage.fetch) {
                    rc = cache->storage.fetch(key, klen, &value, &vlen, cache->storage.priv);
                    if (rc == -1) {
                        SHC_ERROR("Fetch storage callback retunrned an error during migration (%d)", rc);
                        ATOMIC_INCREMENT(errors);
                        ATOMIC_INCREMENT(scanned_items);
                        continue;
                    }
                }
                if (value) {
                    shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name);
                    if (peer) {
                        char *addr = shardcache_node_get_address(peer);
                        SHC_DEBUG("Migrator copying %.*s to peer %s (%s)", klen, key, node_name, addr);
                        int fd = shardcache_get_connection_for_peer(cache, addr);
                        rc = send_to_peer(addr, key, klen, value, vlen, 0, cache->expire_time, fd, 1);
                        if (rc == 0) {
                            shardcache_release_connection_for_peer(cache, addr, fd);
                            ATOMIC_INCREMENT(migrated_items);
                            list_push_value(to_delete, &index->items[i]);
                        } else {
                            close(fd);
                            SHC_WARNING("Errors copying %.*s to peer %s (%s)", klen, key, node_name, addr);
                            ATOMIC_INCREMENT(errors);
                        }
                    } else {
                        SHC_ERROR("Can't find address for peer %s (me : %s)", node_name, cache->me);
                        ATOMIC_INCREMENT(errors);
                    }
                }
            }
            ATOMIC_INCREMENT(scanned_items);
        }

        shardcache_counter_remove(cache->counters, "migrated_items");
        shardcache_counter_remove(cache->counters, "scanned_items");
        shardcache_counter_remove(cache->counters, "total_items");
        shardcache_counter_remove(cache->counters, "migration_errors");
    }

    if (!aborted) {
            SHC_INFO("Migration completed, now removing not-owned  items");
        shardcache_storage_index_item_t *item = list_shift_value(to_delete);
        while (item) {
            if (cache->storage.remove)
                cache->storage.remove(item->key, item->klen, cache->storage.priv);

            SHC_DEBUG2("removed item %.*s", item->klen, item->key);

            item = list_shift_value(to_delete);
        }

        // and now let's expire all the volatile keys that don't belong to us anymore
        ht_foreach_pair(cache->volatile_storage, expire_migrated, cache);
        //ATOMIC_SET(cache->next_expire, 0);
    }

    list_destroy(to_delete);

    SPIN_LOCK(cache->migration_lock);
    cache->migration_done = 1;
    SPIN_UNLOCK(cache->migration_lock);
    if (index) {
        SHC_INFO("Migrator ended: processed %d items, migrated %d, errors %d",
                total_items, migrated_items, errors);
    }

    if (index)
        shardcache_free_index(index);

    shardcache_thread_end(cache);
    return NULL;
}

static int
shardcache_check_migration_continuum(shardcache_t *cache,
                                     shardcache_node_t **nodes,
                                     int num_nodes)
{

    int ignore = 0;
    int i,n;

    if (num_nodes == cache->num_shards) {
        // let's assume the lists are the same, if not
        // ignore will be set again to 0
        ignore = 1;
        for (i = 0 ; i < num_nodes; i++) {
            int found = 0;
            for (n = 0; n < num_nodes; n++) {
                char *label1 = shardcache_node_get_label(nodes[i]);
                char *label2 = shardcache_node_get_label(cache->shards[n]);
                if (*label1 == *label2 && strcmp(label1, label2) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                // the lists differ, we don't want to ignore the request
                ignore = 0;
                break;
            }
        }
    }

    return ignore;
}

int
shardcache_set_migration_continuum(shardcache_t *cache,
                                   shardcache_node_t **nodes,
                                   int num_nodes)
{
    size_t shard_lens[num_nodes];
    char *shard_names[num_nodes];

    SPIN_LOCK(cache->migration_lock);

    if (cache->migration) {
        // already in a migration, ignore this command
        SPIN_UNLOCK(cache->migration_lock);
        return -1;
    }

    if (shardcache_check_migration_continuum(cache, nodes, num_nodes) != 0) {
        SPIN_UNLOCK(cache->migration_lock);
        return -1;
    }

    cache->migration_done = 0;
    cache->migration_shards = malloc(sizeof(shardcache_node_t *) * num_nodes);
    cache->num_migration_shards = num_nodes;
    int i;
    for (i = 0; i < cache->num_migration_shards; i++) {
        shardcache_node_t *node = nodes[i];
        shard_names[i] = shardcache_node_get_label(node);
        int num_replicas = shardcache_node_num_addresses(node);
        char *addresses[num_replicas];
        shardcache_node_get_all_addresses(node, addresses, num_replicas);
        cache->migration_shards[i] = shardcache_node_create(shard_names[i], addresses, num_replicas);
        shard_lens[i] = strlen(shard_names[i]);
    }

    cache->migration = chash_create((const char **)shard_names,
                                    shard_lens,
                                    num_nodes,
                                    200);

    SPIN_UNLOCK(cache->migration_lock);
    return 0;
}

static int
shardcache_migration_begin_internal(shardcache_t *cache,
                                    shardcache_node_t **nodes,
                                    int num_nodes,
                                    int forward)
{
    int i;

    if (shardcache_set_migration_continuum(cache, nodes, num_nodes) != 0) {
        SPIN_UNLOCK(cache->migration_lock);
        return -1;
    }

    SHC_NOTICE("Starting migration");

    pthread_create(&cache->migrate_th, NULL, migrate, cache);

    if (forward) {
        fbuf_t mgb_message = FBUF_STATIC_INITIALIZER;

        for (i = 0; i < num_nodes; i++) {
            if (i > 0) 
                fbuf_add(&mgb_message, ",");
            int num_replicas = shardcache_node_num_addresses(nodes[i]);
            char *label = shardcache_node_get_label(nodes[i]);
            int rindex = random()%num_replicas;
            char *addr = shardcache_node_get_address_at_index(nodes[i], rindex);
            fbuf_printf(&mgb_message, "%s:%s", label, addr);
        }

        for (i = 0; i < cache->num_shards; i++) {
            if (strcmp(shardcache_node_get_label(cache->shards[i]), cache->me) != 0) {
                int num_replicas = shardcache_node_num_addresses(nodes[i]);
                char *label = shardcache_node_get_label(nodes[i]);
                int rindex = random()%num_replicas;
                char *addr = shardcache_node_get_address_at_index(nodes[i], rindex);
                int fd = shardcache_get_connection_for_peer(cache, addr);
                int rc = migrate_peer(addr,
                                      fbuf_data(&mgb_message),
                                      fbuf_used(&mgb_message), fd);
                shardcache_release_connection_for_peer(cache, addr, fd);
                if (rc != 0) {
                    SHC_ERROR("Node %s (%s) didn't aknowledge the migration",
                              label, addr);
                }
            }
        }
        fbuf_destroy(&mgb_message);
    }

    return 0;
}


int
shardcache_migration_begin(shardcache_t *cache,
                           shardcache_node_t **nodes,
                           int num_nodes,
                           int forward)
{
    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_MIGRATION_BEGIN, NULL, 0, nodes, num_nodes, NULL, 0, 0, 0);
    return shardcache_migration_begin_internal(cache, nodes, num_nodes, forward);
}

static int
shardcache_migration_abort_internal(shardcache_t *cache)
{
    int ret = -1;
    SPIN_LOCK(cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->migration);
        free(cache->migration_shards);
        SHC_NOTICE("Migration aborted");
        ret = 0;
    }
    cache->migration = NULL;
    cache->migration_shards = NULL;
    cache->num_migration_shards = 0;

    SPIN_UNLOCK(cache->migration_lock);
    pthread_join(cache->migrate_th, NULL);
    return ret;
}

int
shardcache_migration_abort(shardcache_t *cache)
{
    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_MIGRATION_ABORT, NULL, 0, NULL, 0, NULL, 0, 0, 0);
    return shardcache_migration_abort_internal(cache);
}

static int
shardcache_migration_end_internal(shardcache_t *cache)
{
    int ret = -1;
    SPIN_LOCK(cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->chash);
        shardcache_free_nodes(cache->shards, cache->num_shards);
        cache->chash = cache->migration;
        cache->shards = cache->migration_shards;
        cache->num_shards = cache->num_migration_shards;
        cache->migration = NULL;
        cache->migration_shards = NULL;
        cache->num_migration_shards = 0;
        SHC_NOTICE("Migration ended");
        ret = 0;
    }
    cache->migration_done = 0;
    SPIN_UNLOCK(cache->migration_lock);
    pthread_join(cache->migrate_th, NULL);
    return ret;
}

int
shardcache_migration_end(shardcache_t *cache)
{
    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_MIGRATION_END, NULL, 0, NULL, 0, NULL, 0, 0, 0);
    return shardcache_migration_end_internal(cache);
}

int
shardcache_tcp_timeout(shardcache_t *cache, int new_value)
{
    if (new_value == 0)
        new_value = SHARDCACHE_TCP_TIMEOUT_DEFAULT;

    if (new_value > 0)
        global_tcp_timeout(new_value);

    return connections_pool_tcp_timeout(cache->connections_pool, new_value);
}

int
shardcache_conn_expire_time(shardcache_t *cache, int new_value)
{
    if (new_value == 0)
        new_value = SHARDCACHE_CONNECTION_EXPIRE_DEFAULT;

    return connections_pool_expire_time(cache->connections_pool, new_value);
}

static inline int
shardcache_get_set_option(int *option, int new_value)
{
    int old_value = ATOMIC_READ(*option);

    if (new_value >= 0)
        ATOMIC_SET(*option, new_value);

    return old_value;
}

int
shardcache_use_persistent_connections(shardcache_t *cache, int new_value)
{
    return shardcache_get_set_option(&cache->use_persistent_connections, new_value);
}

int
shardcache_arc_mode(shardcache_t *cache, arc_mode_t new_value)
{
    int old_value = shardcache_get_set_option(&cache->arc_mode, (int)new_value);
    if ((int)new_value != -1 && old_value != new_value)
        arc_set_mode(cache->arc, new_value);
    return old_value;
}

int
shardcache_cache_on_set(shardcache_t *cache, int new_value)
{
    return shardcache_get_set_option(&cache->cache_on_set, new_value);
}

int
shardcache_evict_on_delete(shardcache_t *cache, int new_value)
{
    return shardcache_get_set_option(&cache->evict_on_delete, new_value);
}

int
shardcache_force_caching(shardcache_t *cache, int new_value)
{
    return shardcache_get_set_option(&cache->force_caching, new_value);
}

int
shardcache_iomux_run_timeout_low(shardcache_t *cache, int new_value)
{
    if (new_value == 0)
        new_value = SHARDCACHE_IOMUX_RUN_TIMEOUT_LOW;
    return shardcache_get_set_option(&cache->iomux_run_timeout_low, new_value);
}

int
shardcache_iomux_run_timeout_high(shardcache_t *cache, int new_value)
{
    if (new_value == 0)
        new_value = SHARDCACHE_IOMUX_RUN_TIMEOUT_HIGH;
    return shardcache_get_set_option(&cache->iomux_run_timeout_high, new_value);
}

int
shardcache_expire_time(shardcache_t *cache, int new_value)
{
    return shardcache_get_set_option(&cache->expire_time, new_value);
}

int
shardcache_serving_look_ahead(shardcache_t *cache, int new_value)
{
    return shardcache_get_set_option(&cache->serving_look_ahead, new_value);
}

int
shardcache_lazy_expiration(shardcache_t *cache, int new_value)
{
    return shardcache_get_set_option(&cache->lazy_expiration, new_value);
}

void shardcache_thread_init(shardcache_t *cache)
{
    if (cache->storage.thread_start)
        cache->storage.thread_start(cache->storage.priv);
}

void shardcache_thread_end(shardcache_t *cache)
{
    if (cache->storage.thread_exit)
        cache->storage.thread_exit(cache->storage.priv);
}

shardcache_storage_t *
shardcache_storage_load(char *filename, char **options)
{
    shardcache_storage_t *st = calloc(1, sizeof(shardcache_storage_t));

    // initialize the storage layer 
    int initialized = -1;
    st->internal.handle = dlopen(filename, RTLD_NOW);
    if (!st->internal.handle) {
        SHC_ERROR("Can't open the storage module: %s (%s)\n", filename, dlerror());
        free(st);
        return NULL;
    }
    char *error = NULL;

    int *version = dlsym(st->internal.handle, "storage_version");
    if (!version || ((error = dlerror()) != NULL)) {
        if (error)
            SHC_ERROR("%s", error);
        else
            SHC_ERROR("Can't find the symbol 'storage_version' in the loaded module");

        dlclose(st->internal.handle);
        free(st);
        return NULL;
    }

    if (*version != SHARDCACHE_STORAGE_API_VERSION) {
        SHC_ERROR("The storage plugin version doesn't match (%d != %d)",
                    *version, SHARDCACHE_STORAGE_API_VERSION);
        dlclose(st->internal.handle);
        free(st);
        return NULL;
    }
    st->version = *version;

    st->internal.init = dlsym(st->internal.handle, "storage_init");
    if (!st->internal.init || ((error = dlerror()) != NULL))  {
        if (error)
            SHC_ERROR("%s", error);
        else
            SHC_ERROR("Can't find the symbol 'storage_init' in the loaded module");
        dlclose(st->internal.handle);
        free(st);
        return NULL;
    }

    st->internal.destroy = dlsym(st->internal.handle, "storage_destroy");
    if (!st->internal.destroy || ((error = dlerror()) != NULL))  {
        if (error)
            SHC_ERROR("%s", error);
        else
            SHC_ERROR("Can't find the symbol 'storage_destroy' in the loaded module");
        dlclose(st->internal.handle);
        free(st);
        return NULL;
    }

    st->internal.reset = dlsym(st->internal.handle, "storage_reset");
    
    initialized = st->internal.init(st, options);
    if (initialized != 0) {
        SHC_ERROR("Can't init the storage module: %s\n", filename);
        free(st);
        return NULL;
    }
    return st;
}

void
shardcache_storage_dispose(shardcache_storage_t *st)
{
    if (st->internal.destroy)
       st->internal.destroy(st->priv); 

    if (st->internal.handle)
        dlclose(st->internal.handle);

    free(st);
}

int
shardcache_storage_reset(shardcache_storage_t *st, char **options)
{
    if (st->internal.reset)
        return st->internal.reset(options, st->priv);
    return -1;
}

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
