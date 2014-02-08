#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>

#include "shardcache.h"
#include "messaging.h"
#include "arc_ops.h"
#include "shardcache_internal.h"

/**
 * * Here are the operations implemented
 *
 * */

typedef struct {
    cache_object_t *obj;
    void *data;
    size_t len;
} shardcache_fetch_from_peer_notify_arg;

static int
arc_ops_fetch_from_peer_notify_listener (void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    shardcache_fetch_from_peer_notify_arg *arg = (shardcache_fetch_from_peer_notify_arg *)user;
    cache_object_t *obj = arg->obj;
    int rc = (listener->cb(obj->key, obj->klen, arg->data, arg->len, 0, NULL, listener->priv) == 0);
    if (!rc) {
        free(listener);
        return -1;
    }
    return 1;
}

static int
arc_ops_fetch_from_peer_notify_listener_complete(void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    cache_object_t *obj = (cache_object_t *)user;
    listener->cb(obj->key, obj->klen, NULL, 0, obj->dlen, &obj->ts, listener->priv);
    free(listener);
    return -1;
}

static int
arc_ops_fetch_from_peer_notify_listener_error(void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    cache_object_t *obj = (cache_object_t *)user;
    listener->cb(obj->key, obj->klen, NULL, 0, 0, NULL, listener->priv);
    free(listener);
    return -1;
}

static void
arc_ops_evict_object(shardcache_t *cache, cache_object_t *obj)
{
    if (list_count(obj->listeners)) {
        obj->evict = 1;
        return;
    }
    if (obj->data) {
        free(obj->data);
        obj->data = NULL;
        obj->dlen = 0;
        obj->complete = 0;
        ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_EVICTS].value);
    }
    obj->async = 0;
    obj->evict = 0;
    clear_list(obj->listeners);
}

typedef struct
{
    cache_object_t *obj;
    shardcache_t *cache;
    char *peer_addr;
    int fd;
} shc_fetch_async_arg_t;

static int
arc_ops_fetch_from_peer_async_cb(char *peer,
                              void *key,
                              size_t klen,
                              void *data,
                              size_t len,
                              int error,
                              void *priv)
{
    shc_fetch_async_arg_t *arg = (shc_fetch_async_arg_t *)priv;
    cache_object_t *obj = arg->obj;
    shardcache_t *cache = arg->cache;
    char *peer_addr = arg->peer_addr;
    int fd = arg->fd;
    int complete = 0;
    int total_len = 0;

    pthread_mutex_lock(&obj->lock);
    if (!obj->listeners) {
        pthread_mutex_unlock(&obj->lock);
        return -1;
    }
    if (error) {
        foreach_list_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_error, obj);
        arc_remove(obj->arc, obj->key, obj->klen);
        shardcache_release_connection_for_peer(cache, peer_addr, fd);
        free(arg);
        if (obj->evict)
            arc_ops_evict_object(cache, obj);
    } else if (len) {
        obj->data = realloc(obj->data, obj->dlen + len);
        memcpy(obj->data + obj->dlen, data, len);
        obj->dlen += len;
        shardcache_fetch_from_peer_notify_arg arg = {
            .obj = obj,
            .data = data,
            .len = len
        };
        foreach_list_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener, &arg);
    } else {
        foreach_list_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_complete, obj);
        obj->complete = 1;
        complete = 1;
        total_len = obj->dlen;
        shardcache_release_connection_for_peer(cache, peer_addr, fd);
        free(arg);
        if (obj->evict)
            arc_ops_evict_object(cache, obj);
    }
    pthread_mutex_unlock(&obj->lock);

    if (complete) {
        if (total_len)
            arc_update_size(cache->arc, key, klen, total_len);
        else
            arc_remove(cache->arc, key, klen);
    }
    return !error ? 0 : -1;
}


static int
arc_ops_fetch_from_peer(shardcache_t *cache, cache_object_t *obj, char *peer)
{
    int rc = -1;
    if (shardcache_log_level() >= LOG_DEBUG) {
        char keystr[1024];
        KEY2STR(obj->key, obj->klen, keystr, sizeof(keystr));
        SHC_DEBUG("Fetching data for key %s from peer %s", keystr, peer); 
    }

    char *peer_addr = shardcache_get_node_address(cache, peer);
    if (!peer_addr) {
        SHC_ERROR("Can't find address for node %s\n", peer);
        return rc;
    }

    // another peer is responsible for this item, let's get the value from there

    int fd = shardcache_get_connection_for_peer(cache, peer_addr);
    if (obj->async) {
        shc_fetch_async_arg_t *arg = malloc(sizeof(shc_fetch_async_arg_t));
        arg->obj = obj;
        arg->cache = cache;
        arg->peer_addr = peer_addr;
        arg->fd = fd;
        rc = fetch_from_peer_async(peer_addr,
                                   (char *)cache->auth,
                                   SHC_HDR_CSIGNATURE_SIP,
                                   obj->key,
                                   obj->klen,
                                   arc_ops_fetch_from_peer_async_cb,
                                   arg,
                                   fd,
                                   cache->async_mux);
        if (rc == 0) {
            ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value);
        } else {
            foreach_list_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_error, obj);
            arc_remove(cache->arc, obj->key, obj->klen);
            shardcache_release_connection_for_peer(cache, peer_addr, fd);
            if (obj->evict)
                arc_ops_evict_object(cache, obj);
        }
    } else { 
        fbuf_t value = FBUF_STATIC_INITIALIZER;
        rc = fetch_from_peer(peer_addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, obj->key, obj->klen, &value, fd);
        if (rc == 0 && fbuf_used(&value)) {
            obj->data = fbuf_data(&value);
            obj->dlen = fbuf_used(&value);
            ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value);
            obj->complete = 1;
        }
        shardcache_release_connection_for_peer(cache, peer_addr, fd);
        fbuf_destroy(&value);
    }

    return rc;
}

void *
arc_ops_create(const void *key, size_t len, int async, void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    cache_object_t *obj = calloc(1, sizeof(cache_object_t));

    obj->klen = len;
    obj->key = malloc(len);
    memcpy(obj->key, key, len);
    obj->data = NULL;
    obj->complete = 0;
    if (async) {
        obj->async = async;
        obj->listeners = create_list();
        set_free_value_callback(obj->listeners, free);
    }
    pthread_mutex_init(&obj->lock, NULL);
    obj->arc = cache->arc;

    return obj;
}

static void *
arc_ops_fetch_copy_volatile_object_cb(void *ptr, size_t len)
{
    volatile_object_t *item = (volatile_object_t *)ptr;
    volatile_object_t *copy = malloc(sizeof(volatile_object_t));
    copy->data = malloc(item->dlen);
    memcpy(copy->data, item->data, item->dlen);
    copy->dlen = item->dlen;
    copy->expire = item->expire;
    return copy;
}

size_t
arc_ops_fetch(void *item, void * priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;

    pthread_mutex_lock(&obj->lock);
    if (obj->data) { // the value is already loaded, we don't need to fetch
        pthread_mutex_unlock(&obj->lock);
        return obj->dlen;
    }

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    // if we are not the owner try asking to the peer responsible for this data
    if (!shardcache_test_ownership(cache, obj->key, obj->klen, node_name, &node_len))
    {
        int done = 1;
        int ret = arc_ops_fetch_from_peer(cache, obj, node_name);
        if (ret == -1) {
            int check = shardcache_test_migration_ownership(cache,
                                                            obj->key,
                                                            obj->klen,
                                                            node_name,
                                                            &node_len);
            if (check == 0)
                ret = arc_ops_fetch_from_peer(cache, obj, node_name);
            else if (check == 1 || cache->storage.shared)
                done = 0;
        }
        if (done) {
            if (ret == 0) {
                gettimeofday(&obj->ts, NULL);
                size_t dlen = obj->dlen;
                pthread_mutex_unlock(&obj->lock);
                return dlen;
            }
            return UINT_MAX;
        }
    }

    char keystr[1024];
    if (shardcache_log_level() >= LOG_DEBUG)
        KEY2STR(obj->key, obj->klen, keystr, sizeof(keystr));

    // we are responsible for this item ... 
    // let's first check if it's among the volatile keys otherwise
    // fetch it from the storage
    volatile_object_t *vobj = ht_get_deep_copy(cache->volatile_storage,
                                               obj->key,
                                               obj->klen,
                                               NULL,
                                               arc_ops_fetch_copy_volatile_object_cb);
    if (vobj) {
        obj->data = vobj->data; 
        obj->dlen = vobj->dlen;
        free(vobj);
        if (shardcache_log_level() >= LOG_DEBUG) {
            if (obj->data && obj->dlen) {
                SHC_DEBUG2("Found volatile value %s (%lu) for key %s",
                       shardcache_hex_escape(obj->data, obj->dlen, DEBUG_DUMP_MAXSIZE),
                       (unsigned long)obj->dlen, keystr);
            }
        }
    } else if (cache->use_persistent_storage && cache->storage.fetch) {
        obj->data = cache->storage.fetch(obj->key, obj->klen, &obj->dlen, cache->storage.priv);

        if (shardcache_log_level() >= LOG_DEBUG) {
            if (obj->data && obj->dlen) {
                SHC_DEBUG2("Fetch storage callback returned value %s (%lu) for key %s",
                       shardcache_hex_escape(obj->data, obj->dlen, DEBUG_DUMP_MAXSIZE),
                       (unsigned long)obj->dlen, keystr);
            } else {
                SHC_DEBUG2("Fetch storage callback returned an empty value for key %s", keystr);
            }
        }
    }

    if (!obj->data) {
        pthread_mutex_unlock(&obj->lock);
        if (shardcache_log_level() >= LOG_DEBUG)
            SHC_DEBUG("Item not found for key %s", keystr);
        ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_NOT_FOUND].value);
        return UINT_MAX;
    }
    gettimeofday(&obj->ts, NULL);

    obj->complete = 1;

    if (obj->async) {
        foreach_list_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_complete, obj);
        if (obj->evict)
            arc_ops_evict_object(cache, obj);
    }

    size_t dlen = obj->dlen;

    pthread_mutex_unlock(&obj->lock);
    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value);

    return dlen;
}

void
arc_ops_evict(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;
    pthread_mutex_lock(&obj->lock);
    arc_ops_evict_object(cache, obj);
    pthread_mutex_unlock(&obj->lock);
}

void
arc_ops_destroy(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;

    // no lock is necessary here ... if we are here
    // nobody is referencing us anymore
    if (obj->data) {
        free(obj->data);
    }
    if (obj->key)
        free(obj->key);

    if (obj->listeners)
        destroy_list(obj->listeners);

    pthread_mutex_destroy(&obj->lock);
    free(obj);
}


