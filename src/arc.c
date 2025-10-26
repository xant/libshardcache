#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <limits.h>
#include <memory.h>
#include <stddef.h>

#include <hashtable.h>
#include <refcnt.h>

#include "shardcache_internal.h" // for MUTEX_* macros

#include "arc.h"

#ifdef USE_PACKED_STRUCTURES
#define PACK_IF_NECESSARY __attribute__((packed))
#else
#define PACK_IF_NECESSARY
#endif

/**********************************************************************
 * Simple double-linked list, inspired by the implementation used in the
 * linux kernel.
 */
typedef struct _arc_list {
    struct _arc_list *prev, *next;
} PACK_IF_NECESSARY arc_list_t;

#define arc_list_entry(ptr, type, field) \
    ((type*) (((char*)ptr) - offsetof(type, field)))

#define arc_list_each(pos, head) \
    for (pos = (head)->next; pos && pos != (head); pos = pos->next)

#define arc_list_each_prev(pos, head) \
    for (pos = (head)->prev; pos && pos != (head); pos = pos->prev)

/**********************************************************************
 * The arc state represents one of the m{r,f}u{g,} lists
 */
typedef struct _arc_state {
    arc_list_t head;
    uint64_t size; // note must be accessed only via atomic functions
    uint64_t count; // note must be accessed only via atomic functions
} PACK_IF_NECESSARY arc_state_t;

/* This structure represents an object that is stored in the cache. Consider
 * this structure private, don't access the fields directly. When creating
 * a new object, use the arc_object_create() function to allocate and initialize it. */
typedef struct _arc_object {
    arc_state_t *state;
    arc_list_t head;
    size_t size;
    void *ptr;
    char buf[32];
    void *key;
    size_t klen;
    refcnt_node_t *node;
    int async;
    int locked;
} PACK_IF_NECESSARY arc_object_t;

/* The actual cache. */
struct _arc {
    struct _arc_ops *ops;
    hashtable_t *hash;

    size_t c, p;
    size_t cos;
    struct _arc_state mrug, mru, mfu, mfug;

    int needs_balance;
    int mode;

    pthread_mutex_t lock;

    refcnt_t *refcnt;
};


#define MAX(a, b) ( (a) > (b) ? (a) : (b) )
#define MIN(a, b) ( (a) < (b) ? (a) : (b) )

#define ARC_OBJ_BASE_SIZE(o) (sizeof(arc_object_t) + (((o)->key == (o)->buf) ? 0 : (o)->klen))

static int arc_move(arc_t *cache, arc_object_t *obj, arc_state_t *state);



static inline void
arc_list_init( arc_list_t * head )
{
    head->next = head->prev = head;
}

static inline void
arc_list_destroy(arc_t *cache, arc_list_t *head)
{
    arc_list_t *pos = (head)->next;
    while (pos && pos != (head)) {
        arc_list_t *tmp = pos;
        arc_object_t *obj = arc_list_entry(pos, arc_object_t, head);
        pos = pos->next;
        tmp->prev = tmp->next = NULL;
        release_ref(cache->refcnt, obj->node);
    }
}

static inline void
arc_list_insert(arc_list_t *list, arc_list_t *prev, arc_list_t *next)
{
    next->prev = list;
    list->next = next;
    list->prev = prev;
    prev->next = list;
}

static inline void
arc_list_splice(arc_list_t *prev, arc_list_t *next)
{
    next->prev = prev;
    prev->next = next;
}

static inline void
arc_list_remove(arc_list_t *head)
{
    if (head->prev && head->next) {
        arc_list_splice(head->prev, head->next);
    }
    head->next = head->prev = NULL;
}

static inline void
arc_list_prepend(arc_list_t *head, arc_list_t *list)
{
    arc_list_insert(head, list, list->next);
}

static inline void
arc_list_move_to_head(arc_list_t *head, arc_list_t *list)
{
    arc_list_t *next = head->next;
    arc_list_t *prev = head->prev;
    if (prev)
        prev->next = next;
    if (next)
        next->prev = prev;
    arc_list_prepend(head, list);
}

/* Return the LRU element from the given state. */
static inline arc_object_t *
arc_state_lru(arc_state_t *state)
{
    arc_list_t *head = state->head.prev;
    return arc_list_entry(head, arc_object_t, head);
}

/* Balance the lists so that we can fit an object with the given size into
 * the cache. */
static inline void
arc_balance(arc_t *cache)
{
    if (!ATOMIC_READ(cache->needs_balance))
        return;

    MUTEX_LOCK(cache->lock);
    /* First move objects from MRU/MFU to their respective ghost lists. */
    while (cache->mru.size + cache->mfu.size > ATOMIC_READ(cache->c)) {
        if (cache->mru.size > cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mru);
            arc_move(cache, obj, &cache->mrug);
        } else if (cache->mfu.size > ATOMIC_READ(cache->c) - cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mfu);
            arc_move(cache, obj, &cache->mfug);
        } else {
            break;
        }
    }

    /* Then start removing objects from the ghost lists. */
    while (cache->mrug.size + cache->mfug.size > ATOMIC_READ(cache->c)) {
        if (cache->mfug.size > cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mfug);
            arc_move(cache, obj, NULL);
        } else if (cache->mrug.size > ATOMIC_READ(cache->c) - cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mrug);
            arc_move(cache, obj, NULL);
        } else {
            break;
        }
    }

    ATOMIC_SET(cache->needs_balance, 0);
    MUTEX_UNLOCK(cache->lock);
}

void
arc_update_resource_size(arc_t *cache, arc_resource_t res, size_t size)
{
    arc_object_t *obj = (arc_object_t *)res;
    if (obj) {
        MUTEX_LOCK(cache->lock);
        arc_state_t *state = ATOMIC_READ(obj->state);
        if (LIKELY(state == &cache->mru || state == &cache->mfu)) {
            ATOMIC_DECREASE(state->size, obj->size);
            obj->size = ARC_OBJ_BASE_SIZE(obj) + cache->cos + size;
            ATOMIC_INCREASE(state->size, obj->size);
        }
        ATOMIC_INCREMENT(cache->needs_balance);
        MUTEX_UNLOCK(cache->lock);
    }
}

/* Move the object to the given state. If the state transition requires,
* fetch, evict or destroy the object. */
static inline int
arc_move(arc_t *cache, arc_object_t *obj, arc_state_t *state)
{
    // In the first conditional we check If the object is being locked,
    // which means someone is fetching its value and we don't what
    // don't mess up with it. Whoever is fetching will also take care of moving it
    // to one of the lists (or dropping it)
    // NOTE: while the object is being fetched it doesn't belong
    //       to any list, so there is no point in going ahead
    //       also arc_balance() should never go through this object
    //       (since in none of the lists) so it won't be affected.
    //       The only call which would silently fail is arc_remove()
    //       but if the object is being fetched and need to be removed
    //       will be determined by who is fetching the object or by the
    //       next call to arc_balance() (which would anyway happen if
    //       the object will be put into the cache by the fetcher)
    //
    // In the second conditional instead we handle a specific corner case which
    // happens when concurring threads access an item which has been just fetched
    // but also dropped (so its state is NULL).
    // If a thread entering arc_lookup() manages to get the object out of the hashtable
    // before it's being deleted it will try putting the object to the mfu list without checking first
    // if it was already in a list or not (new objects should be first moved to the 
    // mru list and not the mfu one)
    if (UNLIKELY(obj->locked || (state == &cache->mfu && ATOMIC_READ(obj->state) == NULL)))
        return 0;

    MUTEX_LOCK(cache->lock);

    arc_state_t *obj_state = ATOMIC_READ(obj->state);

    if (LIKELY(obj_state != NULL)) {

        if (LIKELY(obj_state == state)) {
            // short path for recurring keys
            // (those in the mfu list being hit again)
            if (LIKELY(state->head.next != &obj->head))
                arc_list_move_to_head(&obj->head, &state->head);
            MUTEX_UNLOCK(cache->lock);
            return 0;
        }

        // if the state is not NULL
        // (and the object is not going to be being removed)
        // move the ^ (p) marker
        if (LIKELY(state != NULL)) {
            if (obj_state == &cache->mrug) {
                size_t csize = cache->mrug.size
                             ? (cache->mfug.size / cache->mrug.size)
                             : cache->mfug.size / 2;
                cache->p = MIN(ATOMIC_READ(cache->c), cache->p + MAX(csize, 1));
            } else if (obj_state == &cache->mfug) {
                size_t csize = cache->mfug.size
                             ? (cache->mrug.size / cache->mfug.size)
                             : cache->mrug.size / 2;
                size_t diff = MAX(csize, 1);
                if (cache->p > diff)
                    cache->p -= diff;
                else
                    cache->p = 0;
            }
        }

        ATOMIC_DECREASE(obj_state->size, obj->size);
        arc_list_remove(&obj->head);
        ATOMIC_DECREMENT(obj_state->count);
        ATOMIC_SET(obj->state, NULL);
    }

    if (state == NULL) {
        if (ht_delete_if_equals(ATOMIC_READ(cache->hash), (void *)obj->key, obj->klen, obj, sizeof(arc_object_t)) == 0)
            release_ref(cache->refcnt, obj->node);
    } else if (state == &cache->mrug || state == &cache->mfug) {
        obj->async = 0;
        arc_list_prepend(&obj->head, &state->head);
        ATOMIC_INCREMENT(state->count);
        ATOMIC_SET(obj->state, state);
        ATOMIC_INCREASE(state->size, obj->size);
    } else if (obj_state == NULL) {

        obj->locked = 1;
        
        // unlock the cache while the backend is fetching the data
        // (the object has been locked while being fetched so nobody
        // will change its state)
        MUTEX_UNLOCK(cache->lock);
        size_t size = 0;
        int rc = cache->ops->fetch(obj->ptr, &size, cache->ops->priv);
        switch (rc) {
            case 1:
            case -1:
            {
                if (ht_delete_if_equals(ATOMIC_READ(cache->hash), (void *)obj->key, obj->klen, obj, sizeof(arc_object_t)) == 0)
                    release_ref(cache->refcnt, obj->node);
                return rc;
            }
            default:
            {
                if (size >= ATOMIC_READ(cache->c)) {
                    // the (single) object doesn't fit in the cache, let's return it
                    // to the getter without (re)adding it to the cache
                    if (ht_delete_if_equals(ATOMIC_READ(cache->hash), (void *)obj->key, obj->klen, obj, sizeof(arc_object_t)) == 0)
                        release_ref(cache->refcnt, obj->node);
                    return 1;
                }
                MUTEX_LOCK(cache->lock);
                obj->size = ARC_OBJ_BASE_SIZE(obj) + cache->cos + size;
                arc_list_prepend(&obj->head, &state->head);
                ATOMIC_INCREMENT(state->count);
                ATOMIC_SET(obj->state, state);
                ATOMIC_INCREASE(state->size, obj->size);
                ATOMIC_INCREMENT(cache->needs_balance);
                break;
            }
        }
        // since this object is going to be put back into the cache,
        // we need to unmark it so that it won't be ignored next time
        // it's going to be moved to another list
        obj->locked = 0;
    } else {
        arc_list_prepend(&obj->head, &state->head);
        ATOMIC_INCREMENT(state->count);
        ATOMIC_SET(obj->state, state);
        ATOMIC_INCREASE(state->size, obj->size);
    }
    MUTEX_UNLOCK(cache->lock);
    return 0;
}

// this is called when the refcnt garbage collector actually requests us t
// release the memory for a node
static void
free_node_ptr_callback(void *node)
{
    // we don't need locks here .... nobody references obj anymore
    arc_object_t *obj = (arc_object_t *)node;

    if (obj->key != obj->buf)
        free(obj->key);

    free(obj);
}

// this is called when the refcount of the node drops to 0
// (and the node itself is being put into the garbage collector's
// queue by the refcnt manager)
static void
terminate_node_callback(refcnt_node_t *node, void *priv)
{
    arc_object_t *obj = (arc_object_t *)get_node_ptr(node);
    arc_t *cache = (arc_t *)priv;

    if (obj->ptr && cache->ops->evict)
        cache->ops->evict(obj->ptr, cache->ops->priv);

    obj->ptr = NULL;
    obj->state = NULL;
}

/* Create a new cache. */
arc_t *
arc_create(arc_ops_t *ops, size_t c, size_t cached_object_size, uint64_t *lists_size[4], arc_mode_t mode)
{
    arc_t *cache = calloc(1, sizeof(arc_t));

    cache->mode = mode;

    cache->ops = ops;

    cache->hash = ht_create(1<<16, 1<<25, NULL);

    cache->c = c >> 1;
    cache->p = cache->c >> 1;
    cache->cos = cached_object_size;

    arc_list_init(&cache->mrug.head);
    arc_list_init(&cache->mru.head);
    arc_list_init(&cache->mfu.head);
    arc_list_init(&cache->mfug.head);

    lists_size[0] = &cache->mru.size;
    lists_size[1] = &cache->mfu.size;
    lists_size[2] = &cache->mrug.size;
    lists_size[3] = &cache->mfug.size;

    MUTEX_INIT_RECURSIVE(cache->lock);

    cache->refcnt = refcnt_create(1<<8, terminate_node_callback, free_node_ptr_callback);
    return cache;
}

/* Destroy the given cache. Free all objects which remain in the cache. */
void
arc_destroy(arc_t *cache)
{
    arc_list_destroy(cache, &cache->mrug.head);
    arc_list_destroy(cache, &cache->mru.head);
    arc_list_destroy(cache, &cache->mfu.head);
    arc_list_destroy(cache, &cache->mfug.head);
    ht_destroy(cache->hash);
    refcnt_destroy(cache->refcnt);
    MUTEX_DESTROY(cache->lock);
    free(cache);
}

void
arc_clear(arc_t *cache)
{
    MUTEX_LOCK(cache->lock);
    hashtable_t *new_table = ht_create(1<<16, 1<<25, NULL);
    hashtable_t *old_table = ATOMIC_READ(cache->hash);
    if (ATOMIC_CAS(cache->hash, old_table, new_table)) {
        arc_list_destroy(cache, &cache->mrug.head);
        arc_list_destroy(cache, &cache->mfug.head);
        arc_list_destroy(cache, &cache->mru.head);
        arc_list_destroy(cache, &cache->mfu.head);
        ht_destroy(old_table);
    }
    MUTEX_UNLOCK(cache->lock);
}

void
arc_set_size(arc_t *cache, size_t size)
{
    ATOMIC_SET(cache->c, size >> 1);
}

static void *
retain_obj_cb(void *data, size_t dlen, void *user)
{
    retain_ref(((arc_t *)user)->refcnt, ((arc_object_t *)data)->node);
    return data;
}

void
arc_drop_resource(arc_t *cache, arc_resource_t res)
{
    arc_object_t *obj = (arc_object_t *)res;
    if (obj) {
        arc_move(cache, obj, NULL);
        release_ref(cache->refcnt, obj->node);
    }
}

void
arc_remove(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = ht_get_deep_copy(cache->hash, (void *)key, len, NULL, retain_obj_cb, cache);
    if (obj) {
        arc_move(cache, obj, NULL);
        release_ref(cache->refcnt, obj->node);
    }
}

/* Lookup an object with the given key. */
void
arc_release_resource(arc_t *cache, arc_resource_t res)
{
    arc_object_t *obj = (arc_object_t *)res;
    release_ref(cache->refcnt, obj->node);
}

/* Lookup an object with the given key. */
void
arc_retain_resource(arc_t *cache, arc_resource_t res)
{
    arc_object_t *obj = (arc_object_t *)res;

    retain_ref(cache->refcnt, obj->node);
}

/* Initialize a new object with this function. */
static inline arc_object_t *
arc_object_create(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = calloc(1, sizeof(arc_object_t) + cache->cos);

    arc_list_init(&obj->head);

    obj->node = new_node(cache->refcnt, obj, cache);
    if (len > sizeof(obj->buf))
        obj->key = malloc(len);
    else
        obj->key = obj->buf;
    memcpy(obj->key, key, len);
    obj->klen = len;

    obj->size = ARC_OBJ_BASE_SIZE(obj) + cache->cos;

    obj->ptr = (void *)((char *)obj + sizeof(arc_object_t));

    return obj;
}

// the returned object is retained, the caller must call arc_release_resource(obj) to release it
static inline arc_resource_t 
arc_lookup_internal(arc_t *cache, const void *key, size_t len, void **valuep, int async, time_t ttl, int fetch)
{
    // NOTE: this is an atomic operation ensured by the hashtable implementation,
    //       we don't do any real copy in our callback but we just increase the refcount
    //       of the object (if found)
    arc_object_t *obj = ht_get_deep_copy(cache->hash, (void *)key, len, NULL, retain_obj_cb, cache);
    if (obj) {
        if (!ATOMIC_READ(cache->mode) || UNLIKELY(ATOMIC_READ(obj->state) != &cache->mfu)) {
            if (UNLIKELY(arc_move(cache, obj, &cache->mfu) == -1)) {
                fprintf(stderr, "Can't move the object into the cache\n");
                return NULL;
            }
            arc_balance(cache);
        }

        if (valuep)
            *valuep = obj->ptr;

        return obj;
    }

    if (!fetch)
        return NULL;

    obj = arc_object_create(cache, key, len);
    if (UNLIKELY(!obj))
        return NULL;

    // let our cache user initialize the underlying object
    cache->ops->init(key, len, async, ttl, (arc_resource_t)obj, obj->ptr, cache->ops->priv);
    obj->async = async;

    retain_ref(cache->refcnt, obj->node);
    // NOTE: atomicity here is ensured by the hashtable implementation
    int rc = ht_set_if_not_exists(cache->hash, (void *)key, len, obj, sizeof(arc_object_t));
    switch(rc) {
        case -1:
            fprintf(stderr, "Can't set the new value in the internal hashtable\n");
            release_ref(cache->refcnt, obj->node);
            break;
        case 1:
            // the object has been created in the meanwhile
            release_ref(cache->refcnt, obj->node);
            // XXX - yes, we have to release it twice
            release_ref(cache->refcnt, obj->node);
            return arc_lookup(cache, key, len, valuep, async, ttl);
        case 0:
            /* New objects are always moved to the MRU list. */
            rc  = arc_move(cache, obj, &cache->mru);
            if (rc >= 0) {
                arc_balance(cache);
                if (valuep)
                    *valuep = obj->ptr;
                return obj;
            }
            break;
        default:
            fprintf(stderr, "Unknown return code from ht_set_if_not_exists() : %d\n", rc);
            release_ref(cache->refcnt, obj->node);
            break;
    } 
    release_ref(cache->refcnt, obj->node);
    return NULL;
}

// the returned object is retained, the caller must call arc_release_resource(obj) to release it
arc_resource_t 
arc_lookup_nofetch(arc_t *cache, const void *key, size_t len, void **valuep)
{
    return arc_lookup_internal(cache, key, len, valuep, 0, 0, 0);
}


// the returned object is retained, the caller must call arc_release_resource(obj) to release it
arc_resource_t 
arc_lookup(arc_t *cache, const void *key, size_t len, void **valuep, int async, time_t ttl)
{
    return arc_lookup_internal(cache, key, len, valuep, async, ttl, 1);
}

int arc_lookup_multi(arc_t *cache,
                     void **keys,
                     size_t *klens,
                     arc_resource_t *resources,
                     int num_keys,
                     time_t ttl)
{
    int i;

    if (cache->ops->fetch_multi) {
        int *missing = NULL;
        int missing_count = 0;
        for (i = 0; i < num_keys; i++) {
            resources[i] = arc_lookup_nofetch(cache, keys[i], klens[i], NULL);
            if (!resources[i]) {
                missing = realloc(missing, sizeof(int) * (missing_count + 1));
                missing[missing_count++] = i;
            }
        }

        if (missing_count) {
            void *mem = malloc((sizeof(void *) + sizeof(size_t) + sizeof(int)) * missing_count);
            void **missing_objects = &mem;
            size_t *sizes = (size_t *)(mem + missing_count);
            int *statuses = (int *)(sizes + missing_count); 
            int to_fetch = 0;
            for (i = 0; i < missing_count; i++) {
                void *key = keys[missing[i]];
                size_t klen = klens[missing[i]];
                arc_object_t *obj = arc_object_create(cache, key, klen);
                if (UNLIKELY(!obj)) {
                    //TODO - handle error and release resources
                    free(mem);
                    free(missing);
                    return -1;
                }

                // let our cache user initialize the underlying object
                cache->ops->init(key, klen, 1, ttl, (arc_resource_t)obj, obj->ptr, cache->ops->priv);
                obj->async = 1;
                obj->locked = 1;

                retain_ref(cache->refcnt, obj->node);
                // NOTE: atomicity here is ensured by the hashtable implementation
                int rc = ht_set_if_not_exists(cache->hash, (void *)key, klen, obj, sizeof(arc_object_t));
                switch(rc) {
                    case -1:
                        fprintf(stderr, "Can't set the new value in the internal hashtable\n");
                        release_ref(cache->refcnt, obj->node);
                        break;
                    case 1:
                        // the object has been created in the meanwhile
                        release_ref(cache->refcnt, obj->node);
                        // XXX - yes, we have to release it twice
                        release_ref(cache->refcnt, obj->node);
                        resources[missing[i]] = arc_lookup(cache, key, klen, NULL, 1, ttl);
                        break;
                    case 0:
                        missing_objects[to_fetch++] = obj;
                        resources[missing[i]] = obj;
                        break;
                    default:
                        fprintf(stderr, "Unknown return code from ht_set_if_not_exists() : %d\n", rc);
                        release_ref(cache->refcnt, obj->node);
                        break;
                }
            }
            if (to_fetch &&
                cache->ops->fetch_multi(missing_objects, sizes, statuses, to_fetch, cache->ops->priv) != 0)
            {
                // TODO - Handle error
                free(mem);
                free(missing);
                return -1;
            }
            
            for (i = 0; i < to_fetch; i++) {
                switch (statuses[i]) {
                    case 0:
                        break;
                    case 1:
                        break;
                    case -1:
                        break;
                    default:
                        break;
                }
            }

            free(mem);
            free(missing);
        }
    } else {
        for (i = 0; i < num_keys; i++) {
            resources[i] = arc_lookup(cache, keys[i], klens[i], NULL, 1, ttl);
        }
    }
    return 0;
}

typedef struct {
    arc_t *cache;
    time_t ttl;
    void *data;
    size_t dlen;
 } upload_obj_cb_arg_t;

static void *
update_obj_cb(void *data, size_t dlen, void *user)
{
    upload_obj_cb_arg_t *arg = (upload_obj_cb_arg_t *)user;
    arc_object_t *obj = (arc_object_t *)data;
    arc_t *cache = (arc_t *)arg->cache;
    cache->ops->store(obj->ptr, arg->data, arg->dlen, cache->ops->priv);
    if (arg->ttl) {
    }
    //retain_ref(cache->refcnt, obj->node);
    return arg->data;
}

int
arc_load(arc_t *cache, const void *key, size_t klen, void *valuep, size_t vlen, time_t ttl)
{
    upload_obj_cb_arg_t arg = {
        .cache = cache,
        .ttl = ttl,
        .data = valuep,
        .dlen = vlen
    };
    arc_object_t *obj = ht_get_deep_copy(cache->hash, (void *)key, klen, NULL, update_obj_cb, &arg);
    if (obj) {
        //release_ref(cache->refcnt, obj->node);
        return 1;
    }

    obj = arc_object_create(cache, key, klen);
    if (!obj)
        return -1;

    // let our cache user initialize the underlying object
    cache->ops->init(key, klen, 0, ttl, (arc_resource_t)obj, obj->ptr, cache->ops->priv);
    cache->ops->store(obj->ptr, valuep, vlen, cache->ops->priv);

    retain_ref(cache->refcnt, obj->node);
    // NOTE: atomicity here is ensured by the hashtable implementation
    int rc = ht_set_if_not_exists(cache->hash, (void *)key, klen, obj, sizeof(arc_object_t));
    switch(rc) {
        case -1:
            fprintf(stderr, "Can't set the new value in the internal hashtable\n");
            release_ref(cache->refcnt, obj->node);
            break;
        case 1:
            // the object has been created in the meanwhile
            release_ref(cache->refcnt, obj->node);
            // XXX - yes, we have to release it twice
            release_ref(cache->refcnt, obj->node);
            return arc_load(cache, key, klen, valuep, vlen, ttl);
        case 0:
            break;
        default:
            fprintf(stderr, "Unknown return code from ht_set_if_not_exists() : %d\n", rc);
            release_ref(cache->refcnt, obj->node);
            rc = -1;
    }

    release_ref(cache->refcnt, obj->node);
    return rc;
}

size_t
arc_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mru.size) + ATOMIC_READ(cache->mfu.size);
}

size_t
arc_mru_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mru.size);
}

size_t
arc_mfu_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mfu.size);
}

size_t
arc_mrug_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mrug.size);
}

size_t
arc_mfug_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mfug.size);
}

void
arc_get_size(arc_t *cache, size_t *mru_size, size_t *mfu_size, size_t *mrug_size, size_t *mfug_size)
{
    *mru_size = ATOMIC_READ(cache->mru.size);
    *mfu_size = ATOMIC_READ(cache->mfu.size);
    *mrug_size = ATOMIC_READ(cache->mrug.size);
    *mfug_size = ATOMIC_READ(cache->mfug.size);
}

uint64_t
arc_count(arc_t *cache)
{
    return ATOMIC_READ(cache->mru.count) + ATOMIC_READ(cache->mfu.count) +
           ATOMIC_READ(cache->mrug.count) + ATOMIC_READ(cache->mfug.count);
}

void *
arc_get_resource_ptr(arc_resource_t res)
{
    return ((arc_object_t *)res)->ptr;
}

void
arc_set_mode(arc_t *cache, arc_mode_t mode)
{
    ATOMIC_SET(cache->mode, mode);
}

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
