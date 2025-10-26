/* The cached object.
 * The data will be loaded when ARC instruct us to do so.
 */

#include <stdint.h>

#ifdef USE_PACKED_STRUCTURES
#pragma pack(push, 1)
#endif
typedef struct {
    void *key;   // The key (weak reference to the actual key stored in the arc resource)
    size_t klen; // The length of the key
    char kbuf[32];

    // internal storage for data which doesn't exceeds 256 bytes.
    // If the complete data is bigger than 256 bytes, the required
    // memory will be allocated and the data pointer will be set
    // to point to the newly allocated memory.
    char dbuf[32];

    void *data;  // The data (if any, NULL otherwise)
                 // Note that if the data is less than 256 bytes this pointer
                 // will point back to the internal buffer (buf pointer)

    size_t dlen; // The length of the data (if any, 0 otherwise)

    struct timeval ts; // the timestamp of when the object has been loaded
                       // into the cache
    
    time_t ttl;

    linked_list_t *listeners; // list of listeners which will be notified
                              // while the object data is being retreived

    uint16_t flags;
    #define COBJ_FLAG_ASYNC    (1)
    #define COBJ_FLAG_COMPLETE (1<<1)
    #define COBJ_FLAG_EVICTED  (1<<2)
    #define COBJ_FLAG_EVICT    (1<<3)
    #define COBJ_FLAG_DROP     (1<<4)
    #define COBJ_FLAG_FETCHING (1<<5)

    pthread_mutex_t lock; // All operations on this structure should be
                          // synchronized using this lock

    arc_resource_t res;
} cached_object_t;
#ifdef USE_PACKED_STRUCTURES
#pragma pack(pop)
#endif

#define COBJ_CHECK_FLAGS(_o, _f) ((((_o)->flags) & (_f)) == (_f))
#define COBJ_SET_FLAG(_o, _f) ((_o)->flags |= (_f))
#define COBJ_UNSET_FLAG(_o, _f) ((_o)->flags &= ~(_f))

typedef struct {
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_listener_t;

void arc_ops_init(const void *key, size_t len, int async, time_t ttl, arc_resource_t res, void *ptr, void *priv);
int arc_ops_fetch(void *item, size_t *size, void * priv);
void arc_ops_evict(void *item, void *priv);
void arc_ops_store(void *item, void *data, size_t size, void *priv);

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
