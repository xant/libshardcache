#ifndef SHARDCACHE_STORAGE_H
#define SHARDCACHE_STORAGE_H

#include <shardcache.h>

/*
 *******************************************************************************
 * Storage API 
 *******************************************************************************
 */

/**
 * @brief Callback to provide the value for a given key.
 *
 *        The shardcache instance will call this callback
 *        if the value has not been found in the cache.
 *
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param value A pointer to where to store the pointer to the actual value.
 *              The caller has the responsibility to release the memory containing
 *              the value.
 * @param vlen  If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 on success; -1 otherwise
 *
 * @note The returned value pointer MUST be a volatile copy and the caller
 *       WILL release its resources
 */
typedef int (*shardcache_fetch_item_callback_t)
    (void *key, size_t klen, void **value, size_t *vlen, void *priv);


/**
 * @brief Callbback to provide access to a local-only key.
 *        Local keys live only on the local filesystem of node and won't be distributed or cached.
 *        The purpose is to allow storing big/huge data which wouldn't fit in memory.
 *        The data can still be accessed asynchronously in chunks, without fully loading
 *        it in memory
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param fd    A pointer to where to store the filedescriptor that can be used to
 *              read the data.
 *              The caller has the responsibility to close the filedescriptor when done with it.
 * @param vlen  If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 on success; -1 otherwise
 */
typedef int (*shardcache_fetch_local_item_callback_t)
    (void *key, size_t klen, int *fd, size_t *vlen, void *priv);

/**
 * @brief Callback to provide values for multiple keys at once.
 *
 *        The shardcache instance will call this callback
 *        if the value has not been found in the cache for the given keys.
 *
 * @param keys  A valid pointer to an array of keys
 * @param klens An array containing the length of the keys
 * @param nkeys The number of keys in the key array (which matches the length
 *              of the klen array)
 * @param values An array where to store the pointers to the actual values
 *               (NULL if the value for one of the key was not found)
 * @note The items in the values array refer to the key at the same index
 *       in the provided input array.
 *       If the value for a given key was not found it will contain a NULL pointer.
 *       The caller is responsible of releasing the memory of all the values returned
 *       in the array.
 *
 * @param vlens If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 on success; the number of successfully fetched items otherwise
 *
 * @note The returned value pointers MUST be a volatile copy and the caller
 *       MUST release their resources
 * @note In case of failure the function will return how many items it has been able to
 *       fetch before failing.\nThe caller MUST release the memory for the successfully
 *       retrieved items (if any) in such a case.
 *
 */
typedef int (*shardcache_fetch_items_callback_t)
    (void **keys, size_t *klens, int nkeys, void **values, size_t *vlens, void *priv);

/**
 * @brief Callback to store a new value for a given key.
 *
 *        The shardcache instance will call this callback
 *        if a new value needs to be set in the underlying storage
 *
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param value A valid pointer to the value to store
 * @param vlen  The length of the value
 * @param if_not_exists A boolean flag which determines if the value should be stored
 *                      only if there is none already
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 if success;\n
 *         1 if a value was already present in the storage while the 'if_not_exists
 *           flag is on\n
 *         -1 otherwise
 */
typedef int (*shardcache_store_item_callback_t)
    (void *key, size_t klen, void *value, size_t vlen, int if_not_exists, void *priv);

/**
 * @brief Callback to store a new value for a given local-only key.
 *        Local keys live only on the local filesystem of node and won't be distributed or cached.
 *        The purpose is to allow storing big/huge data which wouldn't fit in memory.
 *        The data can still be accessed asynchronously in chunks, without fully loading
 *        it in memory
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param local_path  A valid path where to find the data
 * @param vlen  If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 * @param if_not_exists A boolean flag which determines if the value should be stored
 *                      only if there is none already
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 if success;\n
 *         1 if a value was already present in the storage while the 'if_not_exists
 *           flag is on\n
 *         -1 otherwise
 */
typedef int (*shardcache_store_local_item_callback_t)
    (void *key, size_t klen, char *local_path, size_t vlen, int if_not_exists, void *priv);

/**
 * @brief Callback to atomically compare and swap a value for a given key
 *
 * @param key       A valid pointer to the key
 * @param klen      The length of the key
 * @param old_value A valid pointer to the expected old value
 * @param old_len   The length of the expected old value
 * @param new_value A valid pointer to the new value to store if the old value matched
 * @param new_len   The length of the new value
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 if success, -1 in case of error, 1 if the old_value didn't match so no update happened
 */
typedef int (*shardcache_cas_item_callback_t)
    (void *key, size_t klen, void *old_value, size_t old_len, void *new_value, size_t new_len, void *priv);

/**
 * @brief Callback to atomically increment the value for a given key
 * @note  The value is assumed to be numerical, safe conversion must be handled if necessary
 *
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param amount The numerical amount to increment
 * @param out    A pointer to where to store the new amount after the increment
 * @param priv   The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 on success; -1 otherwise
 */
typedef int (*shardcache_increment_item_callback_t)
    (void *key, size_t klen, int64_t amount, int64_t initial_value, int64_t *out, void *priv);

/**
 * @brief Callback to atomically increment the value for a given key
 * @note  The value is assumed to be numerical, safe conversion must be handled if necessary
 *
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param amount The numerical amount to increment
 * @param out    A pointer to where to store the new amount after the decrement
 * @param priv   The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return 0 on success; -1 otherwise
 */
typedef int (*shardcache_decrement_item_callback_t)
    (void *key, size_t klen, int64_t amount, int64_t initial_value, int64_t *out, void *priv);

/**
 * @brief Callback to remove an existing value for a given key.
 *
 *        The shardcache instance will call this callback
 *        if the value for a given key needs to be removed
 *        from the underlying storage
 *
 * @param key  A valid pointer to the key
 * @param klen The length of the key
 * @param priv The 'priv' pointer previously stored in the shardcache_storage_t
 *             structure at initialization time
 * @return 0 if success, -1 otherwise
 */
typedef int
(*shardcache_remove_item_callback_t)(void *key, size_t klen, void *priv);

/**
 * @brief Callback to check if a specific key exists on the storage
 * @param key  A valid pointer to the key
 * @param klen The length of the key
 * @param priv The 'priv' pointer previously stored in the shardcache_storage_t
 *             structure at initialization time
 * @return 1 if exists, 0 otherwise
 */
typedef int
(*shardcache_exist_item_callback_t)(void *key, size_t klen, void *priv);

/**
 * @struct shardcache_storage_index_item_t
 *
 * @brief structure representing an item in the storage index
 *
 */
typedef struct _shardcache_storage_index_item_s {
    void *key;   //!<A valid pointer to the key
    size_t klen; //!<The length of the key
    size_t vlen; //!<The length of the value
} shardcache_storage_index_item_t;

/**
 * @struct shardcache_storage_index_t
 *
 * @brief structure representing the storage index, holding an array
 *        of shardcache_storage_index_item_t structures. 
 * @note  Can be obtained calling shardcache_get_index()
 *        and MUST be disposed using shardcache_free_index()
 */
typedef struct _shardcache_storage_index_s {
    //! Array of shardcache_storage_index_item_t structures
    shardcache_storage_index_item_t *items;
    //! The number of elements in the 'items' array
    size_t size;
} shardcache_storage_index_t;

/**
 * @brief Callback returning the number of items in the index
 *
 * @param priv The priv pointer owned by the storage
 *
 * @return The total number of items in the storage
 * @note This callback is mandatory if the index callback is set, otherwise it's optional
 * @see shardcache_get_index_callback_t
 * @see shardcache_get_index()
 */
typedef size_t (*shardcache_count_items_callback_t)(void *priv);

/**
 * @brief Callback to fetch the index of stored keys.
 *
 *        The shardcache instance will call this callback
 *        when it will need to retrieve the full index of the keys
 *        owned (and stored) by the instance
 *
 * @param index An array of shardcache_storage_index_item_t structures
 *              to hold the index
 * @param isize The number of slots in the provided index array
 * @param priv  The priv pointer owned by the storage
 *
 * @return The number of items in the index, 0 if none or errors
 * @note If the caller didn't previously check the number of items
 *       via the count() callback, it MUST check if the returned value
 *       matches the passed isize and in case try again with a bigger
 *       index size to ensure there are no more items.
 *       Using the count() method ensure to get all the items
 *       which are expected to exist
 *
 * @note The count might change between the time the count callback returns
 *       and the this callback is called.\n
 *       If this happens the cases are 2 :\n
 *          1 - More items have been added in the meanwhile.
 *              Which means that the caller will get exactly the expected number
 *              of items but there will be more new items in the storage which are not returned.\n
 *          2 - Some items have been removed in the meanwhile.
 *              The caller will get less items than expecting but this is not a problem because
 *              the alloc'd memory is enough and the caller will also know how many items 
 *              have been actually returned by the index callback.
 *
 */
typedef size_t (*shardcache_get_index_callback_t)
    (shardcache_storage_index_item_t *index, size_t isize, void *priv);

/**
 * @brief Callback used to notify the underlying storage about the creation of a new worker thread
 *
 * @param priv The priv pointer owned by the storage
 *
 * @return The total number of items in the storage
 * @note This callback is mandatory if the index callback is set, otherwise it's optional
 * @see shardcache_get_index_callback_t
 * @see shardcache_get_index()
 */
typedef void (*shardcache_thread_start_callback_t)(void *priv);

/**
 * @brief Callback returning the number of items in the index
 *
 * @brief Callback used to notify the underlying storage about the destruction of a new worker thread
 *
 * @return The total number of items in the storage
 * @note This callback is mandatory if the index callback is set, otherwise it's optional
 * @see shardcache_get_index_callback_t
 * @see shardcache_get_index()
 */
typedef void (*shardcache_thread_exit_callback_t)(void *priv);


#define SHARDCACHE_STORAGE_API_VERSION 0x03

typedef struct _shardcache_storage_s shardcache_storage_t;
typedef int (*shardcache_storage_init_t)(shardcache_storage_t *, char **);
typedef void (*shardcache_storage_destroy_t)(void *);
typedef int (*shardcache_storage_reset_t)(char **, void *);

/**
 * @struct shardcache_storage_t
 *
 * @brief      Structure holding all the callback pointers required
 *             by shardcache to interact with underlying storage
 */
struct _shardcache_storage_s {

    uint32_t version; // The version of the storage structure
 
    //! The fecth callback
    shardcache_fetch_item_callback_t       fetch;

    shardcache_fetch_local_item_callback_t fetch_local;

    //! The fetch multiple items callback
    shardcache_fetch_items_callback_t      fetch_multi;

    //! The store callback (optional if the storage is indended to be read-only)
    shardcache_store_item_callback_t       store;

    //! Store data that wouldn't fit in memory
    shardcache_store_local_item_callback_t store_local;

    //! The CAS callback (optional if the storage intends to expose the CAS functionality)
    shardcache_cas_item_callback_t         cas;

    //! The increment callback (optional)
    shardcache_increment_item_callback_t   increment;

    //! The increment callback (optional)
    shardcache_decrement_item_callback_t   decrement;

    //! The remove callback (optional if the storage is intended to be read-only)
    shardcache_remove_item_callback_t      remove;
    /**
     * @brief Optional callback which can be used to 'quickly' check if a key exists in the storage
     * @note The speed of this callback strictly depends on the storage implementation
     */
    shardcache_exist_item_callback_t       exist;
    /**
     * @brief Optional callback which returns the index of keys accessible through the storage
     * @note check shardcache_get_index() documentation for more details
     */
    shardcache_get_index_callback_t        index;
    /**
     * @brief If the index callback is set, this callback will be used to determine how many
     *        items are going to be returned in the index.
     * @note This callback is mandatory if the index callback is set, otherwise it's optional
     * @note check shardcache_get_index() documentation for more details
     */
    shardcache_count_items_callback_t      count;

    
    /**
     * @brief Optional callback which, if set, will be called everytime a new worker
     *        thread (which could access the storage) has been created
     */
    shardcache_thread_start_callback_t     thread_start;

    /**
     * @brief Optional callback which, if set, will be called everytime a new worker
     *        thread (which could have accessed the storage) has been destroyed
     */
    shardcache_thread_exit_callback_t      thread_exit;

    /**
     * @brief If set to a non zero value, shardcache will assume that all the peers
     *        can access the same keys on the storage (for example a shared database,
     *        or a shared filesystem))
     */
    int                                    global;

    /**
     * @brief If set to a non zero value, shardcache will assume that all the replicas
     *        of a given peer can access the same storage and set commands won't be propagated
     *        to all the replicas by a receiving node
     */
    int                                    shared;

    /**
     * @brief Pointer to private data which will passed to all the callbacks
     * @note The implementation can use this pointer to keep its internal data/status/handlers/whatever
     */
    void                                   *priv;

    // the members of this structure are used internally by libshardcache to store the symbols
    // extrated from the loadable storage plugins.
    // The storage itself should never try accessing/modifying them
    struct {
        void *handle; // If not null, it points to the handle returned by dlopen
        shardcache_storage_init_t init;
        shardcache_storage_destroy_t destroy;
        shardcache_storage_reset_t reset;
    } internal;

};

/**
 * @brief Callback used to create a new instance of a storage module
 *
 *        All storage modules need to expose this callback which will be 
 *        called by the shardcache instance at initialization time 
 *        to let the storage initialize and create the shardcache_storage_t
 *        structure and its internals
 *
 * @param options A null-termianted array of strings holding the options
 *                specific to the storage module
 *
 * @return A newly initialized shardcache_storage_t structure usable by the
 *         shardcache instance
 */
typedef shardcache_storage_t *
        (*shardcache_storage_constructor)(const char **options);

/**
 * @brief Callback used to dispose all resources associated to a previously
 *        initialized storage module
 *
 * @param storage A pointer to a valid (and previously initialized)
 *                shardcache_storage_t structure
 */
typedef void (*shardcache_storage_destructor)(shardcache_storage_t *storage);

/*
 *******************************************************************************
 * Counters API 
 *******************************************************************************
 */

/**
 * @brief Structure representing an exposed counter.
 *
 *        Any of the internal shardcache modules can export counters
 *        (as uint32_t integers) for stats purposes.
 *        Any module can add new counters which will be included in the array
 *        returned by shardcache_get_counters().
 *        The value member of the structure will be always accessed via
 *        the atomic builtins and the same is expected from the module 
 *        exporting it.
 *
 * @note  A list of exported counters can be obtained using shardcache_get_counters()
 */
typedef struct {
    char name[256];
    uint64_t value;
} shardcache_counter_t;

/**
 * @brief Returns the list of registered counters with their actual value
 * @param cache    A valid pointer to a shardcache_t structure
 * @param counters A reference to a pointer which will be set to the initialized
 *                 memory holding the array of counters
 * @note           The counters array needs to be released using
 *                 free() once not necessary anymore.
 * @return The number of counters contained in the counters array
 */
int shardcache_get_counters(shardcache_t *cache,
                            shardcache_counter_t **counters);

/**
 * @brief Resets all the counters to 0
 * @param cache A valid pointer to a shardcache_t structure
 */
void shardcache_clear_counters(shardcache_t *cache);

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
