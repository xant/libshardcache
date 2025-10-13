#ifndef SHARDCACHE_H
#define SHARDCACHE_H

/**
 * @file shardcache.h
 * @author Andrea Guzzo
 * @brief shardcache C implementation
 */
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <syslog.h>

/**
 * @brief Opaque structure representing the actual shardcache instance
 */
typedef struct _shardcache_s shardcache_t;

#include <shardcache_log.h>
#include <shardcache_storage.h>
#include <shardcache_node.h>

#define SHARDCACHE_PORT_DEFAULT               4444
#define SHARDCACHE_TCP_TIMEOUT_DEFAULT        5000   // (in millisecs) == 5 secs
#define SHARDCACHE_EXPIRE_TIME_DEFAULT        0      // don't expire keys by default
#define SHARDCACHE_IOMUX_RUN_TIMEOUT_LOW      100000 // (in microsecs)
#define SHARDCACHE_IOMUX_RUN_TIMEOUT_HIGH     500000 // (in microsecs)
#define SHARDCACHE_CONNECTION_EXPIRE_DEFAULT  5000   // (in millisecs)
#define SHARDCACHE_SERVING_LOOK_AHEAD_DEFAULT 64     // number of queued/pipelined
                                                     // requests to handle ahead
#define SHARDCACHE_ASYNC_THREADS_NUM_DEFAULT  1      // number of async i/o threads used
                                                     // for inter-node communication
extern const char *LIBSHARDCACHE_VERSION;
extern const char *LIBSHARDCACHE_BUILD_INFO;

/*
 *******************************************************************************
 * Shardcache API 
 *******************************************************************************
 */

/*
 * @brief Create a shardcache storage from a loadable module
 * @param filename the full-path to the loadable module
 * @param options NULL-terminated array of storage options
 * @return a fully initialized shardcache_storage_t structure which MUST
 *         be released using shardcache_storage_dispose() once not needed anymore
 */
shardcache_storage_t *shardcache_storage_load(char *filename, char **options);

/*
 * @brief Release a shardcache storage previously initialized using shardcache_storage_load()
 * @param st A valid shardcache_storage_t structure obtained using shardcache_storage_load()
 */
void shardcache_storage_dispose(shardcache_storage_t *st);

/*
 * @brief Reset an existing storage
 * @param st A valid shardcache_storage_t structure
 * @return 0 on success, -1 otherwise
 */
int shardcache_storage_reset(shardcache_storage_t *st, char **options);

/**
 * @brief Create a new shardcache instance
 * @param me              A valid <address:port> null-terminated string
 *                        representing the new node to be created
 * @param nodes           A list of <address:port> strings representing the nodes
 *                        taking part to the shardcache 'cloud'
 * @param num_nodes       The number of nodes present in the nodes list
 * @param storage         A shardcache_storage_t structure holding pointers to the
 *                        storage callbacks\n If NULL the internal (memory-only)
 *                        volatile storage will be used for all the keys (and not
 *                        only the ones being set with an expiration time)
 * @param num_workers     The number of worker threads taking care of serving input connections
 * @param num_async       The number of async i/o threads handling the inter-node communication\n
 *                        If greater than 0 it will indicate the actual number of async-i/o
 *                        threads to use\n
 *                        If smaller than 0 (negative) the number of async threads will be calculated
 *                        in function of the number of num_workers (in the actual implemenation 1 extra
 *                        async thread will be created every 20 workers\n
 *                        If 0 the default value (SHARDCACHE_ASYNC_THREADS_NUM_DEFAULT) will be used
 * @param cache_size      The maximum size of the ARC cache
 * @return a newly initialized shardcache descriptor
 * 
 * @note The returned shardcache_t structure MUST be disposed using shardcache_destroy()
 *
 * @note The newly created instance will copy all the pointers contained in the storage structure 
 *       to its internal descriptor so the resources eventually allocated for the storage structure
 *       passed as argument can be safely released after calling shardcache_create()
 */
shardcache_t *shardcache_create(char *me, 
                        shardcache_node_t **nodes,
                        int num_nodes,
                        shardcache_storage_t *storage,
                        int num_workers,
                        int num_async,
                        size_t cache_size);



typedef enum {
    SHARDCACHE_ARC_MODE_STRICT = 0,
    SHARDCACHE_ARC_MODE_LOOSE = 1
} arc_mode_t;

int shardcache_arc_mode(shardcache_t *cache, arc_mode_t new_value);

int shardcache_cache_on_set(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the evict_on_delete behaviour at runtime
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if evict_on_delete is desired, 0 otherwise.\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status).
 * @return the previous value for the evict_on_delete setting
 * @note if evict-on-delete is true, an evict command is sent to all other nodes
 *       when an item is removed from the storage
 * @note defaults to 1
 */
int shardcache_evict_on_delete(shardcache_t *cache, int new_value);

/*
 * @brief Allows to switch between using persistent connections, or making a new connection for
 *        each message sent to a peer
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if persistent connections should be used, 0 otherwise.
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status).
 * @return the previous value for the use_persistent_connections setting
 * @note defaults to 1
 */
int shardcache_use_persistent_connections(shardcache_t *cache, int new_value);

/*
 * @brief Allows to force caching of remote items
 *        (as opposed to the default behaviour  of caching only hot items)
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if force_caching is desired, 0 otherwise\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status)
 * @return the previous value for the force_caching setting
 * @note defaults to 1
 */
int shardcache_force_caching(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the timeout used when creating tcp connections
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   The amount of seconds to use as timeout\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status)
 * @return the previous value for the tcp_timeout setting
 * @note defaults to SHARDCACHE_TCP_TIMEOUT_DEFAULT
 */
int shardcache_tcp_timeout(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the connection pool connection timeout when reusing tcp connections
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   The amount of milliseconds to use as timeout\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status)
 * @return the previous value for the tcp_timeout setting
 * @note defaults to SHARDCACHE_CONNECTION_EXPIRE_DEFAULT
 */
int shardcache_conn_expire_time(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the timeout passed to iomux_run()
 *               by the serving workers and the async reader
 * @note  Both the workers and the async reader use a low timeout
 *        to allow newly added filedescriptors to be included in the
 *        iomux as soon as possible
 * @param cache A valid pointer to a shardcache_t structure
 * @param new_value The amount of microseconds to use as timeout\n
 *                  If -1 is provided as new_value, no change will be applied
 *                  but the actual value will still be returned
 *                  (effectively querying the actual status)
 * @return the previous value for the iomux_run_timeout_low setting
 * @note defaults to SHARDCACHE_IOMUX_RUN_TIMEOUT_LOW
 */
int shardcache_iomux_run_timeout_low(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the timeout passed to iomux_run()
 *               by the listener and expirer
 * @param cache A valid pointer to a shardcache_t structure
 * @param new_value The amount of microseconds to use as timeout\n
 *                  If -1 is provided as new_value, no change will be applied
 *                  but the actual value will still be returned
 *                  (effectively querying the actual status)
 * @return the previous value for the iomux_run_timeout_high setting
 * @note defaults to SHARDCACHE_IOMUX_RUN_TIMEOUT_HIGH
 */
int shardcache_iomux_run_timeout_high(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the expiration time for cached items
 * @param cache A valid pointer to a shardcache_t structure
 * @param new_value The amount of seconds to use as expire time\n
 *                  If 0 no expiration will be scheduled for cached items\n
 *                  If -1 is provided as new_value, no change will be applied
 *                  but the actual value will still be returned
 *                  (effectively querying the actual status)
 * @return the previous value for the expire_time setting
 * @note defaults to SHARDCACHE_IOMUX_EXPIRE_TIME
 */
int shardcache_expire_time(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the number of queued/pipelined requests to handle ahead
 *        while still serving the response to the first request
 * @param cache A valid pointer to a shardcache_t structure
 * @param new_value The amount of requests to look ahead
 * @return the previous value for the look_ahead setting
 * @note defaults to SHARDCACHE_SERVING_LOOK_AHEAD_DEFAULT
 */
int shardcache_serving_look_ahead(shardcache_t *cache, int new_value);

/*
 * @brief Allows to enable/disable the 'lazy_expiration' mode
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if lazy_expiration is desired, 0 otherwise\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status)
 * @return the previous value for the lazy_expiration setting
 * @note When lazy expiration is enabled, cached items will be expired
 *       only when fetched, otherwise the expirer thread will take care
 *       of expiring the cached items as well as volatile items
 * @note defaults to 0
 */
int shardcache_lazy_expiration(shardcache_t *cache, int new_value);

/**
 * @brief Release all the resources used by the shardcache instance
 * @param cache   the instance to release
 */
void shardcache_destroy(shardcache_t *cache);

/**
 * @brief Clear the content of the underlying cache
 * @param cache   the instance to release
 */
void shardcache_clear(shardcache_t *cache);

/**
 * @brief Set the size of the underlying cache
 *        (overriding the initial size set at construction time)
 * @param cache   the instance to release
 * @param new_size the new size
 */
void shardcache_set_size(shardcache_t *cache, size_t new_size);

int shardcache_set_workers_num(shardcache_t *cache, unsigned int num_workers);

/**
 * @brief Get partial value data value for a key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param head    A pointer to the memory where to store the partial data
 * @param hlen    The size of the memory pointed by 'head'
 * @param timestamp   If provided the timestamp of when the object was loaded into the cache
 *                  will be stored at the specified address
 *
 * @return The size copied in the 'head' pointer
 * @note The returned size might be less than what specified in 'hlen'
 *       if the complete data is smaller than hlen
 */
size_t shardcache_head(shardcache_t *cache,
                       void *key,
                       size_t klen,
                       void *head,
                       size_t hlen,
                       struct timeval *timestamp);

/**
 * @brief Get partial value data value for a key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param out     A pointer to the memory where to store the partial data
 * @param olen    The size of the memory pointed by 'out'
 * @param offset  The offset from the beginning of the data we want to retrieve
 * @param timestamp   If provided the timestamp of when the object was loaded into the cache
 *                  will be stored at the specified address
 *
 * @return The size copied in the 'head' pointer
 * @note The returned size might be less than what specified in 'hlen'
 *       if the complete data is smaller than hlen
 **/
size_t shardcache_get_offset_sync(shardcache_t *cache,
                                  void *key,
                                  size_t klen,
                                  void *out,
                                  size_t *olen,
                                  size_t offset,
                                  struct timeval *timestamp);

/**
 * @brief Optional callback which can be passed to shardcache_get()
 *        to receive the data asynchronously
 * @param key         A valid pointer to the key
 * @param klen        The length of the key
 * @param data        The pointer to the chunk of data
 * @param dlen        The length of the current chunk of data
 * @param total_size  If non zero, it indicates that this is the last chunk and
 *                    tells total size of the data
 * @note this argument is set only when the data is completed and we know its
 *       real total size. The callback will not be called anymore once this
 *       parameter has been provided
 * @param timestamp   If not NULL, it indicates that this is the last chunk
 *                    and points to the timeval holding the timestamp of when
 *                    the data has been loaded into the cache
 * @note this argument is set only when the data is completed and we know it
 *       has been completely loaded into the cache.
 *       The callback will not be called anymore once this parameter has been
 *       provided
 * @param priv        The priv pointer passed to shardcache_get()
 * @return 0 if no errors occurred and more data can be provided safely;\n
 *         -1 if an error occurs and we don't want the callback to be called
 *         again (which could eventually abort the fetch operation)
 */
typedef int (*shardcache_get_async_callback_t)(void *key,
                                               size_t klen,
                                               void *data,
                                               size_t dlen,
                                               size_t total_size,
                                               struct timeval *timestamp,
                                               void *priv);

/**
 * @brief Get the value for a key.\n
 *        If a callback is provided as argument the data will be
 *        provided back asynchronously, otherwise this function
 *        will block until the operation is completed and return
 *        the data in the output arguments (value, vlen and timestamp)
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param cb      The shardcache_get_async_callback_t which will be
 *                called for each received chunk
 * @param priv    A pointer which will be passed to the
 *                shardcache_get_async_callback_t at each call
 *
 * @return 0 on success, -1 otherwise
 *
 * @note This function might return immediately if the value for the
 *       requested key is already being downloaded (but not complete yet)
 *       In such a case The callback function will be called by the 
 *       thread which is actually downloading the data hence the callback
 *       needs to be thread-safe
 *
 * @note If the data requested is partially downloaded, the available data
 *       will be immediately passed to the callback and the rest will be 
 *       passed while it's being downloaded
 */
int shardcache_get(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   shardcache_get_async_callback_t cb,
                   void *priv);

/**
 * @brief Get the value for a key synchronously.\n
 *        This function will block until the entire data (if any) has been
 *        retrieved. Use shardcache_get() if you want the operation to be
 *        executed asynchronously.
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param value   A reference to where to store the pointer to the retrieved value
 * @param vlen    If provided the size of the value will be stored 
 *                at the memory address pointed by the vlen argument
 * @param timestamp If provided the timestamp of the given key will be stored 
 *                  at the memory address pointed by the timestamp argument
 *
 * @return 0 on success, -1 otherwise
 *
 * @see shardcache_get()
 */

int shardcache_get_sync(shardcache_t *cache,
                        void *key,
                        size_t klen,
                        void **value,
                        size_t *vlen,
                        struct timeval *timestamp);


/**
 * @brief Get partial value data value for a key asynchronously
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param offset  The offset from where to start retrieving data
 * @param length  The amount of data to fetch
 * @param cb      The shardcache_get_async_callback_t which will be
 *                called for each received chunk
 * @param priv    A pointer which will be passed to the
 *                shardcache_get_async_callback_t at each call
 *
 * @return 0 on success, -1 otherwise
 *
 * @note This function might return immediately if the value for the
 *       requested key is already being downloaded (but not complete yet)
 *       In such a case The callback function will be called by the 
 *       thread which is actually downloading the data hence the callback
 *       needs to be thread-safe
 *
 * @note If the data requested is partially downloaded, the available data
 *       will be immediately passed to the callback and the rest will be 
 *       passed while it's being downloaded
 */

int
shardcache_get_offset(shardcache_t *cache,
                      void *key,
                      size_t klen,
                      size_t offset,
                      size_t length,
                      shardcache_get_async_callback_t cb,
                      void *priv);

int shardcache_get_multi(shardcache_t *cache,
                         void **keys,
                         size_t *lens,
                         int num_keys,
                         shardcache_get_async_callback_t cb,
                         void *priv);

/**
 * @brief Optional callback for all the asynchronous routines returning an integer result
 *        (basically all apart shardcache_get() shardcache_offset())
 * @param key     A valid pointer to the key of the command this response refers to
 * @param klen    The length of the key
 * @param res     The integer response returned from the issued command
 * @param priv    The 'priv' pointer previously passed to the called function
 * 
 */
typedef void (*shardcache_async_response_callback_t)(void *key, size_t klen, int64_t res, void *priv);

/**
 * @brief Check if a specific key exists on the node responsible for it
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param cb      The shardcache_async_response_callback_t which will be
 *                called once the response is completely retrieved
 * @param priv    A pointer which will be passed to the
 *                shardcache_async_response_callback_t when called
 * @return 1 if exists, 0 if doesn't exist, -1 in case of errors
 */
int shardcache_exists(shardcache_t *cache,
                      void *key,
                      size_t klen,
                      shardcache_async_response_callback_t cb,
                      void *priv);

/**
 * @brief Load a key into the cache if not present already,
 *        otherwise update the loaded-timestamp
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @return 0 if successfully touched the item, -1 if it doesn't exists
 *           or in case of errors
 */
int shardcache_touch(shardcache_t *cache,
                     void *key,
                     size_t klen);

/**
 * @brief Set the value for a key
 * @param cache  A valid pointer to a shardcache_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param value  A valid pointer to the value
 * @param vlen   The length of the value
 * @param expire The number of seconds after which the volatile value expires
 *               If 0 the value will not expire and it will be stored using the
 *               actual storage module (which might evntually be a presistent
 *                storage backend as the filesystem or database ones)
 * @param cexpire If not 0 the value will be immediately loaded into the cache and
 *                set to expire after cexpire number of seconds \n
 *                If 0 the 'cache_on_set' global runtime option will be used
 *                to determine if the value should be immediately loaded into the cache
 *                and the global 'expire_time' runtime option will be used
 *                once the new value will be eventually loaded into the cache
 *
 *                actual storage module (which might evntually be a presistent
 *                storage backend as the filesystem or database ones)
 * @param if_not_exists If this param is true, the value will be set only
 *                      if there isn't one already stored
 * @param cb     The shardcache_async_response_callback_t which will be
 *               called once the result has been retreived
 * @param priv   A pointer which will be passed to the
 *               shardcache_async_response_callback_t when called
 * @return 0 on success,
 *         1 if the value already exists on the storage and the if_not_exists param is true;\n
 *        -1 otherwise
 */
int shardcache_set(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen,
                   time_t expire,
                   time_t cexpire,
                   int    if_not_exists,
                   shardcache_async_response_callback_t cb,
                   void *priv);

/**
 * @brief Set the value for a local key.
 *        Local keys live only on the local filesystem of node and won't be distributed or cached.
 *        The purpose is to allow storing big/huge data which wouldn't fit in memory.
 *        The data can still be accessed asynchronously in chunks, without fully loading
 *        it in memory
 * @param cache  A valid pointer to a shardcache_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param local_path  A valid path where to find the data
 * @param vlen   The length of the value
 * @param if_not_exists If this param is true, the value will be set only
 *                      if there isn't one already stored
 * @return 0 on success,
 *         1 if the value already exists on the storage and the if_not_exists param is true;\n
 *        -1 otherwise
 */int shardcache_set_local(shardcache_t *cache,
                       void *key,
                       size_t klen,
                       char *local_path,
                       size_t vlen,
                       int if_not_exists);

/**
 * @brief Set the value for multiple keys fetching the responses asyncrhonously
 * @param cache  A valid pointer to a shardcache_t structure
 * @param keys   A valid pointer to an array of keys
 * @param klens  An array of key lengths
 * @param value  A valid pointer to and array of values
 * @param vlen   An array of value lengths
 * @param nkeys  The number of elements in all the provided arrays (keys, klens, values and vlens)
 * @param expire The number of seconds after which the volatile value expires
 *               If 0 the value will not expire and it will be stored using the
 *               actual storage module (which might evntually be a presistent
 *                storage backend as the filesystem or database ones)
 * @param cexpire If not 0 the value will be immediately loaded into the cache and
 *                set to expire after cexpire number of seconds \n
 *                If 0 the 'cache_on_set' global runtime option will be used
 *                to determine if the value should be immediately loaded into the cache
 *                and the global 'expire_time' runtime option will be used
 *                once the new value will be eventually loaded into the cache
 *
 *                actual storage module (which might evntually be a presistent
 *                storage backend as the filesystem or database ones)
 * @param if_not_exists If this param is true, the value will be set only
 *                      if there isn't one already stored
 * @param cb     The shardcache_async_response_callback_t which will be
 *               called once the result has been retreived
 * @param priv   A pointer which will be passed to the
 *               shardcache_async_response_callback_t when called
 * @return 0 on success, -1 otherwise
 *
 * @note There is no synchronous version of the set_multi() function
 */
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
                     void *priv);


/**
 * @brief Set the value for a key if it doesn't already exist 
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param value A valid pointer to the value
 * @param vlen  The length of the value
 * @param expire
 * @param cexpire
 * @return 0 on success, 1 if the key already exists, -1 in case of error
 */
int shardcache_add(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen,
                   time_t expire,
                   time_t cexpire,
                   shardcache_async_response_callback_t cb,
                   void *priv);

int shardcache_cas(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *prev_value,
                   size_t prev_len,
                   void *new_value,
                   size_t new_len,
                   time_t expire,
                   time_t cexpire,
                   shardcache_async_response_callback_t cb,
                   void *priv);

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
                     void *priv);

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
                     void *priv);

int shardcache_del_multi(shardcache_t *cache,
                         void **keys,
                         size_t *klens,
                         int nkeys,
                         shardcache_async_response_callback_t cb,
                         void *priv);

/**
 * @brief Remove the value for a key
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param cb    The shardcache_async_response_callback_t which will be
 *              called once the result has been retreived
 * @param priv  A pointer which will be passed to the
 *              shardcache_async_response_callback_t when called
 * @return 0 on success, -1 otherwise
 */
int shardcache_del(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   shardcache_async_response_callback_t cb,
                   void *priv);

/**
 * @brief Remove the value from the cache for a key
 * @note the value will not be removed from the underlying storage
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @return 0 on success, -1 otherwise
 */
int shardcache_evict(shardcache_t *cache, void *key, size_t klen);

/**
 * @brief Get the node owning a specific key
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param owner If provided the pointed pointer will be set to the name
 *              (<address:port>) of the node owning the key
 * @param len   If not NULL, the size of the owner string will be stored
 *              in the memory pointed by 'len'
 * @return 1 if the current node (represented by cache) is the owner
 *           of the key, 0 otherwise
 */
int shardcache_test_ownership(shardcache_t *cache,
                              void *key,
                              size_t klen,
                              char *owner,
                              size_t *len);

/**
 * @brief Get the index of keys managed by the specific shardcache instance
 *        by querying the storage module
 * @return A pointer to a shardcache_storage_index_t structure holding 
 *         the index
 * @note The caller MUST release the returned pointer once done with it
 *       by using the shardcache_free_index() function
 */
shardcache_storage_index_t *shardcache_get_index(shardcache_t *cache);

/**
 * @brief Release all resources used by the index provided as argument
 * @param index A pointer to a valid shardcache_storage_index_t structure
 *              previously obtained via the shardcache_get_index() function
 */
void shardcache_free_index(shardcache_storage_index_t *index);

/**
 * @brief   Start a migration process
 * @param cache     A valid pointer to a shardcache_t structure
 * @param nodes     The list of nodes representing the new group to migrate to
 * @param num_nodes The number of nodes in the list
 * @param forward   A boolean flag indicating if the migration command needs to be
 *                  forwarded to all other peers
 * @return 0 on success, -1 otherwise
 */
int shardcache_migration_begin(shardcache_t *cache,
                               shardcache_node_t **nodes,
                               int num_nodes,
                               int forward);

/**
 * @brief   Abort the current migration process
 * @param cache A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_abort(shardcache_t *cache);

/**
 * @brief   End the current migration process by swapping the two continua
 *          and releasing resources for the old one
 * @param cache A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_end(shardcache_t *cache);


/**
 * @brief   Notify the internal shardcache core about a new woerker thread which might
 *          access the API\n
 *          This function must be called early within each user-created thread (which is
 *          intended to access the shardcache API at some stage during its lifespan)
 *          to initialize thread-specific variables
 * @note    shardcache_thread_end() MUST be esplicitly called before the background thread
 *          exits to avoid memory leakage
 */
void shardcache_thread_init(shardcache_t *cache);

/**
 * @brief   Release the resources allocated by shardcache_thread_init()
 *          This function needs to be called before calling pthread_exit()
 *          to free memory allocated by shardcache_thread_init()
 *
 * @note   shardcache_thread_end() is not invoked automatically by the client library\n
 *         It must be called explicitly to avoid a memory leak
 */
void shardcache_thread_end(shardcache_t *cache);

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
