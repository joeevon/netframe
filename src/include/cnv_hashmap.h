#ifndef __CNV_HASHMAP_H_2013_10_19__
#define __CNV_HASHMAP_H_2013_10_19__

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#include "cnv_core_typedef.h"
#include <pthread.h>

#define MUTEX_HASHMAP_TOKEN "mutex_hashmap"

#define ERR_HASHMAP_BASE                     (22000)

    // 本模块的错误定义
#define ERR_HASHMAP_PARAM                    (ERR_HASHMAP_BASE + 1) //非法参数

    typedef int (*pfnCNV_HASHMAP_HASH_CALLBACK)(void *in_pKey);

    typedef K_BOOL(*pfnCNV_HASHMAP_EQUALS_CALLBACK)(void *in_pKeyA, void *in_pKeyB);

    typedef K_BOOL(*pfnCNV_HASHMAP_ITERATOR_CALLBACK)(void *in_pKey, void *in_pValue, void *in_pContext);

    typedef void *(*pfnCNV_HASHMAP_MEMOIZE_CALLBACK)(void *in_pKey, void *in_pContext);

    typedef K_BOOL(*pfnCNV_HASHMAP_ERASE_CALLBACK)(void *in_pKey, void *in_pValue, void *in_pContext, K_BOOL *out_bErase);

    typedef struct tagNAVI_PLAT_CRITICAL_NIX_SECTION
    {
        pthread_mutex_t  m;
        pthread_t        t;
        char             szName[32];
        int             lLockCount;
    } NAVI_PLAT_CRITICAL_NIX_SECTION;

    typedef struct __tagNAVI_SVC_HASHMAP_ENTRY
    {
        void *key;
        int hash;
        void *value;
        struct __tagNAVI_SVC_HASHMAP_ENTRY *next;
    } NAVI_SVC_HASHMAP_ENTRY, *PNAVI_SVC_HASHMAP_ENTRY;

    typedef struct __tagNAVI_SVC_HASHMAP
    {
        NAVI_SVC_HASHMAP_ENTRY **buckets;
        int bucketCount;
        pfnCNV_HASHMAP_HASH_CALLBACK hash;
        pfnCNV_HASHMAP_EQUALS_CALLBACK equals;
        void *lock;
        int size;
    } NAVI_SVC_HASHMAP, *PNAVI_SVC_HASHMAP;

    extern NAVI_SVC_HASHMAP *hashmap(void *in_pHashmap);

    /**
     * Creates a new hash map. Returns NULL if memory allocation fails.
     *
     * @param initialCapacity number of expected entries
     * @param hash function which hashes keys
     * @param equals function which compares keys for equality
     */
    int cnv_hashmap_init(void * *out_pHashmap,
                         int in_lCapacity,
                         pfnCNV_HASHMAP_HASH_CALLBACK in_pHashFunc,
                         pfnCNV_HASHMAP_EQUALS_CALLBACK in_pEqualsFunc);

    /**
     * Frees the hash map. Does not free the keys or values themselves.
     */
    int cnv_hashmap_uninit(void *in_pHashmap);

    /**
     * Hashes the memory pointed to by key with the given size. Useful for
     * implementing hash functions.
     */
    int cnv_hashmap_hash(void *in_pkey, int in_lKeySize);

    /**
     * Puts value for the given key in the map. Returns pre-existing value if
     * any.
     *
     * If memory allocation fails, this function returns NULL, the map's size
     * does not increase, and errno is set to ENOMEM.
     */
    int cnv_hashmap_put(void *in_pHashmap, void *in_pkey, void *in_pValue, void **out_pOldValue);

    /**
     * Gets a value from the map. Returns NULL if no entry for the given key is
     * found or if the value itself is NULL.
     */
    int cnv_hashmap_get(void *in_pHashmap, void *in_pkey, void **out_pValue);

    /**
     * Returns true if the map contains an entry for the given key.
     */
    K_BOOL cnv_hashmap_containsKey(void *in_pHashmap, void *in_pkey);

    /**
     * Gets the value for a key. If a value is not found, this function gets a
     * value and creates an entry using the given callback.
     *
     * If memory allocation fails, the callback is not called, this function
     * returns NULL, and errno is set to ENOMEM.
     */

    int cnv_hashmap_memoize(void *in_pHashmap, void *in_pKey,
                            pfnCNV_HASHMAP_MEMOIZE_CALLBACK in_pMemoizeFunc,
                            void *in_pContext,
                            void **out_pValue);

    /**
     * Removes an entry from the map. Returns the removed value or NULL if no
     * entry was present.
     */
    int cnv_hashmap_remove(void *in_pHashmap, void *in_pKey, void **out_pValue);

    /**
     * Gets the number of entries in this map.
     */
    int cnv_hashmap_size(void *in_pHashmap);

    /**
     * Invokes the given callback on each entry in the map. Stops iterating if
     * the callback returns false.
     */
    int cnv_hashmap_iterator(void *in_pHashmap,
                             pfnCNV_HASHMAP_ITERATOR_CALLBACK in_pIteratorFunc,
                             void *in_pContext);

    /**
     * Invokes the given callback on each entry in the map to erase the matched entry. Stops iterating if
     * the callback returns false.
     */
    int cnv_hashmap_erase(void *in_pHashmap,
                          pfnCNV_HASHMAP_ERASE_CALLBACK in_pEraseFunc,
                          void *in_pContext);

    /**
     * Concurrency support.
     */

    /**
     * Locks the hash map so only the current thread can access it.
     */
    int cnv_hashmap_lock(void *in_pHashmap);

    /**
     * Unlocks the hash map so other threads can access it.
     */
    int cnv_hashmap_unlock(void *in_pHashmap);

    /**
     * Key utilities.
     */

    /**
     * Hashes int keys. 'key' is a pointer to int.
     */
    int cnv_hashmap_inthash(void *in_pKey);

    /**
     * Compares two int keys for equality.
     */
    K_BOOL cnv_hashmap_intequals(void *in_pKeyA, void *in_pKeyB);

    /**
     * For debugging.
     */

    /**
     * Gets current capacity.
     */
    int cnv_hashmap_capacity(void *in_pHashmap);

    /**
     * Counts the number of entry collisions.
     */
    int cnv_hashmap_countcollisions(void *in_pHashmap);

    /**
    * Hashes int keys. 'key' is a pointer to char *.
    */
    int  cnv_hashmap_charhash(void *in_pKey);

    /**
    * Compares two char* keys for equality.
    */
    K_BOOL cnv_hashmap_charequals(void *in_pKeyA, void *in_pKeyB);

#ifdef __cplusplus
};
#endif // __cplusplus
#endif // __CNV_HASHMAP_H_2013_10_19__
