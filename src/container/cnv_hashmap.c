#include "cnv_hashmap.h"
#include "cnv_base_define.h"
#include "cnv_comm.h"
#include "log/cnv_liblog4cplus.h"
#include "cnv_thread_sys.h"
#include <stdio.h>

/************************************************************************
功能：初始化临界区
输入：in_criticalSection 临界区的指针
输入：in_pszName         临界区的名字
输出：无
返回值：0 成功
        other 失败
************************************************************************/
int cnv_plat_InitCriticalSection(K_HANDLE *out_criticalSection, const char *in_pszName)
{
    NAVI_PLAT_CRITICAL_NIX_SECTION *pCriticalSection = (NAVI_PLAT_CRITICAL_NIX_SECTION *)cnv_comm_Malloc(sizeof(NAVI_PLAT_CRITICAL_NIX_SECTION));
    *out_criticalSection = pCriticalSection;
    if(K_NULL == pCriticalSection) return ERR_HASHMAP_PARAM;

    pCriticalSection->t = 0;
    pCriticalSection->lLockCount = 0;
    pthread_mutex_init(&pCriticalSection->m, K_NULL);
    if(K_NULL != in_pszName)
    {
        strncpy(pCriticalSection->szName, in_pszName, sizeof(pCriticalSection->szName));
        pCriticalSection->szName[sizeof(pCriticalSection->szName) - 1] = 0;
    }
    return K_SUCCEED;
}

/************************************************************************
功能：进入临界区
输入：in_criticalSection 临界区的指针
输出：无
返回值：0 成功
        other 失败
************************************************************************/
int cnv_plat_EnterCriticalSection(K_HANDLE in_criticalSection)
{
    NAVI_PLAT_CRITICAL_NIX_SECTION *pCriticalSection = (NAVI_PLAT_CRITICAL_NIX_SECTION *)in_criticalSection;
    if(K_NULL == pCriticalSection) return ERR_HASHMAP_PARAM;
    if(pthread_self() != pCriticalSection->t)
    {
        pthread_mutex_lock(&pCriticalSection->m);
        pCriticalSection->t = pthread_self();
    }
    ++pCriticalSection->lLockCount;
    return K_SUCCEED;
}

/************************************************************************
功能：退出临界区
输入：in_criticalSection 临界区的指针
输出：无
返回值：0 成功
        other 失败
************************************************************************/
int cnv_plat_LeaveCriticalSection(K_HANDLE in_criticalSection)
{
    NAVI_PLAT_CRITICAL_NIX_SECTION *pCriticalSection = (NAVI_PLAT_CRITICAL_NIX_SECTION *)in_criticalSection;
    if(K_NULL == pCriticalSection) return ERR_HASHMAP_PARAM;
    if(pthread_self() == pCriticalSection->t)
    {
        --pCriticalSection->lLockCount;
        if(0 == pCriticalSection->lLockCount)
        {
            pCriticalSection->t = 0;
            pthread_mutex_unlock(&pCriticalSection->m);
        }
    }
    return K_SUCCEED;
}

/************************************************************************
功能：删除临界区
输入：in_criticalSection 临界区的指针
输出：无
返回值：0 成功
        other 失败
************************************************************************/
int cnv_plat_DeleteCriticalSection(K_HANDLE in_criticalSection)
{
    NAVI_PLAT_CRITICAL_NIX_SECTION *pCriticalSection = (NAVI_PLAT_CRITICAL_NIX_SECTION *)in_criticalSection;
    if(K_NULL == pCriticalSection) return ERR_HASHMAP_PARAM;
    pthread_mutex_destroy(&pCriticalSection->m);
    cnv_comm_Free(pCriticalSection);
    return K_SUCCEED;
}

int cnv_hashmap_init(void * *out_pHashmap,
                     int in_lCapacity,
                     pfnCNV_HASHMAP_HASH_CALLBACK in_pHashFunc,
                     pfnCNV_HASHMAP_EQUALS_CALLBACK in_pEqualsFunc)
{

    int lsize;
    int result = K_SUCCEED;
    int minimumBucketCount;
    NAVI_SVC_HASHMAP *map = K_NULL;

    *out_pHashmap = K_NULL;
    if((in_pHashFunc == K_NULL)
            || (in_pEqualsFunc == K_NULL))
    {
        return CNV_ERR_PARAM;
    }

    map = cnv_comm_Malloc(sizeof(NAVI_SVC_HASHMAP));
    //LOG_SYS_DEBUG(">>>>>>>>>malloc: %p.", map);
    if(map == K_NULL)
    {
        return CNV_ERR_MALLOC;
    }

    // 0.75 load factor.
    minimumBucketCount = in_lCapacity * 4 / 3;
    map->bucketCount = 1;
    while(map->bucketCount <= minimumBucketCount)
    {
        // Bucket count must be power of 2.
        map->bucketCount <<= 1;
    }

    lsize = sizeof(NAVI_SVC_HASHMAP_ENTRY *) * map->bucketCount;
    map->buckets = cnv_comm_Malloc(lsize);
    //LOG_SYS_DEBUG(">>>>>>>>>malloc: %p.", map->buckets);
    if(map->buckets == K_NULL)
    {
        cnv_comm_Free(map);
        return CNV_ERR_MALLOC;
    }
    memset(map->buckets, 0, lsize);

    map->size = 0;
    map->hash = in_pHashFunc;
    map->equals = in_pEqualsFunc;

    result  =  cnv_plat_InitCriticalSection(&map->lock, MUTEX_HASHMAP_TOKEN);
    if(result != K_SUCCEED)
    {
        cnv_comm_Free(map->buckets);
        cnv_comm_Free(map);
        return result;
    }
    *out_pHashmap = map;
    return result;
}

NAVI_SVC_HASHMAP *hashmap(void *in_pHashmap)
{
    return (NAVI_SVC_HASHMAP *)in_pHashmap;
}

/**
 * Hashes the given key.
 */
static unsigned int cnv_hashmap_hashKey(void *in_pHashmap, void *in_pKey)
{

    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    int h = map->hash(in_pKey);

    // We apply this secondary hashing discovered by Doug Lea to defend
    // against bad hashes.
    h += ~(h << 9);
    h ^= (((unsigned int) h) >> 14);
    h += (h << 4);
    h ^= (((unsigned int) h) >> 10);

    return h;
}

int cnv_hashmap_size(void *in_pHashmap)
{

    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    if(map != K_NULL)
    {
        return map->size;
    }
    return 0;
}

static int calculateIndex(int bucketCount, int hash)
{
    return ((int) hash) & (bucketCount - 1);
}

static void expandIfNecessary(void *in_pHashmap)
{

    int i, lsize;
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    // If the load factor exceeds 0.75...
    if(map->size > (map->bucketCount * 3 / 4))
    {
        // Start off with a 0.33 load factor.
        int newBucketCount = map->bucketCount << 1;
        NAVI_SVC_HASHMAP_ENTRY **newBuckets = K_NULL;
        lsize = sizeof(NAVI_SVC_HASHMAP_ENTRY *) *newBucketCount;

        if(newBuckets == K_NULL)
        {
            // Abort expansion.
            return;
        }
        newBuckets = cnv_comm_Malloc(lsize);
        // Move over existing entries.
        LOG_SYS_DEBUG(">>>>>>>>>malloc: %p.", newBuckets);
        for(i = 0; i < map->bucketCount; i++)
        {
            NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[i];
            while(entry != K_NULL)
            {
                NAVI_SVC_HASHMAP_ENTRY *next = entry->next;
                int index = calculateIndex(newBucketCount, entry->hash);
                entry->next = newBuckets[index];
                newBuckets[index] = entry;
                entry = next;
            }
        }

        // Copy over internals.
        cnv_comm_Free(map->buckets);
        map->buckets = newBuckets;
        map->bucketCount = newBucketCount;
    }
}

int cnv_hashmap_lock(void *in_pHashmap)
{
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    if(map != K_NULL)
    {
        cnv_plat_EnterCriticalSection(map->lock);
    }
    return K_SUCCEED;
}

int cnv_hashmap_unlock(void *in_pHashmap)
{
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    if(map != K_NULL)
    {
        cnv_plat_LeaveCriticalSection(map->lock);
    }
    return K_SUCCEED;
}
int cnv_hashmap_uninit(void *in_pHashmap)
{

    int i;
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    for(i = 0; i < map->bucketCount; i++)
    {
        NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[i];
        while(entry != K_NULL)
        {
            NAVI_SVC_HASHMAP_ENTRY *next = entry->next;
            cnv_comm_Free(entry);
            entry = next;
        }
    }
    cnv_comm_Free(map->buckets);
    cnv_plat_DeleteCriticalSection(map->lock);
    cnv_comm_Free(map);

    return K_SUCCEED;
}

int cnv_hashmap_hash(void *in_pkey, int in_lKeySize)
{

    int h = in_lKeySize;
    char *data = (char *) in_pkey;
    int i;
    for(i = 0; i < in_lKeySize; i++)
    {
        h = h * 31 + *data;
        data++;
    }
    return h;
}

static NAVI_SVC_HASHMAP_ENTRY *createEntry(void *key, int hash, void *value)
{

    NAVI_SVC_HASHMAP_ENTRY *entry = cnv_comm_Malloc(sizeof(NAVI_SVC_HASHMAP_ENTRY));
    //LOG_SYS_DEBUG(">>>>>>>>>malloc: %p.", entry);
    if(entry == K_NULL)
    {
        return K_NULL;
    }
    entry->key = key;
    entry->hash = hash;
    entry->value = value;
    entry->next = K_NULL;
    return entry;
}

static K_BOOL equalKeys(void *keyA, int hashA, void *keyB, int hashB,
                        pfnCNV_HASHMAP_EQUALS_CALLBACK equals)
{

    if(keyA == keyB)
    {
        return K_TRUE;
    }
    if(hashA != hashB)
    {
        return K_FALSE;
    }
    return equals(keyA, keyB);
}

int cnv_hashmap_put(void *in_pHashmap, void *in_pkey, void *in_pValue, void **out_pOldValue)
{

    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    int hash = cnv_hashmap_hashKey(map, in_pkey);
    int index = calculateIndex(map->bucketCount, hash);
    NAVI_SVC_HASHMAP_ENTRY **p = &(map->buckets[index]);

    if(out_pOldValue != K_NULL)
        *out_pOldValue = K_NULL;

    while(K_TRUE)
    {
        NAVI_SVC_HASHMAP_ENTRY *current = *p;

        // Add a new entry.
        if(current == NULL)
        {
            *p = createEntry(in_pkey, hash, in_pValue);
            if(*p == NULL)
            {
                return CNV_ERR_MALLOC;
            }
            map->size++;
            expandIfNecessary(map);
            return K_SUCCEED;
        }

        // Replace existing entry.
        if(equalKeys(current->key, current->hash, in_pkey, hash, map->equals))
        {
            void *oldValue = current->value;
            current->value = in_pValue;
            if(out_pOldValue != K_NULL)
                *out_pOldValue = oldValue;
            return K_SUCCEED;
        }
        // Move to next entry.
        p = &current->next;
    }
}

int cnv_hashmap_get(void *in_pHashmap, void *in_pkey, void **out_pValue)
{
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    int hash = cnv_hashmap_hashKey(map, in_pkey);
    int index = calculateIndex(map->bucketCount, hash);
    NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[index];
    *out_pValue = K_NULL;

    while(entry != NULL)
    {
        if(equalKeys(entry->key, entry->hash, in_pkey, hash, map->equals))
        {
            *out_pValue = entry->value;
            return K_SUCCEED;
        }
        entry = entry->next;
    }

    return K_FAILED;
}

K_BOOL cnv_hashmap_containsKey(void *in_pHashmap, void *in_pkey)
{

    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    int hash = cnv_hashmap_hashKey(map, in_pkey);
    int index = calculateIndex(map->bucketCount, hash);

    NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[index];
    while(entry != K_NULL)
    {
        if(equalKeys(entry->key, entry->hash, in_pkey, hash, map->equals))
        {
            return K_TRUE;
        }
        entry = entry->next;
    }

    return K_FALSE;
}

int cnv_hashmap_memoize(void *in_pHashmap, void *in_pKey,
                        pfnCNV_HASHMAP_MEMOIZE_CALLBACK in_pMemoizeFunc,
                        void *in_pContext,
                        void **out_pValue)
{

    void *value;
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    int hash = cnv_hashmap_hashKey(map, in_pKey);
    int index = calculateIndex(map->bucketCount, hash);
    NAVI_SVC_HASHMAP_ENTRY **p = &(map->buckets[index]);

    if(out_pValue != K_NULL)
        *out_pValue = K_NULL;

    while(K_TRUE)
    {
        NAVI_SVC_HASHMAP_ENTRY *current = *p;

        // Add a new entry.
        if(current == K_NULL)
        {
            *p = createEntry(in_pKey, hash, K_NULL);
            if(*p == NULL)
            {
                return CNV_ERR_MALLOC;
            }
            value = in_pMemoizeFunc(in_pKey, in_pContext);
            (*p)->value = value;
            map->size++;
            expandIfNecessary(map);
            if(out_pValue != K_NULL)
                *out_pValue = value;
            return K_SUCCEED;
        }

        // Return existing value.
        if(equalKeys(current->key, current->hash, in_pKey, hash, map->equals))
        {
            if(out_pValue != K_NULL)
                *out_pValue = current->value;

            return K_SUCCEED;
        }
        // Move to next entry.
        p = &current->next;
    }
}

int cnv_hashmap_remove(void *in_pHashmap, void *in_pKey, void **out_pValue)
{

    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    int hash = cnv_hashmap_hashKey(map, in_pKey);
    int index = calculateIndex(map->bucketCount, hash);

    // Pointer to the current entry.
    NAVI_SVC_HASHMAP_ENTRY **p = &(map->buckets[index]);
    NAVI_SVC_HASHMAP_ENTRY *current;

    if(out_pValue != K_NULL)
        *out_pValue = K_NULL;

    while((current = *p) != K_NULL)
    {
        if(equalKeys(current->key, current->hash, in_pKey, hash, map->equals))
        {
            void *value = current->value;
            *p = current->next;
            cnv_comm_Free(current);
            map->size--;
            if(out_pValue != K_NULL)
                *out_pValue = value;
            return K_SUCCEED;
        }
        p = &current->next;
    }

    return K_SUCCEED;
}

int cnv_hashmap_iterator(void *in_pHashmap,
                         pfnCNV_HASHMAP_ITERATOR_CALLBACK in_pIteratorFunc,
                         void *in_pContext)
{
    int i;
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);

    if(map != K_NULL)
    {
        for(i = 0; i < map->bucketCount; i++)
        {
            NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[i];
            while(entry != K_NULL)
            {
                if(!in_pIteratorFunc(entry->key, entry->value, in_pContext))
                {
                    return K_SUCCEED;
                }
                entry = entry->next;
            }
        }
    }
    return K_SUCCEED;
}

int cnv_hashmap_erase(void *in_pHashmap,
                      pfnCNV_HASHMAP_ERASE_CALLBACK in_pEraseFunc,
                      void *in_pContext)
{

    int i;
    K_BOOL bErase, result;
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);

    for(i = 0; i < map->bucketCount; i++)
    {
        NAVI_SVC_HASHMAP_ENTRY **p = &(map->buckets[i]);
        NAVI_SVC_HASHMAP_ENTRY *current;
        while((current = *p) != K_NULL)
        {
            bErase = K_FALSE;
            result = in_pEraseFunc(current->key, current->value, in_pContext, &bErase);
            if(bErase)
            {
                *p = current->next;
                cnv_comm_Free(current);
                map->size--;
                continue;
            }
            if(!result)
                return K_SUCCEED;
            p = &current->next;
        }
    }
    return K_SUCCEED;
}

int cnv_hashmap_capacity(void *in_pHashmap)
{

    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);
    if(map != K_NULL)
    {
        int bucketCount = map->bucketCount;
        return bucketCount * 3 / 4;
    }
    return 0;
}

int cnv_hashmap_countcollisions(void *in_pHashmap)
{

    int i, collisions = 0;
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);

    for(i = 0; i < map->bucketCount; i++)
    {
        NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[i];
        while(entry != NULL)
        {
            if(entry->next != NULL)
            {
                collisions++;
            }
            entry = entry->next;
        }
    }
    return collisions;
}

int cnv_hashmap_inthash(void *in_pKey)
{
    // Return the key value itself.
    return *((int *) in_pKey);
}

K_BOOL cnv_hashmap_intequals(void *in_pKeyA, void *in_pKeyB)
{

    int a = *((int *) in_pKeyA);
    int b = *((int *) in_pKeyB);
    return a == b;
}

int cnv_hashmap_charhash(void *in_pKey)
{
    int ret = 0, i;

    char *key = (char *)in_pKey;
    int key_size = strlen(key);
    for(i = 0; i < key_size; ++i)
    {
        //ret = ret * seed + key[i];
        ret = (ret << 7) + (ret << 1) + ret + key[i];
    }
    return ret;
}

K_BOOL cnv_hashmap_charequals(void *in_pKeyA, void *in_pKeyB)
{
    char *a = (char *)in_pKeyA;
    char *b = (char *)in_pKeyB;
    return  !strcmp(a, b);
}
/*
int cnv_hashmap_iterator_ex(void * in_pHashmap,
                             pfnCNV_HASHMAP_ITERATOR_CALLBACK_ex in_pIteratorFunc,
                             void *in_pContext1,)
{
    int i;
    NAVI_SVC_HASHMAP *map = hashmap(in_pHashmap);

    if(map != K_NULL)
    {
        for(i = 0; i < map->bucketCount; i++)
        {
            NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[i];
            while(entry != K_NULL)
            {
                if(!in_pIteratorFunc(entry->key, entry->value, in_pContext))
                {
                    return K_SUCCEED;
                }
                entry = entry->next;
            }
        }
    }
    return K_SUCCEED;
}
*/