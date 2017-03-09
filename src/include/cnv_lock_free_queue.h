#ifndef  __cnv_lock_free_queue_h__
#define  __cnv_lock_free_queue_h__

#ifdef  __cplusplus
extern "C" {
#endif

#include "cnv_queue_type.h"
#include <sys/types.h>

    typedef enum _enumkbool
    {
        kfalse = 0x00000000,
        ktrue = 0x00000001,
    }
    kbool;

    typedef struct _QUEUE_NODE
    {
        kbool IsEmpty;
        void *pObject;
    } QUEUE_NODE;

    typedef struct  _LOCKFREE_QUEUE
    {
        unsigned int Push;
        unsigned int Pop;
        unsigned int Size;
        int WriteAbleCount;
        int ReadAbleCount;
        QUEUE_NODE *queuenode;
        pthread_mutexattr_t mutexattr_;
        pthread_mutex_t mutex_;
        pthread_cond_t cond_enqueue_;
        pthread_cond_t cond_dequeue_;
    } LOCKFREE_QUEUE;

    extern unsigned int AtomAdd(void *var, const unsigned int value);

    extern unsigned int AtomDec(void *var, int value);

    extern void lockfree_queue_init(LOCKFREE_QUEUE *lockfree_queue, int Size);

    extern void lockfree_queue_uninit(LOCKFREE_QUEUE *lockfree_queue);

    extern void lockfree_queue_clear(LOCKFREE_QUEUE *lockfree_queue, int Size);

    extern kbool lockfree_queue_enqueue(LOCKFREE_QUEUE *lockfree_queue, void *pObject, int seconds);

    extern void *lockfree_queue_dequeue(LOCKFREE_QUEUE *lockfree_queue, int seconds);

    extern int lockfree_queue_len(LOCKFREE_QUEUE *lockfree_queue);

#ifdef __cplusplus
}
#endif

#endif // __cnv_lock_free_queue_h__
