#include "cnv_lock_free_queue.h"
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>

unsigned int AtomAdd(void *var, const unsigned int value)
{
    return __sync_fetch_and_add((unsigned int *)(var), value);  //NOLINT
}

unsigned int AtomDec(void *var, int value)
{
    value = value * -1;
    return __sync_fetch_and_add((unsigned int *)(var), value);  //NOLINT
}

int wait_lockfree_queue(LOCKFREE_QUEUE *lockfree_queue, int action, int seconds)
{
    int result = 0;

    pthread_cond_t *cond = (action == POLL_QUEUE_WAIT) ?
                           (&lockfree_queue->cond_dequeue_) : (&lockfree_queue->cond_enqueue_);

    struct timeval now = {0};
    gettimeofday(&now, 0);
    struct timespec outtime = {0};
    outtime.tv_sec = now.tv_sec + seconds;
    result = pthread_cond_timedwait(cond, &lockfree_queue->mutex_, &outtime);

    if(ETIMEDOUT == result)
    {
        result = WAIT_QUEUE_TIMEOUT;
    }
    else if(0 == result)
    {
        result = WAIT_QUEUE_SUCCESS;
    }
    else
    {
        result = WAIT_QUEUE_ERROR;
    }
    return result;
}

void lockfree_queue_init(LOCKFREE_QUEUE *lockfree_queue, int Size)
{
    lockfree_queue->Size = Size;
    lockfree_queue->Pop = 0;
    lockfree_queue->Push = 0;
    lockfree_queue->queuenode = (QUEUE_NODE *)malloc(sizeof(QUEUE_NODE) * Size);
    lockfree_queue_clear(lockfree_queue, Size);
    pthread_mutexattr_init(&lockfree_queue->mutexattr_);
    pthread_mutexattr_settype(&lockfree_queue->mutexattr_, PTHREAD_MUTEX_ERRORCHECK_NP);
    pthread_mutex_init(&lockfree_queue->mutex_, &lockfree_queue->mutexattr_);
    pthread_cond_init(&lockfree_queue->cond_dequeue_, 0);
    pthread_cond_init(&lockfree_queue->cond_enqueue_, 0);
}

void lockfree_queue_uninit(LOCKFREE_QUEUE *lockfree_queue)
{
    if(lockfree_queue->queuenode)
    {
        free(lockfree_queue->queuenode);
        lockfree_queue->queuenode = 0;
    }
    pthread_cond_destroy(&lockfree_queue->cond_enqueue_);
    pthread_cond_destroy(&lockfree_queue->cond_dequeue_);
    pthread_mutex_destroy(&lockfree_queue->mutex_);
    pthread_mutexattr_destroy(&lockfree_queue->mutexattr_);
}

void lockfree_queue_clear(LOCKFREE_QUEUE *lockfree_queue, int Size)
{
    if(0 == lockfree_queue->queuenode) return;

    lockfree_queue->WriteAbleCount = Size;
    lockfree_queue->ReadAbleCount = 0;

    unsigned int i = 0;
    for(i = 0; i < Size; i++)
    {
        lockfree_queue->queuenode[i].IsEmpty = ktrue;
        lockfree_queue->queuenode[i].pObject = 0;
    }
}

kbool lockfree_queue_enqueue(LOCKFREE_QUEUE *lockfree_queue, void *pObject, int seconds)
{
    while(lockfree_queue->WriteAbleCount <= 0)
    {
        pthread_mutex_lock(&lockfree_queue->mutex_); //需要上锁条件等待才能生效,此锁没有实际用途
        if(WAIT_QUEUE_TIMEOUT == wait_lockfree_queue(lockfree_queue, PUSH_QUEUE_WAIT, seconds))
        {
            pthread_mutex_unlock(&lockfree_queue->mutex_);
            return kfalse;
        }
        pthread_mutex_unlock(&lockfree_queue->mutex_);
    }

    if(0 >= AtomDec(&lockfree_queue->WriteAbleCount, 1))
    {
        AtomAdd(&lockfree_queue->WriteAbleCount, 1);
        return kfalse;
    }

    unsigned int pushPos = AtomAdd(&lockfree_queue->Push, 1);
    pushPos = pushPos % lockfree_queue->Size;

    if(!lockfree_queue->queuenode[pushPos].IsEmpty)
    {
        AtomAdd(&lockfree_queue->WriteAbleCount, 1);
        AtomDec(&lockfree_queue->Push, 1);
        return kfalse;
    }
    lockfree_queue->queuenode[pushPos].pObject = pObject;
    lockfree_queue->queuenode[pushPos].IsEmpty = kfalse;

    AtomAdd(&lockfree_queue->ReadAbleCount, 1);
    pthread_cond_broadcast(&lockfree_queue->cond_dequeue_);


    return ktrue;
}

void *lockfree_queue_dequeue(LOCKFREE_QUEUE *lockfree_queue, int seconds)
{
    while(lockfree_queue->ReadAbleCount <= 0)
    {
        pthread_mutex_lock(&lockfree_queue->mutex_);  //需要上锁条件等待才能生效,此锁没有实际用途
        if(WAIT_QUEUE_SUCCESS != wait_lockfree_queue(lockfree_queue, POLL_QUEUE_WAIT, seconds))
        {
            pthread_mutex_unlock(&lockfree_queue->mutex_);
            return 0;
        }
        pthread_mutex_unlock(&lockfree_queue->mutex_);
    }

    if(0 >= AtomDec(&lockfree_queue->ReadAbleCount, 1))
    {
        AtomAdd(&lockfree_queue->ReadAbleCount, 1);
        return 0;
    }

    unsigned int popPos = AtomAdd(&lockfree_queue->Pop, 1);
    popPos = popPos % lockfree_queue->Size;

    if(lockfree_queue->queuenode[popPos].IsEmpty)
    {
        AtomAdd(&lockfree_queue->ReadAbleCount, 1);
        AtomDec(&lockfree_queue->Pop, 1);
        return 0;
    }

    void *pObject = lockfree_queue->queuenode[popPos].pObject;
    lockfree_queue->queuenode[popPos].pObject = 0;
    lockfree_queue->queuenode[popPos].IsEmpty = ktrue;
    AtomAdd(&lockfree_queue->WriteAbleCount, 1);
    pthread_cond_broadcast(&lockfree_queue->cond_enqueue_);

    return pObject;
}

int lockfree_queue_len(LOCKFREE_QUEUE *lockfree_queue)
{
    return lockfree_queue->ReadAbleCount;
}
