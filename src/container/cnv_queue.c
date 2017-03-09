#include "cnv_queue.h"
#include "cnv_comm.h"
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

bool lock_queue(BLOCKING_QUEUE_T *queue)
{
    assert(queue);
    bool result = false;
    if(0 != pthread_mutex_lock(&queue->mutex_))
    {
        int err = errno;
        if(err == EBUSY || err == EDEADLK)
        {
            return result;
        }
    }
    return result = true;
}

void unlock_queue(BLOCKING_QUEUE_T *queue)
{
    assert(queue);
    pthread_mutex_unlock(&queue->mutex_);
}

int wait_queue(BLOCKING_QUEUE_T *queue, int action, int seconds)
{
    int result = 0;

    pthread_cond_t *cond = (action == POLL_QUEUE_WAIT) ?
                           (&queue->cond_poll_) : (&queue->cond_push_);

    if(seconds <= 0)
    {
        result = pthread_cond_wait(cond, &queue->mutex_);
    }
    else
    {
        struct timeval now = { 0 };
        gettimeofday(&now, 0);
        struct timespec outtime = { 0 };
        //outtime.tv_sec = now.tv_sec + milliseconds / 1000;
        //outtime.tv_nsec = now.tv_usec * 1000 + milliseconds % 1000 * 1000 * 1000;
        //效率考虑,精度到秒
        outtime.tv_sec = now.tv_sec + seconds;
        result = pthread_cond_timedwait(cond, &queue->mutex_, &outtime);
    }

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

void initiate_queue(BLOCKING_QUEUE_T *queue, int capacity)
{
    assert(queue);

    SIMPLEQ_INIT(&queue->head_);
    queue->capacity_ = (capacity > 0 && capacity < INT32_MAX) ? capacity : 10000;
    queue->size_ = 0;
    pthread_mutexattr_init(&queue->mutexattr_);
    pthread_mutexattr_settype(&queue->mutexattr_, PTHREAD_MUTEX_ERRORCHECK_NP);
    pthread_mutex_init(&queue->mutex_, &queue->mutexattr_);
    pthread_cond_init(&queue->cond_poll_, 0);
    pthread_cond_init(&queue->cond_push_, 0);
}

void destroy_queue(BLOCKING_QUEUE_T *queue)
{
    assert(queue);
    pthread_cond_destroy(&queue->cond_push_);
    pthread_cond_destroy(&queue->cond_poll_);
    pthread_mutex_destroy(&queue->mutex_);
    pthread_mutexattr_destroy(&queue->mutexattr_);
}

bool push_queue_tail(BLOCKING_QUEUE_T *queue, void *data, int seconds)
{
    assert(queue && data);

    if(!lock_queue(queue)) return false;
    while(queue->capacity_ <= queue->size_)
    {
        //队列满,自动解锁,等待取出唤醒
        if(WAIT_QUEUE_TIMEOUT == wait_queue(queue, PUSH_QUEUE_WAIT, seconds))
        {
            pthread_mutex_unlock(&queue->mutex_);
            return false;
        }
    }

    struct queue_entry_t *node =
        (struct queue_entry_t *)malloc(sizeof(struct queue_entry_t));
    node->data_ = data;
    SIMPLEQ_INSERT_TAIL(&queue->head_, node, queue_entries_t);
    ++queue->size_;
    pthread_mutex_unlock(&queue->mutex_);
    pthread_cond_broadcast(&queue->cond_poll_);

    return true;
}

void *poll_queue_head(BLOCKING_QUEUE_T *queue, int seconds)
{
    assert(queue);

    void *data = 0;
    if(!lock_queue(queue)) return data;
    while(queue->size_ <= 0)
    {
        //队列空,自动解锁,等待插入唤醒
        if(WAIT_QUEUE_SUCCESS != wait_queue(queue, POLL_QUEUE_WAIT, seconds))
        {
            pthread_mutex_unlock(&queue->mutex_);
            return data;
        }
    }

    struct queue_entry_t *node = queue->head_.sqh_first;
    SIMPLEQ_REMOVE_HEAD(&queue->head_, queue_entries_t);
    data = node->data_;
    --queue->size_;
    free(node);
    pthread_mutex_unlock(&queue->mutex_);
    pthread_cond_broadcast(&queue->cond_push_);

    return data;
}

int get_queue_count(BLOCKING_QUEUE_T *queue)
{
    int size = -1;
    if(!lock_queue(queue)) return size;
    size = queue->size_;
    unlock_queue(queue);
    return size;
}

int get_queue_capacity(BLOCKING_QUEUE_T *queue)
{
    int capacity = -1;
    if(!lock_queue(queue)) return capacity;
    capacity = queue->capacity_;
    unlock_queue(queue);
    return capacity;
}

int get_queue_remainsize(BLOCKING_QUEUE_T *queue)
{
    int remainsize = -1;
    if(!lock_queue(queue)) return remainsize;
    remainsize = queue->capacity_ - queue->size_;
    unlock_queue(queue);
    return remainsize;
}

struct queue_entry_t *get_queue_next(struct queue_entry_t *np)
{
    assert(np);
    return (struct queue_entry_t *)SIMPLEQ_NEXT(np, queue_entries_t);
}

struct queue_entry_t *get_queue_first(BLOCKING_QUEUE_T *queue)
{
    assert(queue);
    return SIMPLEQ_FIRST(&queue->head_);
}

int iterator_queuqe(BLOCKING_QUEUE_T *queue, pfnQUEUE_ITERATOR_CALLBACK in_pIteratorFunc, void *in_pContext)
{
    if(get_queue_count(queue) > 0)
    {
        if(!lock_queue(queue)) return false;
        struct queue_entry_t *queuenode = get_queue_first(queue);
        while(queuenode)
        {
            if(!in_pIteratorFunc(queuenode->data_, in_pContext))
            {
                unlock_queue(queue);
                return 1;
            }
            queuenode = get_queue_next(queuenode);
        }
        unlock_queue(queue);
    }
    return 0;
}

bool  earase_queue(BLOCKING_QUEUE_T  *queue)
{
    void  *pOutqueue = NULL;
    K_INT32  lCount = get_queue_count(queue);
    while(lCount--)
    {
        pOutqueue = poll_queue_head(queue, 1);
        cnv_comm_Free(pOutqueue);
    }
    return true;
}
