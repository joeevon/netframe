#include "cnv_blocking_queue.h"
#include "cnv_comm.h"
#include "cnv_base_define.h"
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

int initiate_block_queue(CNV_BLOCKING_QUEUE *queue, int capacity, CNV_UNBLOCKING_QUEUE *unblockqueue)
{
    assert(queue);

    if(unblockqueue)
    {
        queue->unblockqueue = unblockqueue;
    }
    else
    {
        if(!queue->unblockqueue)
        {
            queue->unblockqueue = (CNV_UNBLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
            if(!queue->unblockqueue)
            {
                return  -1;
            }
        }
        initiate_unblock_queue(queue->unblockqueue, capacity);
    }

    pthread_mutexattr_init(&queue->mutexattr_);
    pthread_mutexattr_settype(&queue->mutexattr_, PTHREAD_MUTEX_ERRORCHECK_NP);
    pthread_mutex_init(&queue->mutex_, &queue->mutexattr_);
    pthread_cond_init(&queue->cond_poll_, 0);
    pthread_cond_init(&queue->cond_push_, 0);
    return 0;
}

int wait_block_queue(CNV_BLOCKING_QUEUE *queue, int action, int seconds)
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

void desorty_block_queue(CNV_BLOCKING_QUEUE *queue)
{
    assert(queue);
    pthread_cond_destroy(&queue->cond_push_);
    pthread_cond_destroy(&queue->cond_poll_);
    pthread_mutex_destroy(&queue->mutex_);
    pthread_mutexattr_destroy(&queue->mutexattr_);
}

bool push_block_queue_tail(CNV_BLOCKING_QUEUE *queue, void *data, int seconds)
{
    assert(queue);

    if(!lock_block_queue(queue)) return false;
    CNV_UNBLOCKING_QUEUE *unblockqueue = queue->unblockqueue;
    while(unblockqueue->capacity_ <= unblockqueue->size_)
    {
        //队列满,自动解锁,等待取出唤醒
        if(WAIT_QUEUE_TIMEOUT == wait_block_queue(queue, PUSH_QUEUE_WAIT, seconds))
        {
            pthread_mutex_unlock(&queue->mutex_);
            return false;
        }
    }

    struct queue_entry_t *node =
        (struct queue_entry_t *)malloc(sizeof(struct queue_entry_t));
    node->data_ = data;
    SIMPLEQ_INSERT_TAIL(&unblockqueue->head_, node, queue_entries_t);
    ++unblockqueue->size_;
    pthread_mutex_unlock(&queue->mutex_);
    pthread_cond_broadcast(&queue->cond_poll_);

    return true;
}

void *poll_block_queue_head(CNV_BLOCKING_QUEUE *queue, int seconds)
{
    assert(queue);

    void *data = 0;
    if(!lock_block_queue(queue)) return data;
    CNV_UNBLOCKING_QUEUE *unblockqueue = queue->unblockqueue;
    while(unblockqueue->size_ <= 0)
    {
        //队列空,自动解锁,等待插入唤醒
        if(WAIT_QUEUE_SUCCESS != wait_block_queue(queue, POLL_QUEUE_WAIT, seconds))
        {
            pthread_mutex_unlock(&queue->mutex_);
            return data;
        }
    }

    struct queue_entry_t *node = unblockqueue->head_.sqh_first;
    SIMPLEQ_REMOVE_HEAD(&unblockqueue->head_, queue_entries_t);
    data = node->data_;
    --unblockqueue->size_;
    free(node);
    pthread_mutex_unlock(&queue->mutex_);
    pthread_cond_broadcast(&queue->cond_push_);

    return data;
}

int get_block_queue_count(CNV_BLOCKING_QUEUE *queue)
{
    if(!lock_block_queue(queue)) return -1;
    int size = queue->unblockqueue->size_;
    pthread_mutex_unlock(&queue->mutex_);
    return size;
}

bool lock_block_queue(CNV_BLOCKING_QUEUE *queue)
{
    assert(queue);
    if(0 != pthread_mutex_lock(&queue->mutex_))
    {
        int err = errno;
        if(err == EBUSY || err == EDEADLK)
        {
            return false;
        }
    }

    return true;
}

void unlock_block_queue(CNV_BLOCKING_QUEUE *queue)
{
    assert(queue);
    pthread_mutex_unlock(&queue->mutex_);
}
