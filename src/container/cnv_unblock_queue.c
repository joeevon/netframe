#include "cnv_unblock_queue.h"
#include "cnv_comm.h"
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

void initiate_unblock_queue(CNV_UNBLOCKING_QUEUE *queue, int capacity)
{
    SIMPLEQ_INIT(&queue->head_);
    queue->capacity_ = (capacity > 0 && capacity < INT32_MAX) ? capacity : 10000;
    queue->size_ = 0;
}

void destory_unblock_queue(CNV_UNBLOCKING_QUEUE *queue)
{

}

bool push_unblock_queue_tail(CNV_UNBLOCKING_QUEUE *queue, void *data)
{
    if(queue->capacity_ <= queue->size_)
    {
        return false;
    }

    struct queue_entry_t *node =
        (struct queue_entry_t *)malloc(sizeof(struct queue_entry_t));
    node->data_ = data;
    SIMPLEQ_INSERT_TAIL(&queue->head_, node, queue_entries_t);
    ++queue->size_;

    return true;
}

void *poll_unblock_queue_head(CNV_UNBLOCKING_QUEUE *queue)
{
    if(queue->size_ <= 0)
    {
        return 0;
    }

    struct queue_entry_t *node = queue->head_.sqh_first;
    SIMPLEQ_REMOVE_HEAD(&queue->head_, queue_entries_t);
    void *data = node->data_;
    --queue->size_;
    free(node);

    return data;
}

int get_unblock_queue_count(CNV_UNBLOCKING_QUEUE *queue)
{
    return queue->size_;
}

int get_unblock_queue_remainsize(CNV_UNBLOCKING_QUEUE *queue)
{
    int remainsize = -1;
    remainsize = queue->capacity_ - queue->size_;
    return remainsize;
}

struct queue_entry_t *get_unblock_queue_next(struct queue_entry_t *np)
{
    assert(np);
    return (struct queue_entry_t *)SIMPLEQ_NEXT(np, queue_entries_t);
}

struct queue_entry_t *get_unblock_queue_first(CNV_UNBLOCKING_QUEUE *queue)
{
    assert(queue);
    return SIMPLEQ_FIRST(&queue->head_);
}

int iterator_unblock_queuqe(CNV_UNBLOCKING_QUEUE *queue, pfnQUEUE_ITERATOR_CALLBACK in_pIteratorFunc, void *in_pContext)
{
    assert(queue);

    if(get_unblock_queue_count(queue) > 0)
    {
        struct queue_entry_t *queuenode = get_unblock_queue_first(queue);
        while(queuenode)
        {
            if(!in_pIteratorFunc(queuenode->data_, in_pContext))
            {
                return 1;
            }
            queuenode = get_unblock_queue_next(queuenode);
        }
    }
    return 0;
}

void  earase_unblock_queue(CNV_UNBLOCKING_QUEUE  *queue)
{
    void  *pOutqueue = NULL;
    K_INT32  lCount = get_unblock_queue_count(queue);
    while(lCount--)
    {
        pOutqueue = poll_unblock_queue_head(queue);
        cnv_comm_Free(pOutqueue);
    }
}