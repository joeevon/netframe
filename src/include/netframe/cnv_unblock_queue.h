
/**********************************************************
Copyright (c) 2015, 凯立德 careland.  All rights reserved.
FileName: cnv_unblock_queue.h
Author: wangzy@careland.com.cn
Version: 1.0
Date: 2015-07-03
Description: 单线程非阻塞消息队列
**********************************************************/

#ifndef __CNV_UNBLOCK_QUEUE_H__
#define __CNV_UNBLOCK_QUEUE_H__

#ifdef  __cplusplus
extern "C" {
#endif

#include "cnv_queue_type.h"

    typedef struct __unblocking_queue_t
    {
        int capacity_;    // 最大容量
        int size_;    // 当前元素数
        struct queue_head_t head_;      // 队列头
    } CNV_UNBLOCKING_QUEUE;

    //初始化、反初始化队列
    void initiate_unblock_queue(CNV_UNBLOCKING_QUEUE *queue, int capacity);
    void destory_unblock_queue(CNV_UNBLOCKING_QUEUE *queue);

    //增加和取出队列元素
    bool push_unblock_queue_tail(CNV_UNBLOCKING_QUEUE *queue, void *data);
    void *poll_unblock_queue_head(CNV_UNBLOCKING_QUEUE *queue);

    //获取队列元素个数
    int get_unblock_queue_count(CNV_UNBLOCKING_QUEUE *queue);
    int get_unblock_queue_remainsize(CNV_UNBLOCKING_QUEUE *queue);

    //取队列元素
    struct queue_entry_t *get_unblock_queue_first(CNV_UNBLOCKING_QUEUE *queue);
    struct queue_entry_t *get_unblock_queue_next(struct queue_entry_t *np);

    // 遍历队列
    int iterator_unblock_queuqe(CNV_UNBLOCKING_QUEUE *queue, pfnQUEUE_ITERATOR_CALLBACK in_pIteratorFunc, void *in_pContext);

    //擦除队列
    void  earase_unblock_queue(CNV_UNBLOCKING_QUEUE  *queue);

#ifdef __cplusplus
}
#endif

#endif
