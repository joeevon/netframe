
/**********************************************************
Copyright (c) 2015, 凯立德 careland.  All rights reserved.
FileName: cnv_blocking_queue.h
Author: wangzy@careland.com.cn
Version: 1.0
Date: 2015-07-03
Description: 多线程阻塞消息队列
Function List:
1. 多线程
2. 阻塞式,支持秒级超时机制
3. 可直接交换两个非阻塞队列的指针
**********************************************************/

#ifndef __CNV_BLOCKING_QUEUE_H__
#define __CNV_BLOCKING_QUEUE_H__

#ifdef  __cplusplus
extern "C" {
#endif

#include "cnv_queue_type.h"
#include "cnv_unblock_queue.h"

    typedef struct __blocking_queue_ex_t
    {
        CNV_UNBLOCKING_QUEUE  *unblockqueue;      // 非阻塞队列
        pthread_mutexattr_t mutexattr_;    // 互斥锁
        pthread_mutex_t mutex_;  // 互斥锁
        pthread_cond_t cond_push_;   // 条件变量
        pthread_cond_t cond_poll_;    // 条件变量
    } CNV_BLOCKING_QUEUE;

    //初始化、反初始化队列
    int initiate_block_queue(CNV_BLOCKING_QUEUE *queue, int capacity, CNV_UNBLOCKING_QUEUE *unblockqueue);
    void desorty_block_queue(CNV_BLOCKING_QUEUE *queue);

    //增加和取出队列元素
    bool push_block_queue_tail(CNV_BLOCKING_QUEUE *queue, void *data, int seconds);
    void *poll_block_queue_head(CNV_BLOCKING_QUEUE *queue, int seconds);

    //获取队列元素个数
    int get_block_queue_count(CNV_BLOCKING_QUEUE *queue);

    //锁定,解锁
    bool lock_block_queue(CNV_BLOCKING_QUEUE *queue);
    void unlock_block_queue(CNV_BLOCKING_QUEUE *queue);

    //取队列元素
    struct queue_entry_t *get_block_queue_first(CNV_BLOCKING_QUEUE *queue);
    struct queue_entry_t *get_block_queue_next(struct queue_entry_t *np);

    // 遍历队列
    int iterator_block_queuqe(CNV_BLOCKING_QUEUE *queue, pfnQUEUE_ITERATOR_CALLBACK in_pIteratorFunc, void *in_pContext);

#ifdef __cplusplus
}
#endif

#endif
