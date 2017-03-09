#ifndef CNV_CARELAND_QUEUE_H_
#define CNV_CARELAND_QUEUE_H_

#ifdef  __cplusplus
extern "C" {
#endif

//#include "cnv_base_define.h"
#include "cnv_queue_type.h"

    typedef struct blocking_queue_t
    {
        //最大容量
        int capacity_;
        //当前元素数
        int size_;
        pthread_mutexattr_t mutexattr_;
        //一把锁,两个条件变量的经典实现
        pthread_mutex_t mutex_;
        pthread_cond_t cond_push_;
        pthread_cond_t cond_poll_;
        //队列头
        struct queue_head_t head_;
    } BLOCKING_QUEUE_T;

    void initiate_queue(BLOCKING_QUEUE_T *queue, int capacity);
    void destroy_queue(BLOCKING_QUEUE_T *queue);

    //增加和取出队列元素
    bool push_queue_tail(BLOCKING_QUEUE_T *queue, void *data, int seconds);
    void *poll_queue_head(BLOCKING_QUEUE_T *queue, int seconds);

    //获取队列元素个数
    int get_queue_count(BLOCKING_QUEUE_T *queue);
    int get_queue_capacity(BLOCKING_QUEUE_T *queue);
    int get_queue_remainsize(BLOCKING_QUEUE_T *queue);

    //锁定,解锁和遍历整个队列
    bool lock_queue(BLOCKING_QUEUE_T *queue);
    void unlock_queue(BLOCKING_QUEUE_T *queue);
    struct queue_entry_t *get_queue_first(BLOCKING_QUEUE_T *queue);
    struct queue_entry_t *get_queue_next(struct queue_entry_t *np);
    //遍历队列
    int iterator_queuqe(BLOCKING_QUEUE_T *queue, pfnQUEUE_ITERATOR_CALLBACK in_pIteratorFunc, void *in_pContext);

    //擦除队列
    bool  earase_queue(BLOCKING_QUEUE_T  *queue);

#ifdef __cplusplus
}
#endif

#endif
