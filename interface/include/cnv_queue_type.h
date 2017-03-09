#ifndef __CNV_QUEUE_TYPE_H__
#define __CNV_QUEUE_TYPE_H__

#ifdef  __cplusplus
extern "C" {
#endif

#include "cnv_core_typedef.h"
#include <sys/queue.h>
#include <pthread.h>
#include <stdbool.h>

    typedef void (*ENTRY_FREE_CB)(void *);
    typedef K_BOOL(*pfnQUEUE_ITERATOR_CALLBACK)(void *nodedata, void *in_pContext);

#define POLL_QUEUE_WAIT 1
#define PUSH_QUEUE_WAIT 2
#define WAIT_QUEUE_SUCCESS 1
#define WAIT_QUEUE_TIMEOUT 2
#define WAIT_QUEUE_ERROR 3

    struct queue_entry_t
    {
        void *data_;
        SIMPLEQ_ENTRY(queue_entry_t) queue_entries_t;
    };
    SIMPLEQ_HEAD(queue_head_t, queue_entry_t);

#ifdef __cplusplus
}
#endif

#endif
