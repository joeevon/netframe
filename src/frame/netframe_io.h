#ifndef __CNV_AGENT_IO_H__
#define __CNV_AGENT_IO_H__

#include "netframe_structdefine.h"
#include "netframe_handle.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /*=======================================================
    功能:
        开启io线程
    =========================================================*/
    extern int io_thread_start(HANDLE_THREAD_CONTEXT *pHandleContexts, IO_THREAD_CONTEXT *pIoThreadContexts, CNV_UNBLOCKING_QUEUE *queServer);

    /*=======================================================
    功能:
        反初始化io线程
    =========================================================*/
    extern void io_thread_uninit(IO_THREAD_CONTEXT *pIoThreadContexts);

#ifdef __cplusplus
};
#endif

#endif  //__CNV_AGENT_IO_H__
