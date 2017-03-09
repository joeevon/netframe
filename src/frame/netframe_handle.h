#ifndef __CNV_AGENT_HANDLE_H__
#define __CNV_AGENT_HANDLE_H__

#include "netframe_common.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /*=======================================================
    功能:
        开启handle线程
    =========================================================*/
    extern int handle_thread_start(HANDLE_THREAD_CONTEXT  *pHandleContexts);

    /*=======================================================
    功能:
        反初始化handle线程
    =========================================================*/
    extern void handle_thread_uninit(HANDLE_THREAD_CONTEXT *pHandleContexts);
#ifdef __cplusplus
};
#endif

#endif  //__CNV_AGENT_HANDLE_H__
