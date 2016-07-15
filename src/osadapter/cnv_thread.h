/****************************
    FileName:cnv_agent_thread.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        cnv_agent_thread 头文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-11
    *****************************/

#ifndef __CNV_THREAD_H__
#define __CNV_THREAD_H__

#include "netframe/cnv_core_typedef.h"
#include <pthread.h>

#ifdef __cplusplus
extern "C"
{
#endif

//线程回调函数
    typedef  unsigned int(*pfnCNV_PLAT_THREAD_RECALL)(void *lpThreadParameter);

//线程优先级
    typedef enum __enumNAVI_AGENT_THREAD_PRIORITY
    {
        TP_TIME_CRITICAL,
        TP_HIGHEST,
        TP_ABOVE_NORMAL,
        TP_NORMAL,
        TP_BELOW_NORMAL,
        TP_LOWEST,
        TP_ABOVE_IDLE,
        TP_IDLE
    } enumNAVI_AGENT_THREAD_PRIORITY;

    /*=======================================================
    功能:
        创建一个线程
    参数:
        [in]
            in_pFun 函数指针，该函数接受一个参数，返回一个int类型的值
            in_param 传入线程的参数
            in_attribute 线程的属性 0 - 创建后马上运行,1 - 创建后处于挂起状态
        [out]
            out_threadId 创建出来的线程的id
            out_pThread 创建的线程，空间由外部分配
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int hmi_plat_CreateThread(pfnCNV_PLAT_THREAD_RECALL in_pFun, void *in_param,  int in_attribute, pthread_t *out_threadId, K_HANDLE *out_pThread);

    /*=======================================================
    功能:
        停掉一个线程
    参数:
        [in]
            in_pthreadhandle 所要停止的线程的句柄
            in_lWaitTime 等待线程退出的时间(毫秒)，时间到时如果线程还没有自然终止则强行终止
        [out]
            无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int hmi_plat_StopThread(K_HANDLE  in_pthread, int in_lWaitTime);

    /*=======================================================
    功能:
        检测一个线程是否存在
    参数:
        [in]
            in_pthread  线程的句柄
        [out]
            无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int hmi_plat_IsThreadAlive(K_HANDLE in_pthread);

    /*=======================================================
    功能:
        取得当前线程的ID
    参数:
        无
    返回值:
        当前线程的ID
    =========================================================*/
    int hmi_plat_GetCurrentThreadId();

    /*=======================================================
     功能:
         设置线程的优先级
     参数:
     [in]
         in_pthread   线程的句柄
         in_ePriority   优先级
     [out]
         无
     返回值:
         0    成功
         其它 失败
     =========================================================*/
    int hmi_plat_SetThreadPriority(K_HANDLE in_pthread, enumNAVI_AGENT_THREAD_PRIORITY in_ePriority);

#ifdef __cplusplus
};
#endif

#endif  //__CNV_THREAD_H__
