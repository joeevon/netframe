/****************************
    FileName:netframe_handle.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_handle h文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-19
*****************************/

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

    /*=======================================================
    功能:
        handle线程获取io线程环境变量
    =========================================================*/
    extern int handle_set_iothread_context(IO_THREAD_CONTEXT *pIoThreadContexts, HANDLE_THREAD_CONTEXT *pHandleContexts);

#ifdef __cplusplus
};
#endif

#endif  //__CNV_AGENT_HANDLE_H__
