/****************************
FileName:netframe_auxiliary.h
(C) Copyright 2014 by Careland
凯立德秘密信息
Description:
主要功能简述
    netframe_auxiliary h文件
Note:
    Author:WangZhiyong
    Create Date: 2016-05-12
*****************************/

#ifndef __NETFRAME_AUXILIARY_H__
#define __NETFRAME_AUXILIARY_H__

#include "netframe_structdefine.h"
#include "netframe_handle.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /*=======================================================
    功能:
        开启auxiliary线程
    =========================================================*/
    extern int auxiliary_thread_start(IO_THREAD_CONTEXT *pIoThreadContexts, AUXILIARY_THREAD_CONTEXT *pAuxiliaryContexts);

    /*=======================================================
    功能:
        反初始化auxiliary线程
    =========================================================*/
    extern void auxiliary_thread_uninit(AUXILIARY_THREAD_CONTEXT *pAuxiliaryContexts);

#ifdef __cplusplus
};
#endif

#endif  //__NETFRAME_AUXILIARY_H__
