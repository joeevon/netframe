/****************************
    FileName:netframe_main.c
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        服务 入口文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-07
*****************************/

#include "netframe_main.h"
#include "netframe_common.h"
#include "log/cnv_liblog4cplus.h"
#include "netframe_io.h"
#include "netframe_handle.h"
#include "netframe_accept.h"
#include "cnv_thread.h"
#include "cnv_comm.h"
#include <signal.h>

GLOBAL_PARAMS  g_params;      //全局配置参数
ACCEPT_THREAD_CONTEXT g_tAcceptContext;
IO_THREAD_CONTEXT g_szIoThreadContexts[MAX_IO_THREAD];
HANDLE_THREAD_CONTEXT  g_szHandleContexts[MAX_HANDLE_THREAD];

void  netframe_pthread_join()
{
    int  i = 0;

    if(g_params.tConfigAccept.lNumberOfPort > 0)
    {
        pthread_join(g_params.tConfigAccept.ulThreadId, (void **)0);
    }

    for(i = 0; i  < g_params.tConfigHandle.lNumberOfThread; i++)
    {
        pthread_join(g_params.tConfigHandle.szConfigHandleItem[i].ulThreadId, (void **)0);
    }

    for(i = 0; i  < g_params.tConfigIO.lNumberOfThread; i++)
    {
        pthread_join(g_params.tConfigIO.szConfigIOItem[i].ulThreadId, (void **)0);
    }
}

void netframe_pthread_stop()
{
    int  i = 0;

    for(i = 0; i  < g_params.tConfigHandle.lNumberOfThread; i++)
    {
        hmi_plat_StopThread(g_params.tConfigHandle.szConfigHandleItem[i].ThreadHandle, 50);
    }

    for(i = 0; i  < g_params.tConfigIO.lNumberOfThread; i++)
    {
        hmi_plat_StopThread(g_params.tConfigIO.szConfigIOItem[i].ThreadHandle, 50);
    }

    if(g_params.tConfigAccept.lNumberOfPort > 0)
    {
        hmi_plat_StopThread(g_params.tConfigAccept.ThreadHandle, 50);
    }
}

int  get_svrconf_queue(CNV_UNBLOCKING_QUEUE **queServer)
{
    CNV_UNBLOCKING_QUEUE *queue = (CNV_UNBLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
    if(!queue)
    {
        return  CNV_ERR_MALLOC;
    }
    initiate_unblock_queue(queue, 5000);
    *queServer = queue;

    return CNV_ERR_OK;
}

static void sig_donothing(int signo)
{
    //printf("do nothing");
}

void  netframe_uninit(ACCEPT_THREAD_CONTEXT *pAcceptContexts, IO_THREAD_CONTEXT *pIoThreadContexts, HANDLE_THREAD_CONTEXT *pHandleContexts)
{
    accept_thread_uninit(pAcceptContexts);
    io_thread_uninit(pIoThreadContexts);
    handle_thread_uninit(pHandleContexts);
}

int  initial_netframe(char *strConfPath, CNV_UNBLOCKING_QUEUE *queServer, int nMaxPacketSize)
{
    int nRet = CNV_ERR_OK;
    bzero(&g_params, sizeof(g_params));
    bzero(&g_tAcceptContext, sizeof(g_tAcceptContext));
    bzero(g_szIoThreadContexts, sizeof(g_szIoThreadContexts));
    bzero(g_szHandleContexts, sizeof(g_szHandleContexts));

    if(signal(SIGCHLD, sig_donothing) == SIG_ERR)
    {
        return -1;
    }

    if(signal(SIGPIPE, sig_donothing) == SIG_ERR)
    {
        return -1;
    }

    if(signal(SIGALRM, sig_donothing) == SIG_ERR)
    {
        return -1;
    }

    if(nMaxPacketSize <= 0 || nMaxPacketSize > MAX_PACKET_SZIE)
    {
        LOG_SYS_FATAL("buffer size is illegal:%d! it should between 0 and 65000.", nMaxPacketSize);
        return -1;
    }
    g_params.nMaxBufferSize = nMaxPacketSize;

    //初始化配置文件路径
    nRet = netframe_init_path(strConfPath);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("netframe_init_path failed!");
        return  nRet;
    }

    LOG_SYS_DEBUG("ConfigPath: %s", g_params.tConfigPath.strConfigPath);

    //初始化框架配置
    nRet = netframe_init_config();
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("netframe_init_config failed !");
        return nRet;
    }

    //开启handle线程
    nRet = handle_thread_start(g_szHandleContexts);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("handle_thread_start error : %d ", nRet);
        return  nRet;
    }
    LOG_SYS_DEBUG("handle_thread_start ok.");

    //开启IO线程
    nRet = io_thread_start(g_szHandleContexts, g_szIoThreadContexts, queServer);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("io_thread_start error : %d ", nRet);
        return  nRet;
    }
    LOG_SYS_DEBUG("io_thread_start ok.");

    //开启accept线程
    nRet = accept_thread_start(g_szIoThreadContexts, &g_tAcceptContext);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("cnv_agent_start_accept_thread error : %d ", nRet);
        return  nRet;
    }
    LOG_SYS_DEBUG("accept_thread_start ok.");

    netframe_pthread_join();   //线程挂起
    netframe_pthread_stop(); //终止线程
    netframe_uninit(&g_tAcceptContext, g_szIoThreadContexts, g_szHandleContexts);  //反初始化

    return  CNV_ERR_OK;
}
