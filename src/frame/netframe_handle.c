/****************************
    FileName:netframe_handle.c
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_handle  C文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-19
*****************************/

#include "netframe_handle.h"
#include "cnv_thread.h"
#include "netframe/common_type.h"
#include "netframe/cnv_core_typedef.h"
#include "netframe/cnv_lock_free_queue.h"
#include "netframe/netframe_net.h"
#include "log/cnv_liblog4cplus.h"
#include "netframe_common.h"
#include "netframe/cnv_comm.h"
#include "netframe/cnv_hashmap.h"
#include <unistd.h>
#include <time.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

K_BOOL earase_hashtimer_callback(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase)
{
    HASHMAP_VALUE *pHashValue = (HASHMAP_VALUE *)pValue;
    cnv_comm_Free(pKey);
    TIMER_TASK_STRUCT *ptTimerTaskStr = (TIMER_TASK_STRUCT *)pHashValue->pValue;
    close(ptTimerTaskStr->timerfd);
    cnv_comm_Free(pHashValue->pValue);
    cnv_comm_Free(pValue);

    *bIsEarase = K_TRUE;
    return K_TRUE;
}

void  free_iohandle_lockfreequeue(LOCKFREE_QUEUE  *io_handle_msgque)
{
    IO_TO_HANDLE_DATA *IoHandleData = NULL;
    int  lCount = lockfree_queue_len(io_handle_msgque);
    while(lCount--)
    {
        IoHandleData = (IO_TO_HANDLE_DATA *)lockfree_queue_dequeue(io_handle_msgque, 1);
        cnv_comm_Free(IoHandleData->pDataSend);
        cnv_comm_Free(IoHandleData);
    }
}

void handlethread_wakeup_allio(CNV_UNBLOCKING_QUEUE *queuerespond)
{
    int nRet = CNV_ERR_OK;
    int lAction = 0;
    uint64_t ulWakeup = 1;  //任意值,无实际意义
    K_BOOL bIsWakeup;

    if(queuerespond && get_unblock_queue_count(queuerespond))
    {
        for(int i = 0; i < g_params.tConfigIO.lNumberOfThread; i++)
        {
            bIsWakeup = K_FALSE;
            struct queue_entry_t *queuenode = get_unblock_queue_first(queuerespond);
            while(queuenode && queuenode->data_)
            {
                HANDLE_TO_IO_DATA *ptPostData = (HANDLE_TO_IO_DATA *)(queuenode->data_);
                HANDLE_TO_IO_DATA *ptHandleIoData = (HANDLE_TO_IO_DATA *)cnv_comm_Malloc(sizeof(HANDLE_TO_IO_DATA));
                bzero(ptHandleIoData, sizeof(HANDLE_TO_IO_DATA));
                ptHandleIoData->lAction = ptPostData->lAction;
                ptHandleIoData->lConnectID = ptPostData->lConnectID;
                snprintf(ptHandleIoData->strServIp, sizeof(ptHandleIoData->strServIp) - 1, "%s", ptPostData->strServIp);
                ptHandleIoData->ulPort = ptPostData->ulPort;
                lAction = ptPostData->lAction;
                if(ptPostData->lAction == REFRESH_CONNECT)
                {
                    ptHandleIoData->pDataSend = (char *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
                    CNV_UNBLOCKING_QUEUE *queServer = (CNV_UNBLOCKING_QUEUE *)ptHandleIoData->pDataSend;
                    initiate_unblock_queue(queServer, ptPostData->lDataLen);  //队列时ptHandleIoData->lDataLen为队列长度

                    CNV_UNBLOCKING_QUEUE *queServerIn = (CNV_UNBLOCKING_QUEUE *)ptPostData->pDataSend;
                    struct queue_entry_t *queuenode2 = get_unblock_queue_first(queServerIn);
                    while(queuenode2 && queuenode2->data_)
                    {
                        SERVER_SOCKET_DATA *ptSvrSockDataIn = (SERVER_SOCKET_DATA *)queuenode2->data_;
                        SERVER_SOCKET_DATA *ptSvrSockData = (SERVER_SOCKET_DATA *)cnv_comm_Malloc(sizeof(SERVER_SOCKET_DATA));
                        bzero(ptSvrSockData, sizeof(SERVER_SOCKET_DATA));
                        snprintf(ptSvrSockData->strServerIp, sizeof(ptSvrSockData->strServerIp) - 1, "%s", ptSvrSockDataIn->strServerIp);
                        ptSvrSockData->lPort = ptSvrSockDataIn->lPort;
                        ptSvrSockData->lHeartBeatLen = ptSvrSockDataIn->lHeartBeatLen;
                        ptSvrSockData->pHeartBeat = (K_CHAR *)cnv_comm_Malloc(ptSvrSockData->lHeartBeatLen);
                        memcpy(ptSvrSockData->pHeartBeat, ptSvrSockDataIn->pHeartBeat, ptSvrSockData->lHeartBeatLen);
                        snprintf(ptSvrSockData->strServiceName, sizeof(ptSvrSockData->strServiceName) - 1, ptSvrSockDataIn->strServiceName);
                        snprintf(ptSvrSockData->tCallback.strProtocol, sizeof(ptSvrSockData->tCallback.strProtocol) - 1, ptSvrSockDataIn->tCallback.strProtocol);
                        push_unblock_queue_tail(queServer, ptSvrSockData);
                        queuenode2 = get_unblock_queue_next(queuenode2);
                    }
                }
                else
                {
                    ptHandleIoData->lDataLen = ptPostData->lDataLen;
                    ptHandleIoData->pDataSend = (char *)cnv_comm_Malloc(ptHandleIoData->lDataLen);
                    memcpy(ptHandleIoData->pDataSend, ptPostData->pDataSend, ptHandleIoData->lDataLen);
                }

                nRet = push_block_queue_tail(g_params.tConfigIO.szConfigIOItem[i].pIoThreadContext->handle_io_msgque, ptHandleIoData, 1);  //队列满了把数据丢掉,以免内存泄露
                if(nRet == false)
                {
                    cnv_comm_Free(ptHandleIoData->pDataSend);
                    cnv_comm_Free(ptHandleIoData);
                }
                bIsWakeup = K_TRUE;
                queuenode = get_unblock_queue_next(queuenode);
            }

            if(bIsWakeup)
            {
                nRet = write(g_params.tConfigIO.szConfigIOItem[i].pIoThreadContext->handle_io_eventfd, &ulWakeup, sizeof(ulWakeup));  //handle唤醒io
                if(nRet != sizeof(ulWakeup))
                {
                    LOG_SYS_ERROR("handle wake io failed.");
                }
            }

            if(lAction == REFRESH_CONNECT)   //刷新长连接只要通知一个IO即可,因为所有IO线程公用一个服务器队列
            {
                break;
            }
        }
    }

    int nCount = get_unblock_queue_count(queuerespond);
    while(nCount--)
    {
        HANDLE_TO_IO_DATA *ptHandleIoData = (HANDLE_TO_IO_DATA *)poll_unblock_queue_head(queuerespond);
        if(ptHandleIoData->lAction == REFRESH_CONNECT)
        {
            CNV_UNBLOCKING_QUEUE *queueServer = (CNV_UNBLOCKING_QUEUE *)ptHandleIoData->pDataSend;
            int nCountSvr = get_unblock_queue_count(queueServer);
            while(nCountSvr--)
            {
                SERVER_SOCKET_DATA *ptSvrSockData = (SERVER_SOCKET_DATA *)poll_unblock_queue_head(queueServer);
                cnv_comm_Free(ptSvrSockData->pHeartBeat);
                cnv_comm_Free(ptSvrSockData);
            }
            destory_unblock_queue(queueServer);
        }
        cnv_comm_Free(ptHandleIoData->pDataSend);
        cnv_comm_Free(ptHandleIoData);
    }
}

//处理io消息
void handlethread_handle_iomsg(int  EventfdIo, HANDLE_THREAD_CONTEXT *pHandleContext)
{
    IO_TO_HANDLE_DATA *pIOHanldeData = (IO_TO_HANDLE_DATA *)lockfree_queue_dequeue(&pHandleContext->io_handle_msgque, 1);
    if(!pIOHanldeData)
    {
        return;
    }

    HANDLE_PARAMS *ptHandleParams = (pHandleContext->pHandleParam ? (HANDLE_PARAMS *)pHandleContext->pHandleParam : NULL);
    if(pIOHanldeData->pfncnv_handle_business)
    {
        if(ptHandleParams && ptHandleParams->pBusinessParams)
        {
            pIOHanldeData->pfncnv_handle_business(pIOHanldeData, &pHandleContext->queuerespond, ptHandleParams->pBusinessParams);  //执行回调函数
        }
        else
        {
            pIOHanldeData->pfncnv_handle_business(pIOHanldeData, &pHandleContext->queuerespond, NULL);  //执行回调函数
        }
    }

    int nRet = CNV_ERR_OK;
    uint64_t ulWakeup = 1;  //任意值,无实际意义
    K_BOOL bIsWakeIO = K_FALSE;
    int nNumOfPostMsg = get_unblock_queue_count(&pHandleContext->queuerespond);
    LOG_SYS_DEBUG("nNumOfPostMsg = %d", nNumOfPostMsg);
    while(nNumOfPostMsg--)    // handle线程单独用的队列,无需加锁
    {
        void *pPostData = poll_unblock_queue_head(&pHandleContext->queuerespond);
        nRet = push_block_queue_tail(pIOHanldeData->handle_io_msgque, pPostData, 1);  //队列满了把数据丢掉,以免内存泄露
        if(nRet == false)
        {
            HANDLE_TO_IO_DATA *pHandleIOData = (HANDLE_TO_IO_DATA *)pPostData;
            cnv_comm_Free(pHandleIOData->pDataSend);
            cnv_comm_Free(pHandleIOData);
            continue;
        }
        bIsWakeIO = true;
    }

    if(bIsWakeIO)
    {
        nRet = write(pIOHanldeData->handle_io_eventfd, &ulWakeup, sizeof(ulWakeup));  //handle唤醒io
        if(nRet != sizeof(ulWakeup))
        {
            LOG_SYS_ERROR("handle wake io failed.");
        }
        bIsWakeIO = K_FALSE;
    }

    cnv_comm_Free(pIOHanldeData->pDataSend);
    cnv_comm_Free(pIOHanldeData);

    if(lockfree_queue_len(&pHandleContext->io_handle_msgque) <= 0)
    {
        uint64_t ulData = 0;
        nRet = read(EventfdIo, &ulData, sizeof(uint64_t));   //此数据无实际意义,读出避免重复提醒
    }
}

int netframe_init_handle(HANDLE_THREAD_ITEM *pTheadparam)
{
    int nRet = CNV_ERR_OK;
    HANDLE_THREAD_CONTEXT *pHandleContext = pTheadparam->pHandleContext;

    //监听io写handle
    nRet = netframe_setblockopt(pHandleContext->io_handle_eventfd, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return  nRet;
    }

    nRet = netframe_add_readevent(pHandleContext->Epollfd, pHandleContext->io_handle_eventfd, NULL);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_add_readevent failed!");
        return  nRet;
    }

    if(pHandleContext->queParamFrames && get_unblock_queue_count(pHandleContext->queParamFrames))
    {
        cnv_hashmap_init(&pHandleContext->HashTimerTask, get_unblock_queue_count(pHandleContext->queParamFrames) + 1, cnv_hashmap_charhash, cnv_hashmap_charequals);

        struct queue_entry_t  *queuenode = get_unblock_queue_first(pHandleContext->queParamFrames);
        while(queuenode)     //有定时任务
        {
            HANDLE_TIMER_TASK *ptTimerTask = (HANDLE_TIMER_TASK *)queuenode->data_;

            //hashmap
            void *pOutValue = NULL;
            if(cnv_hashmap_get(pHandleContext->HashTimerTask, ptTimerTask->strTaskName, &pOutValue) == K_SUCCEED)     //重复的任务名
            {
                LOG_SYS_ERROR("dump duplicated taskname!");
                return -1;
            }

            char *pHashKey = (char *)cnv_comm_Malloc(64);
            if(!pHashKey)
            {
                LOG_SYS_ERROR("cnv_comm_Malloc failed!");
                return CNV_ERR_MALLOC;
            }
            bzero(pHashKey, 64);
            snprintf(pHashKey, 63, "%s", ptTimerTask->strTaskName);

            TIMER_TASK_STRUCT *ptTimerTaskStr = (TIMER_TASK_STRUCT *)cnv_comm_Malloc(sizeof(TIMER_TASK_STRUCT));;
            if(!ptTimerTaskStr)
            {
                LOG_SYS_ERROR("cnv_comm_Malloc failed!");
                cnv_comm_Free(pHashKey);
                return CNV_ERR_MALLOC;
            }
            ptTimerTaskStr->pfnCALLBACK_FUNCTION = ptTimerTask->pfn_timertask_cb;
            ptTimerTaskStr->timerfd = timerfd_create(CLOCK_REALTIME, 0);

            HASHMAP_VALUE *pHashValue = (HASHMAP_VALUE *)cnv_comm_Malloc(sizeof(HASHMAP_VALUE));
            if(!pHashValue)
            {
                LOG_SYS_ERROR("cnv_comm_Malloc failed!");
                cnv_comm_Free(pHashKey);
                cnv_comm_Free(ptTimerTaskStr);
                return CNV_ERR_MALLOC;
            }
            pHashValue->pKey = pHashKey;
            pHashValue->pValue = (char *)ptTimerTaskStr;

            cnv_hashmap_put(pHandleContext->HashTimerTask, pHashKey, pHashValue, NULL);

            nRet = netframe_setblockopt(ptTimerTaskStr->timerfd, K_FALSE);
            if(nRet != CNV_ERR_OK)
            {
                LOG_SYS_FATAL("netframe_setblockopt failed!");
                return  nRet;
            }

            nRet = netframe_add_readevent(pHandleContext->Epollfd, ptTimerTaskStr->timerfd, pHashKey);
            if(nRet != CNV_ERR_OK)
            {
                return  nRet;
            }

            struct itimerspec tTimerspec = { { 0 }, { 0 } };
            tTimerspec.it_value.tv_sec = ptTimerTask->tTimer.value_sec;
            tTimerspec.it_value.tv_nsec = ptTimerTask->tTimer.value_nsec;
            tTimerspec.it_interval.tv_sec = ptTimerTask->tTimer.interval_sec;
            tTimerspec.it_interval.tv_nsec = ptTimerTask->tTimer.interval_nsec;
            timerfd_settime(ptTimerTaskStr->timerfd, 0, &tTimerspec, NULL);

            queuenode = get_unblock_queue_next(queuenode);
        }
    }

    return CNV_ERR_OK;
}

int  handle_thread_run(void *pThreadParameter)
{
    int i = 0;
    uint64_t ulData = 0;
    int nCount = -1;   //epoll个数
    struct epoll_event szEpollEvent[DEFAULF_EPOLL_SIZE];
    HANDLE_THREAD_ITEM *pTheadparam = (HANDLE_THREAD_ITEM *)pThreadParameter;
    HANDLE_THREAD_CONTEXT *pHandleContext = pTheadparam->pHandleContext;
    HANDLE_PARAMS *pHandleParams = (HANDLE_PARAMS *)pHandleContext->pHandleParam;
    int Epollfd = pHandleContext->Epollfd;
    int  EventfdIo = pHandleContext->io_handle_eventfd;    //io唤醒
    bzero(szEpollEvent, sizeof(struct epoll_event)*DEFAULF_EPOLL_SIZE);

    int nRet = netframe_init_handle(pTheadparam);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_init_handle failed!");
        return nRet;
    }

    while(1)
    {
        nCount = epoll_wait(Epollfd, szEpollEvent, DEFAULF_EPOLL_SIZE, -1);
        if(nCount > 0)
        {
            for(i = 0; i < nCount; i++)
            {
                if((szEpollEvent[i].events & EPOLLHUP) && !(szEpollEvent[i].events & EPOLLIN))   //错误
                {
                    LOG_SYS_ERROR("epoll event POLLHUP! socketfd:%d", szEpollEvent[i].data.fd);
                }
                else if(szEpollEvent[i].events & POLLNVAL)
                {
                    LOG_SYS_ERROR("epoll event POLLNVAL! socketfd:%d", szEpollEvent[i].data.fd);
                }
                else if(szEpollEvent[i].events & (EPOLLERR | POLLNVAL))
                {
                    LOG_SYS_ERROR("epoll event POLLERR! conid:%s", (char *)szEpollEvent[i].data.ptr);
                }
                else if(szEpollEvent[i].events & (EPOLLIN | EPOLLPRI))     //读事件
                {
                    if(szEpollEvent[i].data.fd == EventfdIo)    //io唤醒
                    {
                        handlethread_handle_iomsg(EventfdIo, pHandleContext);
                    }
                    else   //定时事件
                    {
                        void *pOutValue = NULL;
                        if(cnv_hashmap_get(pHandleContext->HashTimerTask, szEpollEvent[i].data.ptr, &pOutValue) == K_SUCCEED)   //有定时服务
                        {
                            HASHMAP_VALUE *pHashValue = (HASHMAP_VALUE *)pOutValue;
                            TIMER_TASK_STRUCT *ptCbFunctionStr = (TIMER_TASK_STRUCT *)pHashValue->pValue;
                            nRet = read(ptCbFunctionStr->timerfd, &ulData, sizeof(uint64_t));   //此数据无实际意义,读出避免重复提醒
                            if(ptCbFunctionStr->pfnCALLBACK_FUNCTION)
                            {
                                ptCbFunctionStr->pfnCALLBACK_FUNCTION(&pHandleContext->queuerespond, pHandleParams->pBusinessParams);
                                if(get_unblock_queue_count(&pHandleContext->queuerespond) > 0)
                                {
                                    handlethread_wakeup_allio(&pHandleContext->queuerespond);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    return nRet;
}

void handle_thread_uninit(HANDLE_THREAD_CONTEXT *pHandleContexts)
{
    int  i;
    for(i = 0; i < g_params.tConfigHandle.lNumberOfThread; i++)
    {
        HANDLE_THREAD_CONTEXT  *pHandleContext = &pHandleContexts[i];
        close(pHandleContext->io_handle_eventfd);
        close(pHandleContext->Epollfd);
        cnv_hashmap_erase(pHandleContext->HashTimerTask, earase_hashtimer_callback, NULL);
        cnv_hashmap_uninit(pHandleContext->HashTimerTask);
        free_iohandle_lockfreequeue(&(pHandleContext->io_handle_msgque));
        lockfree_queue_uninit(&(pHandleContext->io_handle_msgque));
        free_handleio_unblockqueue(&(pHandleContext->queuerespond));  //  写给IO的队列
    }
}

int  handle_thread_init(HANDLE_THREAD_ITEM *pConfigHandleItem, HANDLE_THREAD_CONTEXT *pHandleContext)
{
    int nRet = CNV_ERR_OK;

    netframe_create_epoll(&(pHandleContext->Epollfd), 5);   //epoll
    pHandleContext->io_handle_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    lockfree_queue_init(&(pHandleContext->io_handle_msgque), g_params.tConfigHandle.lIoHandleMsgSize);
    initiate_unblock_queue(&(pHandleContext->queuerespond), DEFAULT_QUEUE_CAPCITY);   //业务返回的消息队列
    pConfigHandleItem->pHandleContext = pHandleContext;
    nRet = init_handle_params(&pHandleContext->pHandleParam);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("init_handle_params failed.");
    }
    HANDLE_PARAMS *pHandleParams = (HANDLE_PARAMS *)pHandleContext->pHandleParam;
    pHandleContext->queParamFrames = &pHandleParams->queParamFrameUse;

    return nRet;
}

//开启handle线程
int handle_thread_start(HANDLE_THREAD_CONTEXT *pHandleContexts)
{
    int  nRet = CNV_ERR_OK;
    int  i = 0;

    for(i = 0; i < g_params.tConfigHandle.lNumberOfThread; i++)
    {
        HANDLE_THREAD_CONTEXT *pHandleContext = &(pHandleContexts[i]);
        nRet = handle_thread_init(&(g_params.tConfigHandle.szConfigHandleItem[i]), pHandleContext);
        if(nRet != CNV_ERR_OK)
        {
            LOG_SYS_ERROR("handle_thread_init error!");
            return nRet;
        }
        pHandleContext->lthreadindex = i + 1;
        snprintf(pHandleContext->threadname, sizeof(pHandleContext->threadname) - 1, "%s", g_params.tConfigHandle.szConfigHandleItem[i].strThreadName);  //线程名
        nRet = hmi_plat_CreateThread((pfnCNV_PLAT_THREAD_RECALL)handle_thread_run, &(g_params.tConfigHandle.szConfigHandleItem[i]), 0, &g_params.tConfigHandle.szConfigHandleItem[i].ulThreadId, &g_params.tConfigHandle.szConfigHandleItem[i].ThreadHandle);
        LOG_SYS_INFO("handle thread start result:%d", nRet);
    }

    return nRet;
}
