/****************************
    FileName:netframe_io.cgethash client of server failed
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_io  C文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-19
    *****************************/

#include "netframe_io.h"
#include "common_type.h"
#include "cnv_comm.h"
#include "cnv_hashmap.h"
#include "cnv_thread.h"
#include "netframe_net.h"
#include "cnv_net_define.h"
#include "log/cnv_liblog4cplus.h"
#include "cnv_lock_free_queue.h"
#include "cnv_blocking_queue.h"
#include "cnv_unblock_queue.h"
#include "alg_md5.h"
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <time.h>
#include <errno.h>
#include <poll.h>
#include <unistd.h>
#include <assert.h>
#include <arpa/inet.h>

int iothread_handle_respond(int Epollfd, int Eventfd, CNV_BLOCKING_QUEUE *handle_io_msgque, void *HashAddrFd, void *HashConnidFd, IO_THREAD_CONTEXT *pIoThreadContext);

int io_send_monitor(IO_THREAD_CONTEXT *pIoThreadContext)
{
    HANDLE_TO_IO_DATA *ptHandleIoData = NULL;

    int nRet = pIoThreadContext->pfncnv_monitor_callback(&pIoThreadContext->tMonitorElement, &ptHandleIoData);
    if(nRet != 0)
    {
        return nRet;
    }

    nRet = push_block_queue_tail(pIoThreadContext->handle_io_msgque, ptHandleIoData, 1);  //队列满了把数据丢掉,以免内存泄露
    if(nRet == false)
    {
        cnv_comm_Free(ptHandleIoData->pDataSend);
        cnv_comm_Free(ptHandleIoData);
        return -1;
    }

    iothread_handle_respond(pIoThreadContext->Epollfd, pIoThreadContext->handle_io_eventfd, pIoThreadContext->handle_io_msgque, pIoThreadContext->HashAddrFd, pIoThreadContext->HashAddrFd, pIoThreadContext);
    return 0;
}

void  monitor_iothread(IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_ACC_DEBUG("io %d, RecvLengthPerSecond=%ld", pIoThreadContext->threadindex, pIoThreadContext->tMonitorElement.lRecvLength / g_params.tMonitor.interval_sec);
    LOG_ACC_DEBUG("io %d, RcvPackNumPerSecond=%ld", pIoThreadContext->threadindex, pIoThreadContext->tMonitorElement.lRecvPackNum / g_params.tMonitor.interval_sec);
    LOG_ACC_DEBUG("io %d, ParsePackNumPerSecond=%ld", pIoThreadContext->threadindex, pIoThreadContext->tMonitorElement.lParsePackNum / g_params.tMonitor.interval_sec);
    LOG_ACC_DEBUG("io %d, RepTimesPerSecond=%d", pIoThreadContext->threadindex, pIoThreadContext->tMonitorElement.nRespondTimes / g_params.tMonitor.interval_sec);
    LOG_ACC_DEBUG("io %d, SvrPackNumPerSecond=%ld", pIoThreadContext->threadindex, pIoThreadContext->tMonitorElement.lSvrPackNum / g_params.tMonitor.interval_sec);
    LOG_ACC_DEBUG("io %d, SvrFailedNumPerSecond=%ld", pIoThreadContext->threadindex, pIoThreadContext->tMonitorElement.lSvrFailedNum / g_params.tMonitor.interval_sec);
    LOG_ACC_DEBUG("io %d, SeedOfKey=%d", pIoThreadContext->threadindex, pIoThreadContext->SeedOfKey);
    if(pIoThreadContext->queServer)
    {
        LOG_ACC_DEBUG("io %d, queServer.count=%d", pIoThreadContext->threadindex, get_unblock_queue_count(pIoThreadContext->queServer));
    }
    else
    {
        LOG_ACC_DEBUG("io %d, queServer.count=%d", pIoThreadContext->threadindex, 0);
    }
    LOG_ACC_DEBUG("io %d, HashConnidFd.size=%d", pIoThreadContext->threadindex, cnv_hashmap_size(pIoThreadContext->HashConnidFd));
    LOG_ACC_DEBUG("io %d, HashAddrFd.size=%d", pIoThreadContext->threadindex, cnv_hashmap_size(pIoThreadContext->HashAddrFd));
    LOG_ACC_DEBUG("io %d, handle_io_msgque.size=%d", pIoThreadContext->threadindex, get_block_queue_count(pIoThreadContext->handle_io_msgque));
    for(int i = 0; i < g_params.tConfigHandle.lNumberOfThread; i++)
    {
        if(pIoThreadContext->szHandleContext[i + 1])
        {
            LOG_ACC_DEBUG("handle %d, io_handle_msgque.size=%d", i + 1, lockfree_queue_len(&(pIoThreadContext->szHandleContext[i + 1]->io_handle_msgque)));

            pIoThreadContext->tMonitorElement.szHanldeMsgQueCount[pIoThreadContext->tMonitorElement.nHandleThreadCount] = lockfree_queue_len(&(pIoThreadContext->szHandleContext[i + 1]->io_handle_msgque));
            pIoThreadContext->tMonitorElement.nHandleThreadCount++;
        }
    }

    if(pIoThreadContext->pfncnv_monitor_callback)
    {
        memcpy(pIoThreadContext->tMonitorElement.strStartTime, pIoThreadContext->strStartTime, sizeof(pIoThreadContext->tMonitorElement.strStartTime) - 1);
        pIoThreadContext->tMonitorElement.nThreadIndex = pIoThreadContext->threadindex;
        pIoThreadContext->tMonitorElement.nClientConNum = cnv_hashmap_size(pIoThreadContext->HashConnidFd);
        pIoThreadContext->tMonitorElement.nSvrConnNum = cnv_hashmap_size(pIoThreadContext->HashAddrFd);
        pIoThreadContext->tMonitorElement.nIoMsgQueCount = get_block_queue_count(pIoThreadContext->handle_io_msgque);
        io_send_monitor(pIoThreadContext);
    }

    bzero(&pIoThreadContext->tMonitorElement, sizeof(pIoThreadContext->tMonitorElement));
    LOG_ACC_DEBUG("");
}

void free_acceptio_fifo(cnv_fifo *accept_io_msgque)
{
    while(cnv_fifo_len(accept_io_msgque) > 0)
    {
        ACCEPT_TO_IO_DATA AcceptIOData = { 0 };
        int nRet = cnv_fifo_get(accept_io_msgque, (unsigned char *)&AcceptIOData, sizeof(ACCEPT_TO_IO_DATA));
        if(nRet == 0)     //可能为空消息
        {
            continue;
        }

        netframe_close_socket(AcceptIOData.fd);
    }
}

void free_handleio_blockqueue(CNV_BLOCKING_QUEUE *block_queue)
{
    CNV_UNBLOCKING_QUEUE *unblockqueue = block_queue->unblockqueue;
    HANDLE_TO_IO_DATA  *HandleIoData = NULL;
    int lCount = get_unblock_queue_count(unblockqueue);
    while(lCount--)
    {
        HandleIoData = (HANDLE_TO_IO_DATA *)poll_unblock_queue_head(unblockqueue);
        cnv_comm_Free(HandleIoData->pDataSend);
        cnv_comm_Free(HandleIoData);
    }
    cnv_comm_Free(unblockqueue);
    desorty_block_queue(block_queue);
}

void free_server_unblock_queue(CNV_UNBLOCKING_QUEUE *queServer)
{
    SERVER_SOCKET_DATA *ptSvrSockData = NULL;
    int nCount = get_unblock_queue_count(queServer);
    while(nCount--)
    {
        ptSvrSockData = (SERVER_SOCKET_DATA *)poll_unblock_queue_head(queServer);
        cnv_comm_Free(ptSvrSockData->pHeartBeat);
        cnv_comm_Free(ptSvrSockData);
    }
}

int iothread_handle_write(int Epollfd, void *pConnId, void *HashConnidFd, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("iothread_handle_write.");
    void *pOutValue = NULL;
    int nRet = cnv_hashmap_get(HashConnidFd, pConnId, &pOutValue);  // 用connid获取socket相关结构体
    if(nRet != K_SUCCEED)
    {
        LOG_SYS_ERROR("hashmap can not get value!");
        return nRet;
    }
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);
    CLIENT_SOCKET_ELEMENT *ptClnSockElement = &pSocketElement->uSockElement.tClnSockElement;

    int nLenAlreadyWrite = 0;
    nRet = netframe_write(pSocketElement->Socket, ptClnSockElement->pWriteRemain, ptClnSockElement->lWriteRemain, &nLenAlreadyWrite);
    if(nRet == CNV_ERR_OK)  //写完,删除临时缓存,并修改为读事件
    {
        ptClnSockElement->lWriteRemain = 0;
        cnv_comm_Free(ptClnSockElement->pWriteRemain);
        ptClnSockElement->pWriteRemain = NULL;
        netframe_modify_writeevent(Epollfd, pSocketElement->Socket, pConnId);
    }
    else
    {
        if(nRet == AGENT_NET_WRITE_INCOMPLETED)   //没写完,继续写
        {
            memmove(ptClnSockElement->pWriteRemain, ptClnSockElement->pWriteRemain + nLenAlreadyWrite, ptClnSockElement->lWriteRemain - nLenAlreadyWrite);
            ptClnSockElement->lWriteRemain -= nLenAlreadyWrite;
        }
        else   //放弃写,修改为读事件
        {
            ptClnSockElement->lWriteRemain = 0;
            cnv_comm_Free(ptClnSockElement->pWriteRemain);
            ptClnSockElement->pWriteRemain = NULL;
            netframe_modify_writeevent(Epollfd, pSocketElement->Socket, pConnId);
        }
    }

    LOG_SYS_DEBUG("iothread_handle_write end.");
    return  nRet;
}

//获取连接句柄
int  iothread_get_hashsocket(HANDLE_TO_IO_DATA *pHandIOData, void *HashAddrFd, void  *HashConnidFd, char **pOutValue)
{
    int nRet = CNV_ERR_OK;
    char strKey[32] = "";

    if(pHandIOData->lAction == RESPOND_CLIENT || pHandIOData->lAction == CLOSE_CLIENT)  //应答客户端
    {
        snprintf(strKey, sizeof(strKey), "%d", pHandIOData->lConnectID);
        LOG_SYS_DEBUG("selected: key:%s", strKey);
        nRet = cnv_hashmap_get(HashConnidFd, strKey, (void **)pOutValue);
        if(nRet != K_SUCCEED)
        {
            LOG_SYS_ERROR("cnv_hashmap_get failded! HashConnidFd.size=%d, key=%s.", cnv_hashmap_size(HashConnidFd), strKey);
            cnv_hashmap_iterator(HashConnidFd, printhashmap, NULL);
            return nRet;
        }
    }
    else if(pHandIOData->lAction == REQUEST_SERVICE)   //向服务端发送请求
    {
        snprintf(strKey, sizeof(strKey) - 1, "%s", pHandIOData->strServIp);
        cnv_comm_StrcatA(strKey, ":");
        char  strPort[10] = { 0 };
        snprintf(strPort, sizeof(strPort) - 1, "%d", pHandIOData->ulPort);
        cnv_comm_StrcatA(strKey, strPort);
        LOG_SYS_DEBUG("selected: ip:port : %s", strKey);
        nRet = cnv_hashmap_get(HashAddrFd, strKey, (void **)pOutValue);
        if(nRet != K_SUCCEED)
        {
            LOG_SYS_ERROR("cnv_hashmap_get failded! HashAddrFd.size=%d, key=%s.", cnv_hashmap_size(HashAddrFd), strKey);
            cnv_hashmap_iterator(HashAddrFd, printhashmap, NULL);
            return nRet;
        }
    }
    else if(pHandIOData->lAction == NOTICE_CLIENT)  //服务端下发客户端
    {
        snprintf(strKey, sizeof(strKey) - 1, "%s", pHandIOData->strServIp);
        LOG_SYS_DEBUG("selected: ip:port : %s", strKey);
        nRet = cnv_hashmap_get(HashConnidFd, strKey, (void **)pOutValue);
        if(nRet != K_SUCCEED)
        {
            LOG_SYS_ERROR("cnv_hashmap_get failded! HashConnidFd.size=%d, key=%s.", cnv_hashmap_size(HashConnidFd), strKey);
            cnv_hashmap_iterator(HashConnidFd, printhashmap, NULL);
            return nRet;
        }
    }
    else
    {
        LOG_SYS_FATAL("action error!");
        return CNV_ERR_PARAM;
    }

    return CNV_ERR_OK;
}

int on_write_client_failed(HANDLE_TO_IO_DATA *pHandleIOData, IO_THREAD_CONTEXT  *pIoThreadContext)
{
    LOG_SYS_DEBUG("on_write_client_failed begin.");
    int nRet = CNV_ERR_OK;
    uint64_t ulWakeup = 1;  //任意值,无实际意义
    K_BOOL bIsWakeIO = K_FALSE;
    CNV_UNBLOCKING_QUEUE queRespMsg;
    initiate_unblock_queue(&queRespMsg, 100);

    if(pHandleIOData->pfnsend_failed_callback != NULL)
    {
        pHandleIOData->pfnsend_failed_callback(&queRespMsg, pHandleIOData);
    }
    else
    {
        return -1;
    }

    int nNumOfRespMsg = get_unblock_queue_count(&queRespMsg);
    LOG_SYS_DEBUG("nNumOfRespMsg = %d", nNumOfRespMsg);
    while(nNumOfRespMsg--)      // handle线程单独用的队列,无需加锁
    {
        void *pRespData = poll_unblock_queue_head(&queRespMsg);
        nRet = push_block_queue_tail(pIoThreadContext->handle_io_msgque, pRespData, 1);  //队列满了把数据丢掉,以免内存泄露
        if(nRet == false)
        {
            HANDLE_TO_IO_DATA *pHandleIOData = (HANDLE_TO_IO_DATA *)pRespData;
            cnv_comm_Free(pHandleIOData->pDataSend);
            cnv_comm_Free(pRespData);
            continue;
        }
        bIsWakeIO = true;
    }

    if(bIsWakeIO)
    {
        nRet = write(pIoThreadContext->handle_io_eventfd, &ulWakeup, sizeof(ulWakeup));  //唤醒io
        if(nRet != sizeof(ulWakeup))
        {
            LOG_SYS_ERROR("handle wake io failed.");
        }
        bIsWakeIO = K_FALSE;
    }

    LOG_SYS_DEBUG("on_write_server_failed end.");
    return CNV_ERR_OK;
}

int save_client_remain_data(int Epollfd, SOCKET_ELEMENT *pSocketElement, HANDLE_TO_IO_DATA *ptHandleIOData, int nLenAlreadyWrite)
{
    CLIENT_SOCKET_ELEMENT *ptClnSockElement = &pSocketElement->uSockElement.tClnSockElement;

    if(nLenAlreadyWrite >= ptClnSockElement->lWriteRemain)  //只剩下新的数据
    {
        int nTmpRemain = ptClnSockElement->lWriteRemain;
        ptClnSockElement->lWriteRemain = ptClnSockElement->lWriteRemain + ptHandleIOData->lDataLen - nLenAlreadyWrite;
        if(ptClnSockElement->pWriteRemain != NULL)
        {
            cnv_comm_Free(ptClnSockElement->pWriteRemain);
            ptClnSockElement->pWriteRemain = NULL;
        }
        ptClnSockElement->pWriteRemain = (char *)malloc(ptClnSockElement->lWriteRemain);
        assert(ptClnSockElement->pWriteRemain);
        memcpy(ptClnSockElement->pWriteRemain, ptHandleIOData->pDataSend + (nLenAlreadyWrite - nTmpRemain), ptClnSockElement->lWriteRemain);
    }
    else  //之前剩下的数据没写完
    {
        int nReRemain = ptClnSockElement->lWriteRemain - nLenAlreadyWrite;
        char *pReRemain = (char *)malloc(nReRemain);
        assert(pReRemain);
        memcpy(pReRemain, ptClnSockElement->pWriteRemain + nLenAlreadyWrite, nReRemain);

        ptClnSockElement->lWriteRemain = ptClnSockElement->lWriteRemain + ptHandleIOData->lDataLen - nLenAlreadyWrite;
        if(ptClnSockElement->pWriteRemain != NULL)
        {
            cnv_comm_Free(ptClnSockElement->pWriteRemain);
            ptClnSockElement->pWriteRemain = NULL;
        }
        memcpy(ptClnSockElement->pWriteRemain, pReRemain, nReRemain);
        memcpy(ptClnSockElement->pWriteRemain + nReRemain, ptHandleIOData->pDataSend, ptHandleIOData->lDataLen);
        cnv_comm_Free(pReRemain);
    }

    return CNV_ERR_OK;
}

int sendmsg_to_client(SOCKET_ELEMENT *pSocketElement, HANDLE_TO_IO_DATA *pHandleIOData, int *pnLenAlreadyWrite)
{
    if(pSocketElement->uSockElement.tClnSockElement.lWriteRemain == 0)
    {
        struct iovec tIovecWriteData = { 0 };
        tIovecWriteData.iov_base = pHandleIOData->pDataSend;
        tIovecWriteData.iov_len = pHandleIOData->lDataLen;
        pSocketElement->uSockElement.tClnSockElement.msg.msg_iov = &tIovecWriteData;
        pSocketElement->uSockElement.tClnSockElement.msg.msg_iovlen = 1;
    }
    else if(pSocketElement->uSockElement.tClnSockElement.lWriteRemain > 0)
    {
        struct iovec szIovecWriteData[2];
        bzero(szIovecWriteData, sizeof(struct iovec) * 2);
        szIovecWriteData[0].iov_base = pSocketElement->uSockElement.tClnSockElement.pWriteRemain;
        szIovecWriteData[0].iov_len = pSocketElement->uSockElement.tClnSockElement.lWriteRemain;
        szIovecWriteData[1].iov_base = pHandleIOData->pDataSend;
        szIovecWriteData[1].iov_len = pHandleIOData->lDataLen;
        pSocketElement->uSockElement.tClnSockElement.msg.msg_iov = szIovecWriteData;
        pSocketElement->uSockElement.tClnSockElement.msg.msg_iovlen = 2;
    }

    int nDataLen = pSocketElement->uSockElement.tClnSockElement.lWriteRemain + pHandleIOData->lDataLen;
    LOG_SYS_DEBUG("size to write:%d", nDataLen);
    return netframe_sendmsg(pSocketElement->Socket, &pSocketElement->uSockElement.tClnSockElement.msg, nDataLen, pnLenAlreadyWrite);
}

int respond_write_client(int Epollfd, char *pOutValue, HANDLE_TO_IO_DATA *pHandleIOData, void *HashConnidFd, IO_THREAD_CONTEXT  *pIoThreadContext)
{
    LOG_SYS_DEBUG("respond_write_client.");
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);
    pSocketElement->uSockElement.tClnSockElement.nReserveOne = pHandleIOData->nReserveOne;
    pSocketElement->uSockElement.tClnSockElement.nReserverTwo = pHandleIOData->nReserverTwo;
    int nLenAlreadyWrite = 0;

    int nRet = sendmsg_to_client(pSocketElement, pHandleIOData, &nLenAlreadyWrite);
    if(nRet == CNV_ERR_OK)
    {
        pSocketElement->uSockElement.tClnSockElement.lWriteRemain = 0;
        pSocketElement->Time = cnv_comm_get_utctime();
        LOG_SYS_DEBUG("write %d bytes successfully", pSocketElement->uSockElement.tSvrSockElement.lWriteRemain + pHandleIOData->lDataLen);
    }
    else
    {
        if(nRet == AGENT_NET_WRITE_BUSY || nRet == AGENT_NET_WRITE_INCOMPLETED)   //保存未写完数据,修改epoll事件
        {
            LOG_SYS_ERROR("write imcompleted!");
            pSocketElement->Time = cnv_comm_get_utctime();
            return save_client_remain_data(Epollfd, pSocketElement, pHandleIOData, nLenAlreadyWrite);
        }
        else if(nRet == AGENT_NET_CONNECTION_RESET || nRet == AGENT_NET_NOT_CONNECTED)   //连接异常,从hashmp删掉并找下一台服务器
        {
            LOG_SYS_ERROR("connection abnormal!");

            if(on_write_client_failed(pHandleIOData, pIoThreadContext) != CNV_ERR_OK)
            {
                remove_client_socket_hashmap(Epollfd, HashConnidFd, pSocketElement->pConnId);
            }
        }
        else   //发送错误
        {
            LOG_SYS_ERROR("write data failed!");
        }
    }

    LOG_SYS_DEBUG("respond_write_client end.");
    return  nRet;
}

void refresh_long_connect(IO_THREAD_CONTEXT *pIoThreadContext, CNV_UNBLOCKING_QUEUE *queServer)
{
    LOG_SYS_DEBUG("refresh_long_connect begin.");

    if(queServer && get_unblock_queue_count(queServer) > 0)
    {
        int nNewServerCount = get_unblock_queue_count(queServer);
        CNV_UNBLOCKING_QUEUE queServerTmp;
        initiate_unblock_queue(&queServerTmp, nNewServerCount);
        K_BOOL bIsServerExist;

        while(nNewServerCount--)
        {
            bIsServerExist = K_FALSE;
            SERVER_SOCKET_DATA *ptNewSvrSockData = (SERVER_SOCKET_DATA *)poll_unblock_queue_head(queServer);

            int nOriServerCount = get_unblock_queue_count(pIoThreadContext->queServer);
            while(nOriServerCount--)
            {
                SERVER_SOCKET_DATA *ptOriSvrSockData = (SERVER_SOCKET_DATA *)poll_unblock_queue_head(pIoThreadContext->queServer);
                push_unblock_queue_tail(pIoThreadContext->queServer, ptOriSvrSockData);

                if(!strcmp(ptNewSvrSockData->strServerIp, ptOriSvrSockData->strServerIp) && ptNewSvrSockData->lPort == ptOriSvrSockData->lPort)
                {
                    bIsServerExist = K_TRUE;
                    break;
                }
            }

            if(!bIsServerExist)
            {
                push_unblock_queue_tail(&queServerTmp, ptNewSvrSockData);  //需要建立连接
                push_unblock_queue_tail(pIoThreadContext->queServer, ptNewSvrSockData);  //全部服务器信息
            }
            else   //已经存在,释放内存
            {
                cnv_comm_Free(ptNewSvrSockData->pHeartBeat);
                cnv_comm_Free(ptNewSvrSockData);
            }
        }

        if(get_unblock_queue_count(&queServerTmp) > 0)
        {
            netframe_long_connect(pIoThreadContext, &queServerTmp);
        }
    }

    LOG_SYS_DEBUG("refresh_long_connect end.");
}

void on_write_server_failed(HANDLE_TO_IO_DATA *pHandleIOData, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("on_write_server_failed begin.");
    int nRet = CNV_ERR_OK;
    //uint64_t ulWakeup = 1;  //任意值,无实际意义
    K_BOOL bIsWakeIO = K_FALSE;
    CNV_UNBLOCKING_QUEUE queRespMsg;
    initiate_unblock_queue(&queRespMsg, 100);
    pIoThreadContext->tMonitorElement.lSvrFailedNum++;

    if(pHandleIOData->pfnsend_failed_callback)
    {
        pHandleIOData->pfnsend_failed_callback(&queRespMsg, pHandleIOData);

        int nNumOfRespMsg = get_unblock_queue_count(&queRespMsg);
        LOG_SYS_DEBUG("nNumOfRespMsg = %d", nNumOfRespMsg);
        while(nNumOfRespMsg--)      // handle线程单独用的队列,无需加锁
        {
            void *pRespData = poll_unblock_queue_head(&queRespMsg);
            nRet = push_block_queue_tail(pIoThreadContext->handle_io_msgque, pRespData, 1);  //队列满了把数据丢掉,以免内存泄露
            if(nRet == false)
            {
                cnv_comm_Free(((HANDLE_TO_IO_DATA *)pRespData)->pDataSend);
                cnv_comm_Free(pRespData);
                continue;
            }
            bIsWakeIO = true;
        }

        if(bIsWakeIO)
        {
            iothread_handle_respond(pIoThreadContext->Epollfd, pIoThreadContext->handle_io_eventfd, pIoThreadContext->handle_io_msgque, pIoThreadContext->HashAddrFd, pIoThreadContext->HashAddrFd, pIoThreadContext);
        }
    }

    LOG_SYS_DEBUG("on_write_server_failed end.");
}

int write_server_remain_data(IO_THREAD_CONTEXT *pIoThreadContext, SOCKET_ELEMENT *pSocketElement, HANDLE_TO_IO_DATA *pHandleIOData, int nLenAlreadyWrite)
{
    int nWriteAlready = 0;

    int nRet = netframe_write(pSocketElement->Socket, pHandleIOData->pDataSend + nLenAlreadyWrite, pHandleIOData->lDataLen - nLenAlreadyWrite, &nWriteAlready);
    if(nRet == CNV_ERR_OK)
    {
        pSocketElement->uSockElement.tSvrSockElement.lWriteRemain = 0;
        pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
        LOG_SYS_DEBUG("write %d bytes successfully", pHandleIOData->lDataLen - nLenAlreadyWrite);
    }
    else
    {
        if(nRet == AGENT_NET_WRITE_INCOMPLETED)   //保存未写完数据,修改epoll事件
        {
            pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
            return write_server_remain_data(pIoThreadContext, pSocketElement, pHandleIOData, nLenAlreadyWrite + nWriteAlready);
        }
        else  //发送错误,因为是上一个遗留的数据,直接丢弃,避免造成乱包
        {
            LOG_SYS_ERROR("write remain data failed!");
            on_write_server_failed(pHandleIOData, pIoThreadContext);
            return -1;
        }
    }

    return CNV_ERR_OK;
}

int respond_write_next_server(SERVER_SOCKET_DATA *pSvrSockData, HANDLE_TO_IO_DATA *pHandleIOData, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("respond_write_next_server begin.");
    int nRet = CNV_ERR_OK;
    int nLenAlreadyWrite = 0;
    NAVI_SVC_HASHMAP *map = (NAVI_SVC_HASHMAP *)(pIoThreadContext->HashAddrFd);

    if(map != K_NULL)
    {
        for(int i = 0; i < map->bucketCount; i++)
        {
            NAVI_SVC_HASHMAP_ENTRY *entry = map->buckets[i];
            while(entry != K_NULL)
            {
                SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)(entry->value))->pValue);
                if(strcmp(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strServiceName, pSvrSockData->strServiceName) == 0
                        && (strcmp(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strServerIp, pSvrSockData->strServerIp) != 0
                            || pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->lPort != pSvrSockData->lPort))  //同一类服务器
                {
                    nRet = netframe_write(pSocketElement->Socket, pHandleIOData->pDataSend, pHandleIOData->lDataLen, &nLenAlreadyWrite);
                    if(nRet == CNV_ERR_OK)
                    {
                        pSocketElement->uSockElement.tSvrSockElement.lWriteRemain = 0;
                        pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
                        LOG_SYS_INFO("write next server success");
                        return CNV_ERR_OK;
                    }
                    else
                    {
                        if(nRet == AGENT_NET_WRITE_INCOMPLETED)     //保存未写完数据,修改epoll事件
                        {
                            LOG_SYS_ERROR("write imcompleted!");
                            pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
                            return write_server_remain_data(pIoThreadContext, pSocketElement, pHandleIOData, nLenAlreadyWrite);
                        }
                        else if(nRet == AGENT_NET_CONNECTION_ABNORMAL)       //连接异常
                        {
                            LOG_SYS_ERROR("connection to %s:%d is abnormal!", pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strServerIp, pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->lPort);
                        }
                    }
                }
                entry = entry->next;
            }
        }
    }

    LOG_SYS_ERROR("respond_write_next_server failed!");
    LOG_SYS_DEBUG("respond_write_next_server end.");
    return -1;
}

int respond_write_server_again(SERVER_SOCKET_DATA *pSvrSockData, HANDLE_TO_IO_DATA *pHandleIOData, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("respond_write_server_again begin.");

    int nRet = netframe_reconnect_server(pSvrSockData, pIoThreadContext);
    if(nRet != CNV_ERR_OK)
    {
        return nRet;
    }

    void *pOutValue = NULL;
    char strHashKey[64] = { 0 };
    snprintf(strHashKey, sizeof(strHashKey) - 1, "%s", pSvrSockData->strServerIp);
    cnv_comm_StrcatA(strHashKey, ":");
    char strPort[10] = { 0 };
    snprintf(strPort, sizeof(strPort) - 1, "%d", pSvrSockData->lPort);
    cnv_comm_StrcatA(strHashKey, strPort);
    nRet = cnv_hashmap_get(pIoThreadContext->HashAddrFd, strHashKey, &pOutValue);
    if(nRet != K_SUCCEED)
    {
        LOG_SYS_ERROR("cnv_hashmap_get failded! HashAddrFd.size=%d, key=%s.", cnv_hashmap_size(pIoThreadContext->HashAddrFd), strHashKey);
        return nRet;
    }
    SOCKET_ELEMENT *ptSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue); //原来的SOCKET_ELEMENT已删除

    int nLenAlreadyWrite = 0;
    nRet = netframe_write(ptSocketElement->Socket, pHandleIOData->pDataSend, pHandleIOData->lDataLen, &nLenAlreadyWrite);
    if(nRet == CNV_ERR_OK)
    {
        ptSocketElement->uSockElement.tSvrSockElement.lWriteRemain = 0;
        ptSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
        LOG_SYS_DEBUG("write %d bytes successfully", ptSocketElement->uSockElement.tSvrSockElement.lWriteRemain + pHandleIOData->lDataLen);
    }
    else
    {
        if(nRet == AGENT_NET_WRITE_INCOMPLETED)   //保存未写完数据,修改epoll事件
        {
            LOG_SYS_ERROR("write imcompleted!");
            ptSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
            return write_server_remain_data(pIoThreadContext, ptSocketElement, pHandleIOData, nLenAlreadyWrite);
        }
        else if(nRet == AGENT_NET_CONNECTION_ABNORMAL)     //连接异常
        {
            LOG_SYS_ERROR("connect is abnormal, find next server to send!");
            return -1;
        }
    }

    LOG_SYS_DEBUG("respond_write_server_again end.");
    return CNV_ERR_OK;
}

int respond_write_server(int Epollfd, char *pOutValue, HANDLE_TO_IO_DATA *pHandleIOData, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("respond_write_server begin.");
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);
    int nLenAlreadyWrite = 0;

    int nRet = netframe_write(pSocketElement->Socket, pHandleIOData->pDataSend, pHandleIOData->lDataLen, &nLenAlreadyWrite);
    if(nRet == CNV_ERR_OK)
    {
        pSocketElement->uSockElement.tSvrSockElement.lWriteRemain = 0;
        pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
        LOG_SYS_DEBUG("write %d bytes successfully", pSocketElement->uSockElement.tSvrSockElement.lWriteRemain + pHandleIOData->lDataLen);
    }
    else
    {
        if(nRet == AGENT_NET_WRITE_INCOMPLETED)  //保存未写完数据,修改epoll事件
        {
            pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
            return write_server_remain_data(pIoThreadContext, pSocketElement, pHandleIOData, nLenAlreadyWrite);
        }
        else if(nRet == AGENT_NET_CONNECTION_ABNORMAL)    //连接异常
        {
            SERVER_SOCKET_DATA *pSvrSockData = (SERVER_SOCKET_DATA *)(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData);
            LOG_SYS_ERROR("connect is abnormal! ip:%s, port:%d", pSvrSockData->strServerIp, pSvrSockData->lPort);

            nRet = respond_write_server_again(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData, pHandleIOData, pIoThreadContext);  //重连,再写一次
            if(nRet != CNV_ERR_OK)
            {
                nRet = respond_write_next_server(pSvrSockData, pHandleIOData, pIoThreadContext);  //本连接失败,发送到同类服务器
                if(nRet != CNV_ERR_OK)
                {
                    on_write_server_failed(pHandleIOData, pIoThreadContext);  //发送失败
                }
            }
        }
        else  //发送错误
        {
            LOG_SYS_ERROR("write data failed!");
        }
    }

    LOG_SYS_DEBUG("respond_write_server end.");
    return nRet;
}

SERVER_SOCKET_DATA *get_svrsockdata_from_queue(HANDLE_TO_IO_DATA *ptHandleIOData, CNV_UNBLOCKING_QUEUE *queServer)
{
    struct queue_entry_t *queuenode = get_unblock_queue_first(queServer);
    while(queuenode)
    {
        SERVER_SOCKET_DATA *ptSvrSockData = (SERVER_SOCKET_DATA *)queuenode->data_;
        if(!strcmp(ptSvrSockData->strServerIp, ptHandleIOData->strServIp) && ptSvrSockData->lPort == ptHandleIOData->ulPort)
        {
            return ptSvrSockData;
        }

        queuenode = get_unblock_queue_next(queuenode);
    }

    return NULL;
}

// handle返回处理
int iothread_handle_respond(int Epollfd, int Eventfd, CNV_BLOCKING_QUEUE *handle_io_msgque, void *HashAddrFd, void *HashConnidFd, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("iothread_handle_respond.");
    int  nRet = CNV_ERR_OK;
    uint64_t  ulData = 0;
    char  *pOutValue = NULL;
    CNV_UNBLOCKING_QUEUE *unblockqueue = NULL;

    lock_block_queue(handle_io_msgque);

    if(handle_io_msgque->unblockqueue == pIoThreadContext->handle_msgque_one)
    {
        unblockqueue = pIoThreadContext->handle_msgque_one;
        handle_io_msgque->unblockqueue = pIoThreadContext->handle_msgque_two;
    }
    else
    {
        unblockqueue = pIoThreadContext->handle_msgque_two;
        handle_io_msgque->unblockqueue = pIoThreadContext->handle_msgque_one;
    }

    unlock_block_queue(handle_io_msgque);

    int lNumOfRepMsg = get_unblock_queue_count(unblockqueue);
    while(lNumOfRepMsg--)
    {
        HANDLE_TO_IO_DATA *pHandleIOData = (HANDLE_TO_IO_DATA *)poll_unblock_queue_head(unblockqueue);
        if(!pHandleIOData)    // 可能为空消息
        {
            continue;
        }

        if(pHandleIOData->lAction == REQUEST_SERVICE)     //服务请求
        {
            nRet = iothread_get_hashsocket(pHandleIOData, HashAddrFd, HashConnidFd, &pOutValue);
            if(nRet == CNV_ERR_OK)
            {
                nRet = respond_write_server(Epollfd, pOutValue, pHandleIOData, pIoThreadContext);
                if(nRet == CNV_ERR_OK)
                {
                    pIoThreadContext->tMonitorElement.lSvrPackNum++;
                }
            }
            else
            {
                LOG_SYS_ERROR("write_service, iothread_get_hashsocket failed !");
                SERVER_SOCKET_DATA *ptSvrSockData = get_svrsockdata_from_queue(pHandleIOData, pIoThreadContext->queServer);
                if(ptSvrSockData)
                {
                    nRet = respond_write_server_again(ptSvrSockData, pHandleIOData, pIoThreadContext);  //重连,再写一次
                    if(nRet == CNV_ERR_OK)
                    {
                        pIoThreadContext->tMonitorElement.lSvrPackNum++;
                    }
                    else
                    {
                        nRet = respond_write_next_server(ptSvrSockData, pHandleIOData, pIoThreadContext);  //本连接失败,发送到同类服务器
                        if(nRet == CNV_ERR_OK)
                        {
                            pIoThreadContext->tMonitorElement.lSvrPackNum++;
                        }
                        else
                        {
                            pIoThreadContext->tMonitorElement.lSvrFailedNum++;
                            on_write_server_failed(pHandleIOData, pIoThreadContext);  //发送失败
                        }
                    }
                }
                else
                {
                    LOG_SYS_ERROR("no responding server found!");
                }
            }
        }
        else if(pHandleIOData->lAction == RESPOND_CLIENT)    //客户端应答
        {
            nRet = iothread_get_hashsocket(pHandleIOData, HashAddrFd, HashConnidFd, &pOutValue);
            if(nRet != CNV_ERR_OK)
            {
                LOG_SYS_ERROR("write_client, iothread_get_hashsocket failed !");
                cnv_comm_Free(pHandleIOData->pDataSend);
                cnv_comm_Free(pHandleIOData);
                continue;
            }

            nRet = respond_write_client(Epollfd, pOutValue, pHandleIOData, HashConnidFd, pIoThreadContext);
            if(nRet != CNV_ERR_OK)
            {
                LOG_SYS_ERROR("respond_write_client error!");
            }
        }
        else if(pHandleIOData->lAction == NOTICE_CLIENT)    //服务端下发客户端
        {
            nRet = iothread_get_hashsocket(pHandleIOData, HashAddrFd, HashConnidFd, &pOutValue);
            if(nRet == CNV_ERR_OK)
            {
                nRet = respond_write_client(Epollfd, pOutValue, pHandleIOData, HashConnidFd, pIoThreadContext);
                if(nRet != CNV_ERR_OK)
                {
                    LOG_SYS_ERROR("respond_write_client error!");
                }
            }
            else
            {
                on_write_client_failed(pHandleIOData, pIoThreadContext);
            }
        }
        else if(pHandleIOData->lAction == REFRESH_CONNECT)     //刷新长连接
        {
            CNV_UNBLOCKING_QUEUE *queServer = (CNV_UNBLOCKING_QUEUE *)(pHandleIOData->pDataSend);
            refresh_long_connect(pIoThreadContext, queServer);
            destory_unblock_queue(queServer);
        }
        else if(pHandleIOData->lAction == CLOSE_CLIENT)      //关闭客户端
        {
            nRet = iothread_get_hashsocket(pHandleIOData, HashAddrFd, HashConnidFd, &pOutValue);
            if(nRet != CNV_ERR_OK)
            {
                LOG_SYS_ERROR("write_client, iothread_get_hashsocket failed !");
                cnv_comm_Free(pHandleIOData->pDataSend);
                cnv_comm_Free(pHandleIOData);
                continue;
            }

            HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)pOutValue;
            SOCKET_ELEMENT  *pSocketElement = (SOCKET_ELEMENT *)pHashValue->pValue;
            remove_client_socket_hashmap(pIoThreadContext->Epollfd, pIoThreadContext->HashConnidFd, pSocketElement->pConnId);
        }

        if(pHandleIOData->pfn_handle_callback)
        {
            pHandleIOData->pfn_handle_callback(NULL, pHandleIOData->pCallbackPara);
        }

        cnv_comm_Free(pHandleIOData->pDataSend);
        cnv_comm_Free(pHandleIOData);
    }
    pIoThreadContext->tMonitorElement.nRespondTimes++;

    if(get_block_queue_count(handle_io_msgque) <= 0)
    {
        nRet = read(Eventfd, &ulData, sizeof(uint64_t));   //此数无用,读出缓存消息,避免epoll重复提醒
        if(nRet != sizeof(uint64_t))
        {
            LOG_SYS_FATAL("handle respond read eventfd failed !");
        }
    }
    LOG_SYS_DEBUG("iothread_handle_respond end.");
    return  CNV_ERR_OK;
}

//选择handle线程
int  io_select_handle_thread(IO_THREAD_CONTEXT *pIoThreadContext, HANDLE_THREAD_CONTEXT **pHandleContexts, CNV_UNBLOCKING_QUEUE  *queuedistribute, HANDLE_THREAD_CONTEXT **pHandleContext)
{
    int  lThreadIndex = 0;

    char *pThreadIndex = (char *)poll_unblock_queue_head(queuedistribute);
    push_unblock_queue_tail(queuedistribute, pThreadIndex);    //此处取出后重新插入,达到分配效果
    lThreadIndex = atoi(pThreadIndex);
    LOG_SYS_DEBUG("io thread %d select handle thread %d", pIoThreadContext->threadindex, lThreadIndex);
    *pHandleContext = pHandleContexts[lThreadIndex];
    if(!*pHandleContext)
    {
        return  CNV_ERR_SELECT_THREAD;
    }

    return  CNV_ERR_OK;
}

// 接收accept消息
int iothread_recv_accept(int Epollfd, int Eventfd, cnv_fifo *accept_io_msgque, void *HashConnidFd, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("iothread_recv_accept.");
    int nRet = CNV_ERR_OK;
    uint64_t ulData = 0;
    void *pOldValue = NULL;

    while(cnv_fifo_len(accept_io_msgque) > 0)
    {
        ACCEPT_TO_IO_DATA AcceptIOData = {0};
        nRet = cnv_fifo_get(accept_io_msgque, (unsigned char *)&AcceptIOData, sizeof(ACCEPT_TO_IO_DATA));
        if(nRet == 0)    //可能为空消息
        {
            continue;
        }

        SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)cnv_comm_Malloc(sizeof(SOCKET_ELEMENT));
        if(!pSocketElement)
        {
            continue;
        }
        memset(pSocketElement, 0x00, sizeof(SOCKET_ELEMENT));
        pSocketElement->Socket = AcceptIOData.fd;
        pSocketElement->Time = cnv_comm_get_utctime();
        if(strcmp(AcceptIOData.strTransmission, "UDP") != 0)  //udp协议不清理客户端
        {
            pSocketElement->bIsToclear = true;
        }
        else
        {
            pSocketElement->bIsToclear = false;
        }
        snprintf(pSocketElement->uSockElement.tClnSockElement.strProtocol, DEFAULT_ARRAY_SIZE - 1, "%s", AcceptIOData.strProtocol);
        snprintf(pSocketElement->uSockElement.tClnSockElement.strTransmission, DEFAULT_ARRAY_SIZE - 1, "%s", AcceptIOData.strTransmission);
        pSocketElement->lAction = 1;
        pSocketElement->uSockElement.tClnSockElement.msg.msg_name = &(pSocketElement->uSockElement.tClnSockElement.tClientAddr);
        pSocketElement->uSockElement.tClnSockElement.msg.msg_namelen = sizeof(struct sockaddr_in);
        pSocketElement->uSockElement.tClnSockElement.msg.msg_iov = &(pSocketElement->uSockElement.tClnSockElement.tIovecClnData);
        pSocketElement->uSockElement.tClnSockElement.msg.msg_iovlen = 1;
        pSocketElement->uSockElement.tClnSockElement.msg.msg_control = pSocketElement->uSockElement.tClnSockElement.strControl;
        pSocketElement->uSockElement.tClnSockElement.msg.msg_controllen = sizeof(pSocketElement->uSockElement.tClnSockElement.strControl);
        pSocketElement->uSockElement.tClnSockElement.pfncnv_parse_protocol = AcceptIOData.pfncnv_parse_protocol;
        pSocketElement->uSockElement.tClnSockElement.pfncnv_handle_business = AcceptIOData.pfncnv_handle_business;

        char *pKey = (char *)cnv_comm_Malloc(33);
        if(!pKey)
        {
            cnv_comm_Free(pSocketElement);
            continue;
        }
        bzero(pKey, 33);

        if(AcceptIOData.uMapType == 0)   //用连接ID做映射
        {
            int ConnId = netframe_get_hashkey(HashConnidFd, &(pIoThreadContext->SeedOfKey));
            snprintf(pKey, 32, "%d", ConnId);
        }
        else   //用客户端地址做映射
        {
            snprintf(pKey, 32, "%s", AcceptIOData.strClientIp);
        }
        pSocketElement->pConnId = pKey;
        LOG_SYS_DEBUG("pKey:%s", pKey);

        if(cnv_hashmap_containsKey(HashConnidFd, pKey) == K_TRUE)
        {
            LOG_SYS_ERROR("key:%s already exists.", pKey);
            cnv_comm_Free(pSocketElement);
            cnv_comm_Free(pKey);
            continue;
        }

        HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)cnv_comm_Malloc(sizeof(HASHMAP_VALUE));
        if(!pHashValue)
        {
            cnv_comm_Free(pSocketElement);
            cnv_comm_Free(pKey);
            continue;
        }
        pHashValue->lSize = sizeof(HASHMAP_VALUE);
        pHashValue->pValue = (char *)pSocketElement;

        nRet = cnv_hashmap_put(HashConnidFd, pKey, pHashValue, &pOldValue);
        if(nRet != K_SUCCEED)
        {
            LOG_SYS_DEBUG("cnv_hashmap_put failed!");
            cnv_comm_Free(pSocketElement);
            cnv_comm_Free(pKey);
            cnv_comm_Free(pHashValue);
            continue;
        }

        nRet = netframe_add_readevent(Epollfd, AcceptIOData.fd, pKey);  //把客户端连接句柄加入读监听
        if(nRet != CNV_ERR_OK)
        {
            LOG_SYS_ERROR("netframe_add_readevent failed!");
            remove_client_socket_hashmap(Epollfd, HashConnidFd, pKey);
        }
    }

    if(cnv_fifo_len(accept_io_msgque) <= 0)
    {
        nRet = read(Eventfd, &ulData, sizeof(uint64_t));   //此数无用,读出缓存消息,避免epoll重复提醒
        if(nRet != sizeof(uint64_t))
        {
            LOG_SYS_FATAL("read Eventfd error !");
        }
    }
    LOG_SYS_DEBUG("iothread_recv_accept end.");
    return  CNV_ERR_OK;
}

// 接收数据
int iothread_handle_read(int Epollfd, void *pConnId, void *HashConnidFd, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("iothread_handle_read.");
    int nDataReadLen = 0;
    int nDistributeValue = 0;   //分发类型为哈希时,需要使用的哈希值
    unsigned int nPacketSize = 0;
    void  *pOutValue = NULL;
    void *pAuxiliary = NULL;
    char *pPacket = NULL; //使用时置零
    HANDLE_THREAD_CONTEXT *pHandleContext = NULL;

    int nRet = cnv_hashmap_get(HashConnidFd, pConnId, &pOutValue);  //用connid获取socket相关结构体
    if(nRet != K_SUCCEED)
    {
        LOG_SYS_DEBUG("threadid:%d,hashmap can not get value! HashConnidFd.size=%d", pIoThreadContext->threadindex, cnv_hashmap_size(HashConnidFd));
        cnv_hashmap_iterator(HashConnidFd, printhashmap, NULL);
        return CNV_ERR_HASHMAP_GET;
    }
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);
    struct msghdr *pmsg = &(pSocketElement->uSockElement.tClnSockElement.msg);
    CLIENT_SOCKET_DATA *ptClnSockData = &(pSocketElement->uSockElement.tClnSockElement.SocketData);
    if(!ptClnSockData->pDataBuffer)   //接收数据缓存
    {
        ptClnSockData->pDataBuffer = cnv_comm_Malloc(g_params.nMaxBufferSize);
        if(!ptClnSockData->pDataBuffer)
        {
            return  CNV_ERR_MALLOC;
        }
    }
    pmsg->msg_iov->iov_base = ptClnSockData->pDataBuffer + ptClnSockData->lDataRemain;       //读缓存
    pmsg->msg_iov->iov_len = g_params.nMaxBufferSize - ptClnSockData->lDataRemain;      //空闲缓存长度
    bzero(pmsg->msg_iov->iov_base, pmsg->msg_iov->iov_len);

    nRet = netframe_recvmsg(pSocketElement->Socket, pmsg, &nDataReadLen);  //接收数据
    if(nRet != CNV_ERR_OK)
    {
        if(nRet == AGENT_NET_CLIENT_CLOSED)   //客户端关闭
        {
            LOG_SYS_DEBUG("threadid:%d,client closed.", pIoThreadContext->threadindex);
            remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
        }
        else if(nRet == AGENT_NET_READ_BUSY)  //系统繁忙
        {
            LOG_SYS_DEBUG("threadid:%d, read nothing.", pIoThreadContext->threadindex);
        }
        else if(nRet == AGENT_NET_ERR_READ)    //读取错误
        {
            LOG_SYS_FATAL("threadid:%d, read failed!", pIoThreadContext->threadindex);
            remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
        }
        return nRet;
    }

    char strClientIp[32] = { 0 };
    struct cmsghdr *cmptr = NULL;
    for(cmptr = CMSG_FIRSTHDR(pmsg); cmptr; cmptr = CMSG_NXTHDR(pmsg, cmptr))
    {
        if((cmptr->cmsg_level == IPPROTO_IP) && (cmptr->cmsg_type == IP_PKTINFO))
        {
            LOG_SYS_DEBUG("%s,%d\n", inet_ntoa(((struct in_pktinfo *)CMSG_DATA(cmptr))->ipi_spec_dst), ((struct in_pktinfo *)CMSG_DATA(cmptr))->ipi_ifindex);
        }
        memcpy(strClientIp, inet_ntoa(((struct in_pktinfo *)CMSG_DATA(cmptr))->ipi_spec_dst), sizeof(strClientIp) - 1);
    }

    pIoThreadContext->tMonitorElement.lRecvLength += nDataReadLen;
    pIoThreadContext->tMonitorElement.lRecvPackNum++;
    pSocketElement->Time = cnv_comm_get_utctime();  //收、发数据后重置时间戳
    LOG_SYS_DEBUG("lDataRemain:%d, read data length:%d", ptClnSockData->lDataRemain, nDataReadLen);

    ptClnSockData->pMovePointer = ptClnSockData->pDataBuffer;
    ptClnSockData->lDataRemain += nDataReadLen;
    pfnCNV_PARSE_PROTOCOL pfncnvparseprotocol = pSocketElement->uSockElement.tClnSockElement.pfncnv_parse_protocol;  //协议解析回调函数
    if(!pfncnvparseprotocol)
    {
        LOG_SYS_INFO("pfncnvparseprotocol is empty.");
        ptClnSockData->lDataRemain = 0;
        return CNV_ERR_OK;
    }

    while(ptClnSockData->lDataRemain > 0)
    {
        ptClnSockData->pMovePointer += nPacketSize; //数据缓存指针偏移
        nPacketSize = 0;
        pPacket = NULL;
        nRet = pfncnvparseprotocol(&(ptClnSockData->pMovePointer), &(ptClnSockData->lDataRemain), &pPacket, &nPacketSize, &pAuxiliary);  //协议解析
        if(nRet != CNV_PARSE_SUCCESS)
        {
            if(nRet == CNV_PARSE_FINISH && ptClnSockData->lDataRemain > 0)  //结束解析而且有剩余数据
            {
                memcpy(ptClnSockData->pDataBuffer, ptClnSockData->pMovePointer, ptClnSockData->lDataRemain);
            }
            else if(nRet == CNV_PARSE_SHUTDOWN)    //关闭客户端
            {
                remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
            }
            else if(nRet == CNV_PARSE_MOVE)     //数据偏移
            {
                ptClnSockData->lDataRemain -= nPacketSize;      //总数据长度减去一个包的数据大小
                if(ptClnSockData->lDataRemain == 0 && pAuxiliary)     //解析完了才把pAuxiliary内存释放
                {
                    cnv_comm_Free(pAuxiliary);
                    pAuxiliary = NULL;
                }
                continue;
            }
            else if(nRet == CNV_PARSE_ERROR)    //解析错误,数据清空
            {
                memset(ptClnSockData->pDataBuffer, 0, g_params.nMaxBufferSize);
                ptClnSockData->lDataRemain = 0;
            }

            if(pAuxiliary)
            {
                cnv_comm_Free(pAuxiliary);
                pAuxiliary = NULL;
            }
            break;
        }

        if(pIoThreadContext->nDistributeType == DISTRIBUTE_TYPE_HASH)
        {
            if(pAuxiliary)
            {
                nDistributeValue = *(int *)pAuxiliary;
            }
            else
            {
                nDistributeValue = atol((char *)&nDataReadLen) % pIoThreadContext->nHandleThreadCount;   //取随机数
            }
        }

        ptClnSockData->lDataRemain -= nPacketSize;      //总数据长度减去一个包的数据大小
        if(ptClnSockData->lDataRemain == 0 && pAuxiliary)    //解析完了才把pAuxiliary内存释放
        {
            cnv_comm_Free(pAuxiliary);
            pAuxiliary = NULL;
        }
        pIoThreadContext->tMonitorElement.lParsePackNum++;

        IO_TO_HANDLE_DATA *pIOHanldeData = (IO_TO_HANDLE_DATA *)cnv_comm_Malloc(sizeof(IO_TO_HANDLE_DATA));    //io->handle  header
        if(!pIOHanldeData)
        {
            cnv_comm_Free(pPacket);
            return CNV_ERR_MALLOC;
        }
        pIOHanldeData->lConnectID = atoi((char *)pConnId);
        memcpy(pIOHanldeData->strServIp, strClientIp, sizeof(pIOHanldeData->strServIp) - 1);
        pIOHanldeData->ulPort = pSocketElement->uSockElement.tClnSockElement.tSvrSockData.lPort;
        pIOHanldeData->lDataLen = nPacketSize;
        pIOHanldeData->handle_io_eventfd = pIoThreadContext->handle_io_eventfd;
        pIOHanldeData->handle_io_msgque = pIoThreadContext->handle_io_msgque;
        pIOHanldeData->io_thread_index = pIoThreadContext->threadindex;
        pIOHanldeData->pDataSend = pPacket;
        pIOHanldeData->pfncnv_handle_business = pSocketElement->uSockElement.tClnSockElement.pfncnv_handle_business;
        pIOHanldeData->nReserveOne = pSocketElement->uSockElement.tClnSockElement.nReserveOne;
        pIOHanldeData->nReserverTwo = pSocketElement->uSockElement.tClnSockElement.nReserverTwo;

        if(pIoThreadContext->nDistributeType == 0 || pIoThreadContext->nDistributeType == DISTRIBUTE_TYPE_WEIGHT)
        {
            io_select_handle_thread(pIoThreadContext, pIoThreadContext->szHandleContext, pIoThreadContext->queDistribute, &pHandleContext);
        }
        else if(pIoThreadContext->nDistributeType == DISTRIBUTE_TYPE_HASH)
        {
            pHandleContext = pIoThreadContext->szHandleContext[pIoThreadContext->szIoRespHandle[nDistributeValue % g_params.tConfigHandle.lNumberOfThread]];
        }

        nRet = lockfree_queue_enqueue(&(pHandleContext->io_handle_msgque), pIOHanldeData, 1);   //队列满了把数据丢掉,以免内存泄露
        if(nRet == false)
        {
            LOG_SYS_ERROR("queue is full!");
            cnv_comm_Free(pPacket);
            cnv_comm_Free(pIOHanldeData);
            pIoThreadContext->tMonitorElement.lSvrFailedNum++;
            continue;
        }

        uint64_t ulWakeup = 1;   //任意值,无实际意义
        nRet = write(pHandleContext->io_handle_eventfd, &ulWakeup, sizeof(ulWakeup));  //io唤醒handle
        if(nRet != sizeof(ulWakeup))
        {
            LOG_SYS_FATAL("io wake up handle failed !");
        }
    }
    LOG_SYS_DEBUG("iothread_handle_read end.");
    return  CNV_ERR_OK;
}

int  io_set_handle_contexts(IO_THREAD_ITEM   *pConfigIOItem, HANDLE_THREAD_CONTEXT *pHandleContexts, IO_THREAD_CONTEXT *pIoThreadContext)
{
    int nThreadIndex = 0;
    int nRealHandleThread = 0;  //handle线程变量的排序
    char strDistriTrans[32] = { 0 };
    cnv_comm_string_trans(pConfigIOItem->strDistribution, sizeof(strDistriTrans), ',', strDistriTrans);

    char  *pDistribution = strtok(strDistriTrans, ",");
    while(pDistribution)
    {
        nThreadIndex = atoi(pDistribution);
        if(nThreadIndex > g_params.tConfigHandle.lNumberOfThread)
        {
            LOG_SYS_FATAL("please check the confile file!");
            return  CNV_ERR_PARAM;
        }
        pIoThreadContext->szHandleContext[nThreadIndex] = &(pHandleContexts[nThreadIndex - 1]);
        pIoThreadContext->szIoRespHandle[nRealHandleThread++] = nThreadIndex;
        pDistribution = strtok(NULL, ",");
    }
    pIoThreadContext->nHandleThreadCount = nRealHandleThread;

    return  CNV_ERR_OK;
}

// 单个线程内部初始化
int netframe_init_io(IO_THREAD_ITEM   *pTheadparam)
{
    int  nRet = CNV_ERR_OK;
    IO_THREAD_CONTEXT *pIoThreadContext = pTheadparam->pIoThreadContext;
    CNV_UNBLOCKING_QUEUE *queDistribute = pIoThreadContext->queDistribute;

    //启动时间
    time_t rawtime;
    time(&rawtime);
    struct tm *ptm = gmtime(&rawtime);
    snprintf(pIoThreadContext->strStartTime, sizeof(pIoThreadContext->strStartTime) - 1, "%d-%d-%d %d:%d:%d", ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour + 8, ptm->tm_min, ptm->tm_sec);

    //负载解析
    cnv_parse_distribution(pTheadparam->strAlgorithm, pTheadparam->strDistribution, queDistribute);
    LOG_SYS_DEBUG("io thread : %s, distribution: %s", pTheadparam->strThreadName, pTheadparam->strDistribution);
    iterator_unblock_queuqe(queDistribute, printDistribution, (void *)0);

    //建立长连接
    nRet = netframe_long_connect(pIoThreadContext, pIoThreadContext->queServer);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_long_connect failed.");
        return nRet;
    }

    //监听accept写io
    nRet = netframe_setblockopt(pIoThreadContext->accept_io_eventfd, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return  nRet;
    }

    nRet = netframe_add_readevent(pIoThreadContext->Epollfd, pIoThreadContext->accept_io_eventfd, NULL);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_add_readevent failed!");
        return  nRet;
    }

    //监听handle写io
    nRet = netframe_setblockopt(pIoThreadContext->handle_io_eventfd, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return  nRet;
    }

    nRet = netframe_add_readevent(pIoThreadContext->Epollfd, pIoThreadContext->handle_io_eventfd, NULL);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_add_readevent failed!");
        return  nRet;
    }

    //心跳间隔
    nRet = netframe_setblockopt(pIoThreadContext->timerfd_hearbeat, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return  nRet;
    }

    nRet = netframe_init_timer(pIoThreadContext->Epollfd, pIoThreadContext->timerfd_hearbeat, &(g_params.tHeartBeat));
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_init_timer failed!");
        return  nRet;
    }

    //清理socket
    nRet = netframe_setblockopt(pIoThreadContext->timerfd_socketclear, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return  nRet;
    }

    nRet = netframe_init_timer(pIoThreadContext->Epollfd, pIoThreadContext->timerfd_socketclear, &(g_params.tSocketClear));
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_init_timer failed!");
        return  nRet;
    }

    //监控服务
    ioset_monitor_callback(&pIoThreadContext->pfncnv_monitor_callback);  //回调函数

    nRet = netframe_setblockopt(pIoThreadContext->timerfd_monitor, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return  nRet;
    }

    nRet = netframe_init_timer(pIoThreadContext->Epollfd, pIoThreadContext->timerfd_monitor, &(g_params.tMonitor));
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_init_timer failed!");
        return  nRet;
    }

    return  CNV_ERR_OK;
}

// 线程运行
int  io_thread_run(void *pThreadParameter)
{
    int nCount = -1;
    uint64_t ulData = 0;
    struct epoll_event szEpollEvent[DEFAULF_EPOLL_SIZE];
    bzero(szEpollEvent, sizeof(szEpollEvent));
    IO_THREAD_ITEM *pTheadparam = (IO_THREAD_ITEM *)pThreadParameter;
    IO_THREAD_CONTEXT *pIoThreadContext = pTheadparam->pIoThreadContext;
    pIoThreadContext->EpollEvent = szEpollEvent;
    int Epollfd = pIoThreadContext->Epollfd;
    int EventfdAccept = pIoThreadContext->accept_io_eventfd;  //ACCEPT唤醒
    int EventfdHandle = pIoThreadContext->handle_io_eventfd;   //HANDLE唤醒
    int timerfd_hearbeat = pIoThreadContext->timerfd_hearbeat;  //心跳
    int timerfd_clearsocket = pIoThreadContext->timerfd_socketclear;  //清理socket
    int timerfd_monitor = pIoThreadContext->timerfd_monitor;      //服务监视
    cnv_fifo *accept_io_msgque = pIoThreadContext->accept_io_msgque;   //accept -> io
    CNV_BLOCKING_QUEUE *handle_io_msgque = pIoThreadContext->handle_io_msgque;   //handle -> io
    void *HashConnidFd = pIoThreadContext->HashConnidFd;   //hashmap  key:connect id  value: socket fd
    void *HashAddrFd = pIoThreadContext->HashAddrFd;      //hashmap  key : ip_port   value :socket  fd

    int nRet = netframe_init_io(pTheadparam);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_init_io failed!");
        return nRet;
    }

    while(1)
    {
        nCount = epoll_wait(Epollfd, szEpollEvent, DEFAULF_EPOLL_SIZE, -1);
        if(nCount > 0)
        {
            for(int i = 0; i < nCount; i++)
            {
                if(szEpollEvent[i].events & (EPOLLIN | EPOLLPRI))   //读事件
                {
                    if(EventfdAccept == szEpollEvent[i].data.fd)   //accept唤醒
                    {
                        iothread_recv_accept(Epollfd, szEpollEvent[i].data.fd, accept_io_msgque, HashConnidFd, pIoThreadContext);
                    }
                    else if(EventfdHandle == szEpollEvent[i].data.fd)   //handle唤醒
                    {
                        iothread_handle_respond(Epollfd, szEpollEvent[i].data.fd, handle_io_msgque, HashAddrFd, HashConnidFd, pIoThreadContext);
                    }
                    else if(timerfd_monitor == szEpollEvent[i].data.fd)     //进程监控
                    {
                        read(timerfd_monitor, &ulData, sizeof(uint64_t));
                        monitor_iothread(pIoThreadContext);
                    }
                    else if(timerfd_hearbeat == szEpollEvent[i].data.fd)   //心跳
                    {
                        netframe_heart_beat(szEpollEvent[i].data.fd, pIoThreadContext);
                    }
                    else if(timerfd_clearsocket == szEpollEvent[i].data.fd)   //socket清理
                    {
                        netframe_socket_clear(Epollfd, szEpollEvent[i].data.fd, HashConnidFd);
                    }
                    else     //客户端消息
                    {
                        nRet = iothread_handle_read(Epollfd, szEpollEvent[i].data.ptr, HashConnidFd, pIoThreadContext);
                        if(nRet != CNV_ERR_OK)
                        {
                            if(nRet == CNV_ERR_HASHMAP_GET)
                            {
                                netframe_delete_event(Epollfd, szEpollEvent[i].data.fd);
                                netframe_close_socket(szEpollEvent[i].data.fd);
                            }
                        }
                    }
                }
                else if(szEpollEvent[i].events & EPOLLRDHUP)   //对端关闭
                {
                    LOG_SYS_DEBUG("peer shutdown.");
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
                else if((szEpollEvent[i].events & EPOLLHUP) && !(szEpollEvent[i].events & EPOLLIN))  //错误
                {
                    LOG_SYS_ERROR("%s", strerror(errno));
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
                else if(szEpollEvent[i].events & POLLNVAL)
                {
                    LOG_SYS_ERROR("%s", strerror(errno));
                }
                else if(szEpollEvent[i].events & (EPOLLERR | POLLNVAL))
                {
                    LOG_SYS_ERROR("%s", strerror(errno));
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
                else if(szEpollEvent[i].events & EPOLLOUT)  //写事件
                {
                    iothread_handle_write(Epollfd, szEpollEvent[i].data.ptr, HashConnidFd, pIoThreadContext);
                }
                else
                {
                    LOG_SYS_ERROR("unrecognized error, %s", strerror(errno));
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
            }
        }
        else if(nCount < 0)   //错误
        {
            LOG_SYS_ERROR("%s", strerror(errno));
        }
    }

    return nRet;
}

//功能:io线程反初始化
void  io_thread_uninit(IO_THREAD_CONTEXT *pIoThreadContexts)
{
    free_server_unblock_queue(pIoThreadContexts[0].queServer);  //服务的配置队列,框架公用,释放一次即可
    cnv_comm_Free(pIoThreadContexts[0].queServer);
    for(int i = 0; i < g_params.tConfigIO.lNumberOfThread; i++)
    {
        IO_THREAD_CONTEXT  *pIoThreadContext = &pIoThreadContexts[i];
        int  Epollfd = pIoThreadContext->Epollfd;
        free_acceptio_fifo(pIoThreadContext->accept_io_msgque);
        cnv_fifo_free(pIoThreadContext->accept_io_msgque);
        free_handleio_unblockqueue(pIoThreadContext->handle_msgque_one);
        cnv_comm_Free(pIoThreadContext->handle_msgque_one);
        free_handleio_unblockqueue(pIoThreadContext->handle_msgque_two);
        cnv_comm_Free(pIoThreadContext->handle_msgque_two);
        free_handleio_blockqueue(pIoThreadContext->handle_io_msgque);
        cnv_comm_Free(pIoThreadContext->handle_io_msgque);
        free_unblock_queue(pIoThreadContext->queDistribute);
        cnv_comm_Free(pIoThreadContext->queDistribute);
        cnv_hashmap_erase(pIoThreadContext->HashConnidFd, earase_client_socket_hashmap, &Epollfd);
        cnv_hashmap_uninit(pIoThreadContext->HashConnidFd);
        cnv_hashmap_erase(pIoThreadContext->HashAddrFd, earase_server_socket_hashmap, &Epollfd);
        cnv_hashmap_uninit(pIoThreadContext->HashAddrFd);
        close(pIoThreadContext->accept_io_eventfd);
        close(pIoThreadContext->handle_io_eventfd);
        close(pIoThreadContext->timerfd_hearbeat);
        close(pIoThreadContext->timerfd_socketclear);
        close(pIoThreadContext->timerfd_monitor);
        close(pIoThreadContext->Epollfd);
    }
}

//功能:io线程初始化
int  io_thread_init(IO_THREAD_ITEM   *pConfigIOItem, HANDLE_THREAD_CONTEXT *pHandleContexts, IO_THREAD_CONTEXT *pIoThreadContext)
{
    int  nRet = CNV_ERR_OK;
    nRet = io_set_handle_contexts(pConfigIOItem, pHandleContexts, pIoThreadContext);
    if(nRet != CNV_ERR_OK)
    {
        return nRet;
    }
    netframe_create_epoll(&(pIoThreadContext->Epollfd), 5);   //epoll
    pIoThreadContext->accept_io_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    pIoThreadContext->handle_io_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    pIoThreadContext->timerfd_hearbeat = timerfd_create(CLOCK_REALTIME, 0);
    pIoThreadContext->timerfd_socketclear = timerfd_create(CLOCK_REALTIME, 0);
    pIoThreadContext->timerfd_monitor = timerfd_create(CLOCK_REALTIME, 0);

    pIoThreadContext->accept_io_msgque = cnv_fifo_alloc(DEFAULT_FIFO_CAPCITY);
    if(!pIoThreadContext->accept_io_msgque)
    {
        return CNV_ERR_MALLOC;
    }

    pIoThreadContext->handle_msgque_one = (CNV_UNBLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
    if(!pIoThreadContext->handle_msgque_one)
    {
        return  CNV_ERR_MALLOC;
    }
    initiate_unblock_queue(pIoThreadContext->handle_msgque_one, g_params.tConfigIO.lHandleIoMsgSize);     //handle one

    pIoThreadContext->handle_msgque_two = (CNV_UNBLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
    if(!pIoThreadContext->handle_msgque_two)
    {
        return  CNV_ERR_MALLOC;
    }
    initiate_unblock_queue(pIoThreadContext->handle_msgque_two, g_params.tConfigIO.lHandleIoMsgSize);     //handle two

    pIoThreadContext->handle_io_msgque = (CNV_BLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_BLOCKING_QUEUE));
    if(!pIoThreadContext->handle_io_msgque)
    {
        return CNV_ERR_MALLOC;
    }
    initiate_block_queue(pIoThreadContext->handle_io_msgque, g_params.tConfigIO.lHandleIoMsgSize, pIoThreadContext->handle_msgque_one);   // handle队列

    pIoThreadContext->queDistribute = (CNV_UNBLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
    if(!pIoThreadContext->queDistribute)
    {
        return CNV_ERR_MALLOC;
    }
    initiate_unblock_queue(pIoThreadContext->queDistribute, 30);      //负载队列

    nRet = cnv_hashmap_init(&(pIoThreadContext->HashConnidFd), DEFAULT_HASHMAP_CAPCITY, cnv_hashmap_charhash, cnv_hashmap_charequals);
    if(nRet != K_SUCCEED)
    {
        return CNV_ERR_HASHMAP_INIT;
    }

    nRet = cnv_hashmap_init(&(pIoThreadContext->HashAddrFd), DEFAULT_HASHMAP_CAPCITY, cnv_hashmap_charhash, cnv_hashmap_charequals);
    if(nRet != K_SUCCEED)
    {
        return CNV_ERR_HASHMAP_INIT;
    }
    pConfigIOItem->pIoThreadContext = pIoThreadContext;

    return nRet;
}

//开启io线程
int io_thread_start(HANDLE_THREAD_CONTEXT *pHandleContexts, IO_THREAD_CONTEXT *pIoThreadContexts, CNV_UNBLOCKING_QUEUE *queServer)
{
    int  nRet = CNV_ERR_OK;
    int  i;

    for(i = 0; i < g_params.tConfigIO.lNumberOfThread; i++)
    {
        IO_THREAD_CONTEXT *pIoThreadContext = &(pIoThreadContexts[i]);
        nRet = io_thread_init(&(g_params.tConfigIO.szConfigIOItem[i]), pHandleContexts, pIoThreadContext);
        if(nRet != CNV_ERR_OK)
        {
            return nRet;
        }
        pIoThreadContext->threadindex = i + 1;
        snprintf(pIoThreadContext->threadname, sizeof(pIoThreadContext->threadname) - 1, "%s", g_params.tConfigIO.szConfigIOItem[i].strThreadName);  //线程名
        pIoThreadContext->queServer = queServer;
        pIoThreadContext->nDistributeType = g_params.tConfigIO.szConfigIOItem[i].nDistributeType;
        nRet = hmi_plat_CreateThread((pfnCNV_PLAT_THREAD_RECALL)io_thread_run, &(g_params.tConfigIO.szConfigIOItem[i]), 0, &g_params.tConfigIO.szConfigIOItem[i].ulThreadId, &g_params.tConfigIO.szConfigIOItem[i].ThreadHandle);
        LOG_SYS_INFO("io thread start result:%d", nRet);
    }

    return nRet;
}