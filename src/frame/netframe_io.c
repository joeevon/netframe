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
extern ACCEPT_THREAD_CONTEXT g_tAcceptContext;

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

void  monitor_iothread(IO_THREAD_CONTEXT *pIoThreadContext)
{
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

        STATISTICS_QUEQUE_DATA *ptStatisQueData = NULL;
        pIoThreadContext->pfncnv_monitor_callback(&pIoThreadContext->tMonitorElement, &ptStatisQueData);

        int nRet = lockfree_queue_enqueue(&(g_tAcceptContext.statis_msgque), ptStatisQueData, 1);   //队列满了把数据丢掉,以免内存泄露
        if(nRet == false)
        {
            LOG_SYS_ERROR("auxiliary queue is full!");
            cnv_comm_Free(ptStatisQueData->pData);
            cnv_comm_Free(ptStatisQueData);
        }

        uint64_t ulWakeup = 1;   //任意值,无实际意义
        nRet = write(g_tAcceptContext.accept_eventfd, &ulWakeup, sizeof(ulWakeup));  //io唤醒handle
        if(nRet != sizeof(ulWakeup))
        {
            LOG_SYS_FATAL("io wake up accept failed !");
        }
    }

    bzero(&pIoThreadContext->tMonitorElement, sizeof(pIoThreadContext->tMonitorElement));
    LOG_ACC_DEBUG("");
}

int iothread_handle_write(int Epollfd, void *pConnId, void *HashConnidFd, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("iothread_handle_write.");
    LOG_SYS_DEBUG("iothread_handle_write end.");
    return  0;
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

int32_t write_client_remain_data(IO_THREAD_CONTEXT *pIoThreadContext, SOCKET_ELEMENT *pSocketElement, HANDLE_TO_IO_DATA *pHandleIOData, int32_t nLenAlreadyWrite)
{
    int32_t nWriteAlready = 0;

    int32_t nRet = netframe_write(pSocketElement->Socket, pHandleIOData->pDataSend + nLenAlreadyWrite, pHandleIOData->lDataLen - nLenAlreadyWrite, &nWriteAlready);
    if(nRet == CNV_ERR_OK)
    {
        pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
        LOG_SYS_DEBUG("write %d bytes success.", nWriteAlready);
    }
    else
    {
        if(nRet == AGENT_NET_WRITE_INCOMPLETED)      //继续写未写完的数据
        {
            pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
            return write_client_remain_data(pIoThreadContext, pSocketElement, pHandleIOData, nLenAlreadyWrite + nWriteAlready);
        }
        else  //发送错误,因为是上一个遗留的数据,直接丢弃,避免造成乱包
        {
            LOG_SYS_ERROR("write remain data failed!");
            on_write_client_failed(pHandleIOData, pIoThreadContext);
            return -1;
        }
    }

    return CNV_ERR_OK;
}

int respond_write_client(int Epollfd, char *pOutValue, HANDLE_TO_IO_DATA *pHandleIOData, void *HashConnidFd, IO_THREAD_CONTEXT  *pIoThreadContext)
{
    LOG_SYS_DEBUG("respond_write_client.");
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);

    int nLenAlreadyWrite = 0;
    int nRet = netframe_write(pSocketElement->Socket, pHandleIOData->pDataSend, pHandleIOData->lDataLen, &nLenAlreadyWrite);
    if(nRet == CNV_ERR_OK)
    {
        pSocketElement->Time = cnv_comm_get_utctime();
    }
    else
    {
        if(nRet == AGENT_NET_WRITE_BUSY || nRet == AGENT_NET_WRITE_INCOMPLETED)   //没写完,继续写
        {
            LOG_SYS_ERROR("write imcompleted!");
            pSocketElement->Time = cnv_comm_get_utctime();
            return write_client_remain_data(pIoThreadContext, pSocketElement, pHandleIOData, nLenAlreadyWrite);
        }
        else if(nRet == AGENT_NET_CONNECTION_RESET || nRet == AGENT_NET_NOT_CONNECTED)   //连接异常
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

                if(!strcmp(ptNewSvrSockData->strServerIp, ptOriSvrSockData->strServerIp) && ptNewSvrSockData->nPort == ptOriSvrSockData->nPort)
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
                            || pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->nPort != pSvrSockData->nPort))  //同一类服务器
                {
                    nRet = netframe_write(pSocketElement->Socket, pHandleIOData->pDataSend, pHandleIOData->lDataLen, &nLenAlreadyWrite);
                    if(nRet == CNV_ERR_OK)
                    {
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
                            LOG_SYS_ERROR("connection to %s:%d is abnormal!", pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strServerIp, pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->nPort);
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
    snprintf(strPort, sizeof(strPort) - 1, "%d", pSvrSockData->nPort);
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
        ptSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
        LOG_SYS_DEBUG("write %d bytes successfully", pHandleIOData->lDataLen);
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
        pSocketElement->Time = cnv_comm_get_utctime();   //重置时间戳
        LOG_SYS_DEBUG("write %d bytes successfully", pHandleIOData->lDataLen);
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
            LOG_SYS_ERROR("connect is abnormal! ip:%s, port:%d", pSvrSockData->strServerIp, pSvrSockData->nPort);

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
        if(!strcmp(ptSvrSockData->strServerIp, ptHandleIOData->strServIp) && ptSvrSockData->nPort == ptHandleIOData->ulPort)
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
                free(pHandleIOData->pDataSend);
                free(pHandleIOData);
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
                free(pHandleIOData->pDataSend);
                free(pHandleIOData);
                continue;
            }

            HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)pOutValue;
            SOCKET_ELEMENT  *pSocketElement = (SOCKET_ELEMENT *)pHashValue->pValue;
            remove_client_socket_hashmap(pIoThreadContext->Epollfd, pIoThreadContext->HashConnidFd, pSocketElement->pConnId);
        }

        free(pHandleIOData->pDataSend);
        free(pHandleIOData);
    }
    pIoThreadContext->tMonitorElement.nRespondTimes++;

    if(lock_block_queue(handle_io_msgque) == true)
    {
        if(handle_io_msgque->unblockqueue->size_ <= 0)
        {
            uint64_t  ulData = 0;
            nRet = read(Eventfd, &ulData, sizeof(uint64_t));   //此数无用,读出缓存消息,避免epoll重复提醒
        }
        unlock_block_queue(handle_io_msgque);
    }

    LOG_SYS_DEBUG("iothread_handle_respond end.");
    return  CNV_ERR_OK;
}

//选择handle线程
int  io_select_handle_thread(IO_THREAD_CONTEXT *pIoThreadContext, HANDLE_THREAD_CONTEXT **pHandleContext)
{
    int  lThreadIndex = 0;

    char *pThreadIndex = (char *)poll_unblock_queue_head(pIoThreadContext->queDistribute);
    push_unblock_queue_tail(pIoThreadContext->queDistribute, pThreadIndex);    //此处取出后重新插入,达到分配效果
    lThreadIndex = atoi(pThreadIndex);
    LOG_SYS_DEBUG("io thread %d select handle thread %d", pIoThreadContext->threadindex, lThreadIndex);
    *pHandleContext = (pIoThreadContext->szHandleContext)[lThreadIndex];
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

        SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)malloc(sizeof(SOCKET_ELEMENT));
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
        memcpy(pSocketElement->uSockElement.tClnSockElement.strClientIp, AcceptIOData.strClientIp, sizeof(pSocketElement->uSockElement.tClnSockElement.strClientIp) - 1);
        pSocketElement->uSockElement.tClnSockElement.uClientPort = AcceptIOData.uClientPort;
        pSocketElement->uSockElement.tClnSockElement.pfncnv_parse_protocol = AcceptIOData.pfncnv_parse_protocol;
        pSocketElement->uSockElement.tClnSockElement.pfncnv_handle_business = AcceptIOData.pfncnv_handle_business;

        if(pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer == NULL)    //接收数据缓存
        {
            pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer = (char *)malloc(g_params.nMaxBufferSize);
            if(pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer == NULL)
            {
                return  CNV_ERR_MALLOC;
            }
        }

        char *pKey = (char *)malloc(33);
        if(!pKey)
        {
            cnv_comm_Free(pSocketElement);
            continue;
        }
        memset(pKey, 0, 33);

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

        HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)malloc(sizeof(HASHMAP_VALUE));
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
int iothread_handle_read(int Epollfd, void *pConnId, int nSocket, void *HashConnidFd, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("iothread_handle_read.");

    void *pOutValue = NULL;
    if(cnv_hashmap_get(HashConnidFd, pConnId, &pOutValue) != K_SUCCEED)  //用connid获取socket相关结构体
    {
        LOG_SYS_DEBUG("threadid:%d,hashmap can not get value! HashConnidFd.size=%d", pIoThreadContext->threadindex, cnv_hashmap_size(HashConnidFd));
        cnv_hashmap_iterator(HashConnidFd, printhashmap, NULL);
        netframe_delete_event(Epollfd, nSocket);
        netframe_close_socket(nSocket);
        return CNV_ERR_HASHMAP_GET;
    }
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);
    struct msghdr *pmsg = &(pSocketElement->uSockElement.tClnSockElement.msg);
    CLIENT_SOCKET_DATA *ptClnSockData = &(pSocketElement->uSockElement.tClnSockElement.SocketData);
    pmsg->msg_iov->iov_base = ptClnSockData->pDataBuffer + ptClnSockData->lDataRemain;       //拼接剩余数据
    pmsg->msg_iov->iov_len = g_params.nMaxBufferSize - ptClnSockData->lDataRemain;    //剩余缓存长度
    memset(pmsg->msg_iov->iov_base, 0, pmsg->msg_iov->iov_len);

    int nDataReadLen = 0;
    int nRet = netframe_recvmsg(pSocketElement->Socket, pmsg, &nDataReadLen);  //接收数据
    if(nRet != CNV_ERR_OK)
    {
        if(nRet == AGENT_NET_CLIENT_CLOSED)   //客户端关闭
        {
            remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
        }
        else if(nRet == AGENT_NET_READ_BUSY)  //系统繁忙
        {
            LOG_SYS_DEBUG("threadid:%d, read nothing.", pIoThreadContext->threadindex);
        }
        else if(nRet == AGENT_NET_READ_ABNORMAL)    //读取错误
        {
            remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
        }

        return nRet;
    }

    pSocketElement->Time = cnv_comm_get_utctime();  //收、发数据后重置时间戳
    pIoThreadContext->tMonitorElement.lRecvPackNum++;
    ptClnSockData->pMovePointer = ptClnSockData->pDataBuffer;
    ptClnSockData->lDataRemain += nDataReadLen;
    LOG_SYS_DEBUG("lDataRemain:%d, read data length:%d", ptClnSockData->lDataRemain, nDataReadLen);

    pfnCNV_PARSE_PROTOCOL pfncnvparseprotocol = pSocketElement->uSockElement.tClnSockElement.pfncnv_parse_protocol;  //协议解析回调函数

    while(ptClnSockData->lDataRemain > 0)
    {
        char *pPacket = NULL;
        unsigned int nPacketSize = 0;
        unsigned int nMoveSize = 0;
        nRet = pfncnvparseprotocol(&(ptClnSockData->pMovePointer), &(ptClnSockData->lDataRemain), &pPacket, &nPacketSize, &nMoveSize);  //协议解析
        if(nRet != CNV_PARSE_SUCCESS)
        {
            if(nRet == CNV_PARSE_FINISH)  //结束解析而且有剩余数据
            {
                if(ptClnSockData->lDataRemain < g_params.nMaxBufferSize)
                {
                    memcpy(ptClnSockData->pDataBuffer, ptClnSockData->pMovePointer, ptClnSockData->lDataRemain);
                }
                else
                {
                    LOG_SYS_ERROR("single package beyond max buffer.");
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
                }
            }
            else if(nRet == CNV_PARSE_SHUTDOWN)    //关闭客户端
            {
                remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
            }
            else if(nRet == CNV_PARSE_MOVE)     //数据偏移
            {
                ptClnSockData->lDataRemain -= nMoveSize;      //总数据长度减去一个包的数据大小
                ptClnSockData->pMovePointer += nMoveSize; //数据缓存指针偏移
                continue;
            }
            else if(nRet == CNV_PARSE_ERROR)    //解析错误,关闭客户端
            {
                remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
            }

            break;
        }

        if(nMoveSize > ptClnSockData->lDataRemain)
        {
            LOG_SYS_ERROR("MoveSize larger than DataRemain.");
            remove_client_socket_hashmap(Epollfd, HashConnidFd, pConnId);
            break;
        }

        ptClnSockData->lDataRemain -= nMoveSize;      //总数据长度减去一个包的数据大小
        ptClnSockData->pMovePointer += nMoveSize; //数据缓存指针偏移
        pIoThreadContext->tMonitorElement.lParsePackNum++;

        IO_TO_HANDLE_DATA *pIOHanldeData = (IO_TO_HANDLE_DATA *)malloc(sizeof(IO_TO_HANDLE_DATA));    //io->handle  header
        pIOHanldeData->lConnectID = atoi((char *)pConnId);
        memcpy(pIOHanldeData->strClientIp, pSocketElement->uSockElement.tClnSockElement.strClientIp, sizeof(pIOHanldeData->strClientIp) - 1);
        pIOHanldeData->ulPort = pSocketElement->uSockElement.tClnSockElement.uClientPort;
        pIOHanldeData->pDataSend = pPacket;
        pIOHanldeData->lDataLen = nPacketSize;
        pIOHanldeData->handle_io_eventfd = pIoThreadContext->handle_io_eventfd;
        pIOHanldeData->handle_io_msgque = pIoThreadContext->handle_io_msgque;
        pIOHanldeData->pfncnv_handle_business = pSocketElement->uSockElement.tClnSockElement.pfncnv_handle_business;

        HANDLE_THREAD_CONTEXT *pHandleContext = NULL;
        io_select_handle_thread(pIoThreadContext, &pHandleContext);

        nRet = lockfree_queue_enqueue(&(pHandleContext->io_handle_msgque), pIOHanldeData, 1);   //队列满了把数据丢掉,以免内存泄露
        if(nRet == false)
        {
            LOG_SYS_ERROR("io_handle queue is full!");
            free(pPacket);
            free(pIOHanldeData);
            pIoThreadContext->tMonitorElement.lSvrFailedNum++;
            continue;
        }

        uint64_t  ulWakeup = 1;   //任意值,无实际意义
        nRet = write(pHandleContext->io_handle_eventfd, &ulWakeup, sizeof(ulWakeup));  //io唤醒handle
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
    CALLBACK_STRUCT_T tCallback;
    bzero(&tCallback, sizeof(tCallback));
    snprintf(tCallback.strProtocol, sizeof(tCallback.strProtocol) - 1, "io_monitor");
    set_callback_function(SERVER_CALLBACK_FUNC, &tCallback);
    pIoThreadContext->pfncnv_monitor_callback = tCallback.pfncnv_monitor_callback;

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
    uint64_t ulData = 0;
    struct epoll_event szEpollEvent[DEFAULF_EPOLL_SIZE];
    memset(szEpollEvent, 0, sizeof(szEpollEvent));
    IO_THREAD_ITEM *pTheadparam = (IO_THREAD_ITEM *)pThreadParameter;
    IO_THREAD_CONTEXT *pIoThreadContext = pTheadparam->pIoThreadContext;
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
        int nCount = epoll_wait(Epollfd, szEpollEvent, DEFAULF_EPOLL_SIZE, -1);
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
                        iothread_handle_read(Epollfd, szEpollEvent[i].data.ptr, szEpollEvent[i].data.fd, HashConnidFd, pIoThreadContext);
                    }
                }
                else if(szEpollEvent[i].events & EPOLLRDHUP)   //对端关闭
                {
                    LOG_SYS_DEBUG("peer shutdown.");
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
                else if((szEpollEvent[i].events & EPOLLHUP) && !(szEpollEvent[i].events & EPOLLIN))  //错误
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
                else if(szEpollEvent[i].events & POLLNVAL)
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                }
                else if(szEpollEvent[i].events & (EPOLLERR | POLLNVAL))
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
                else if(szEpollEvent[i].events & EPOLLOUT)  //写事件
                {
                    iothread_handle_write(Epollfd, szEpollEvent[i].data.ptr, HashConnidFd, pIoThreadContext);
                }
                else
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                    remove_client_socket_hashmap(Epollfd, HashConnidFd, szEpollEvent[i].data.ptr);
                }
            }

            memset(szEpollEvent, 0, sizeof(struct epoll_event)*nCount);
        }
        else if(nCount < 0)   //错误
        {
            LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
        }
    }

    return nRet;
}

//功能:io线程反初始化
void  io_thread_uninit(IO_THREAD_CONTEXT *pIoThreadContexts)
{
    free_server_unblock_queue(pIoThreadContexts[0].queServer);  //服务的配置队列,所有IO线程公用,释放一次即可
    cnv_comm_Free(pIoThreadContexts[0].queServer);
    pIoThreadContexts[0].queServer = NULL;
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
        nRet = hmi_plat_CreateThread((pfnCNV_PLAT_THREAD_RECALL)io_thread_run, &(g_params.tConfigIO.szConfigIOItem[i]), 0, &g_params.tConfigIO.szConfigIOItem[i].ulThreadId, &g_params.tConfigIO.szConfigIOItem[i].ThreadHandle);
        LOG_SYS_INFO("io thread start result:%d", nRet);
    }

    return nRet;
}