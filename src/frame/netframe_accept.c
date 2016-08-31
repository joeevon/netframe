/****************************
    FileName:netframe_accept.c
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_accept  C文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-19
*****************************/

#define _GNU_SOURCE
#include "netframe_accept.h"
#include "netframe_admin.h"
#include "cnv_comm.h"
#include "cnv_hashmap.h"
#include "cnv_thread.h"
#include "log/cnv_liblog4cplus.h"
#include "netframe_net.h"
#include "cnv_net_define.h"
#include "cnv_unblock_queue.h"
#include "cnv_blocking_queue.h"
#include <poll.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>

int  accept_select_io_thread(ACCEPT_THREAD_ITEM *pAcceptItem, IO_THREAD_CONTEXT **pIoThreadContext, CNV_UNBLOCKING_QUEUE *queEventfds)
{
    int  nRet = CNV_ERR_OK;
    int  lIndex = 0;
    char strKey[32] = { 0 };

    char  *pIndex = (char *)poll_unblock_queue_head(pAcceptItem->queDistribute);
    push_unblock_queue_tail(pAcceptItem->queDistribute, pIndex);    //此处取出后重新插入,达到分配效果
    lIndex = atoi(pIndex);
    LOG_SYS_DEBUG("protocol %s select io thread %d", pAcceptItem->tCallback.strProtocol, lIndex);
    *pIoThreadContext = pAcceptItem->szIOThreadContext[lIndex];
    if(!*pIoThreadContext)
    {
        LOG_SYS_ERROR("accept select io thread failed, lIndex:%d", lIndex);
        return  CNV_ERR_SELECT_THREAD;
    }

    snprintf(strKey, sizeof(strKey) - 1, "%d", lIndex);
    nRet = iterator_unblock_queuqe(queEventfds, queue_search_int, strKey);  // 避免重复唤醒
    if(!nRet)    //原来的队列中不存在
    {
        char *pEventIndex = (char *)cnv_comm_Malloc(sizeof(int));
        if(!pEventIndex)
        {
            LOG_SYS_ERROR("cnv_comm_Malloc failed!");
            return CNV_ERR_MALLOC;
        }
        snprintf(pEventIndex, sizeof(int), "%s", pIndex);
        push_unblock_queue_tail(queEventfds, pEventIndex);
    }

    return CNV_ERR_OK;
}

int  accept_set_iodata(int ClientFd, ACCEPT_THREAD_ITEM *pAcceptItem, struct sockaddr_in *ClientAddr, ACCEPT_TO_IO_DATA *AcceptIOData)
{
    AcceptIOData->fd = ClientFd;  // 客户端fd
    if(pAcceptItem->uMapType == 0)   //默认用连接ID做映射
    {
        AcceptIOData->uMapType = 0;
    }
    else    //用客户端的ip和端口做映射
    {
        AcceptIOData->uMapType = 1;
        memcpy(AcceptIOData->strClientIp, inet_ntoa(ClientAddr->sin_addr), sizeof(AcceptIOData->strClientIp) - 1);
        AcceptIOData->uClientPort = ntohs(ClientAddr->sin_port);
    }
    snprintf(AcceptIOData->strTransmission, sizeof(AcceptIOData->strTransmission) - 1, "%s", pAcceptItem->strTransmission);   // 通信协议
    snprintf(AcceptIOData->strProtocol, sizeof(AcceptIOData->strProtocol) - 1, "%s", pAcceptItem->tCallback.strProtocol);     // 服务协议
    AcceptIOData->pfncnv_parse_protocol = pAcceptItem->tCallback.pfncnv_parse_protocol;
    AcceptIOData->pfncnv_handle_business = pAcceptItem->tCallback.pfncnv_handle_business;

    return  CNV_ERR_OK;
}

int  accept_filter_client(ACCEPT_THREAD_ITEM  *pAcceptItem, struct sockaddr_in *ClientAddr)
{
    if(pAcceptItem->tAllowedClients.lNumOfHosts <= 0)    //无需过滤
    {
        return 0;
    }

    int i;
    char strClientIP[33] = {0};
    snprintf(strClientIP, 32, "%s", inet_ntoa(ClientAddr->sin_addr));

    for(i = 0; i < pAcceptItem->tAllowedClients.lNumOfHosts; i++)
    {
        if(!strcmp(pAcceptItem->tAllowedClients.tHosts[i].strHost, strClientIP))
        {
            return 0;
        }
    }

    return 1;
}

int accept_get_portinfo(int ServerFd, void *HashFdListen, ACCEPT_THREAD_ITEM **pAcceptItem)
{
    int nRet = 0;
    char  strKey[33] = "";
    void  *pOutValue = NULL;
    snprintf(strKey, sizeof(strKey) - 1, "%d", ServerFd);

    nRet = cnv_hashmap_get(HashFdListen, strKey, &pOutValue);
    if(nRet != 0)
    {
        return nRet;
    }
    HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)pOutValue;
    *pAcceptItem = (ACCEPT_THREAD_ITEM *)(pHashValue->pValue);

    return CNV_ERR_OK;
}

int  accept_init_server(ACCEPT_THREAD_CONTEXT *pAcceptContext, ACCEPT_THREAD_ITEM *pAcceptItem, void *HashFdListen)
{
    int  nRet = CNV_ERR_OK;
    void  *pOutvalue = 0;
    int  Socket = 0;
    CNV_UNBLOCKING_QUEUE *queDistribute = pAcceptItem->queDistribute;

    cnv_parse_distribution(pAcceptItem->strAlgorithm, pAcceptItem->strDistribution, queDistribute);  //解析负载
    LOG_SYS_DEBUG("protocol : %s, port : %d, distribution:%s:", pAcceptItem->tCallback.strProtocol, pAcceptItem->ulPort, pAcceptItem->strDistribution);
    iterator_unblock_queuqe(queDistribute, printDistribution, NULL);
    if(!strcmp(pAcceptItem->tCallback.strProtocol, "admin"))      //telnet管理
    {
        pAcceptItem->tCallback.pfncnv_parse_protocol = admin_parse_data;
        pAcceptItem->tCallback.pfncnv_handle_business = admin_handle_data;
    }
    else
    {
        set_callback_function(CLIENT_CALLBACK_FUNC, &(pAcceptItem->tCallback));  //设置回调函数
    }

    if(!strcmp(pAcceptItem->strTransmission, "UDP"))   //udp协议创建好socket后直接添加到io线程epoll等待数据
    {
        if(get_unblock_queue_count(queDistribute) > 1)
        {
            LOG_SYS_ERROR("udp transmission no more than one io thread!");
            return CNV_ERR_CONFIG;
        }

        struct sockaddr_in tServAddr = { 0 };
        tServAddr.sin_family = AF_INET;
        tServAddr.sin_addr.s_addr = inet_addr(pAcceptItem->strHost);
        tServAddr.sin_port = htons(pAcceptItem->ulPort);

        nRet = netframe_init_udpserver(&Socket, &tServAddr);    //创建socket
        if(nRet != CNV_ERR_OK)
        {
            return nRet;
        }
        pAcceptContext->UdpSocket = Socket;

        ACCEPT_TO_IO_DATA  AcceptIOData = { 0 };
        AcceptIOData.fd = Socket;
        snprintf(AcceptIOData.strTransmission, sizeof(AcceptIOData.strTransmission) - 1, "%s", pAcceptItem->strTransmission);   //通信协议
        snprintf(AcceptIOData.strProtocol, sizeof(AcceptIOData.strProtocol) - 1, "%s", pAcceptItem->tCallback.strProtocol);     //服务协议
        AcceptIOData.pfncnv_parse_protocol = pAcceptItem->tCallback.pfncnv_parse_protocol;
        AcceptIOData.pfncnv_handle_business = pAcceptItem->tCallback.pfncnv_handle_business;

        char  *pIndex = (char *)poll_unblock_queue_head(pAcceptItem->queDistribute);
        push_unblock_queue_tail(pAcceptItem->queDistribute, pIndex);    //重新放进去，结束进程时释放内存，以免泄露
        int lIndex = atoi(pIndex);
        IO_THREAD_CONTEXT *pIoThreadContext = pAcceptItem->szIOThreadContext[lIndex];

        nRet = cnv_fifo_put(pIoThreadContext->accept_io_msgque, (unsigned char *)&AcceptIOData, sizeof(ACCEPT_TO_IO_DATA));  //accept -> io
        if(nRet == 0)
        {
            LOG_SYS_FATAL("accept put udp socket to io failed!");
            netframe_close_socket(Socket);
            return CNV_ERR_ACCEPT_IO_SOCKET;
        }

        uint64_t  ulWakeup = 10;
        nRet = write(pIoThreadContext->accept_io_eventfd, &ulWakeup, sizeof(ulWakeup));
        if(nRet != sizeof(ulWakeup))
        {
            LOG_SYS_ERROR("accept wake up io failed!");
        }
        else
        {
            LOG_SYS_DEBUG("accept send udp socket to io thread %d", lIndex);
        }
    }
    else    //tcp和unixsocket协议需要监听连接,在accept线程epoll管理
    {
        if(!strcmp(pAcceptItem->strTransmission, "TCP"))
        {
            struct sockaddr_in tServAddr = { 0 };
            tServAddr.sin_family = AF_INET;
            tServAddr.sin_addr.s_addr = inet_addr(pAcceptItem->strHost);
            tServAddr.sin_port = htons(pAcceptItem->ulPort);

            nRet = netframe_init_tcpserver(&Socket, &tServAddr, 20000);    //创建socket
            if(nRet != CNV_ERR_OK)
            {
                LOG_SYS_FATAL("netframe_init_tcpserver failed!");
                netframe_close_socket(Socket);
                return nRet;
            }
            pAcceptContext->TcpListenSocket = Socket;
        }
        else if(!strcmp(pAcceptItem->strTransmission, "UNIXSOCKET"))
        {
            struct sockaddr_un tServAddr = { 0 };
            tServAddr.sun_family = AF_UNIX;
            snprintf(tServAddr.sun_path, sizeof(tServAddr.sun_path) - 1, "%s", pAcceptItem->strUnixDomainPath);

            nRet = netframe_init_unixsocket(&Socket, &tServAddr);
            if(nRet != CNV_ERR_OK)
            {
                LOG_SYS_DEBUG("netframe_init_unixsocket failed!");
                netframe_close_socket(Socket);
                return nRet;
            }

            pAcceptContext->UnixListenSocket = Socket;
        }
        else
        {
            LOG_SYS_FATAL("please check transmission in config file :%s.", g_params.tConfigPath.strConfigPath);
            return CNV_ERR_CONFIG;
        }

        char *pKey = (char *)cnv_comm_Malloc(sizeof(int));
        if(!pKey)
        {
            return  CNV_ERR_MALLOC;
        }
        memset(pKey, 0x00, sizeof(int));  // 没有memset，后面get不到
        snprintf(pKey, sizeof(int), "%d", Socket);

        char *pValue = (char *)cnv_comm_Malloc(sizeof(ACCEPT_THREAD_ITEM));
        if(!pValue)
        {
            cnv_comm_Free(pKey);
            return  CNV_ERR_MALLOC;
        }
        memcpy(pValue, pAcceptItem, sizeof(ACCEPT_THREAD_ITEM));

        HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)cnv_comm_Malloc(sizeof(HASHMAP_VALUE));
        if(!pAcceptItem)
        {
            cnv_comm_Free(pKey);
            cnv_comm_Free(pValue);
            return  CNV_ERR_MALLOC;
        }
        pHashValue->lSize = sizeof(HASHMAP_VALUE);
        pHashValue->pValue = pValue;
        cnv_hashmap_put(HashFdListen, pKey, pHashValue, &pOutvalue);
        if(pOutvalue)
        {
            cnv_comm_Free(pOutvalue);
        }

        nRet = netframe_add_readevent(pAcceptContext->Epollfd, Socket, NULL);   //加入epoll读监听
        if(nRet != CNV_ERR_OK)
        {
            LOG_SYS_ERROR("netframe_add_readevent error!");
            netframe_close_socket(Socket);
            if(!strcmp(pAcceptItem->strUnixDomainPath, ""))
            {
                unlink(pAcceptItem->strUnixDomainPath);
            }

            return nRet;
        }
    }
    return CNV_ERR_OK;
}

int  netframe_init_accept(ACCEPT_THREAD_CONTEXT  *pAcceptContext, NETFRAME_CONFIG_ACCEPT *pThreadparam, void *HashFdListen)
{
    int  nRet = CNV_ERR_OK;
    int  i;

    for(i = 0; i < pThreadparam->lNumberOfPort; i++)        //绑定每个端口并加入读监听
    {
        nRet = accept_init_server(pAcceptContext, &(pThreadparam->szConfigAcceptItem[i]), HashFdListen);
        if(nRet != CNV_ERR_OK)
        {
            LOG_SYS_FATAL("accept_init_server failed!");
            return nRet;
        }
    }

    return  CNV_ERR_OK;
}

int  accept_thread_run(void *pThreadParameter)
{
    int nCount = -1;
    uint64_t ulWakeup = 1;   //任意值,无实际意义
    int lClientfd = 0;
    socklen_t lClientLen = sizeof(struct sockaddr_in);
    struct sockaddr_in tClientAddr;
    memset(&tClientAddr, 0x00, sizeof(struct sockaddr_in));
    struct epoll_event szEpollEvent[DEFAULF_EPOLL_SIZE];
    memset(szEpollEvent, 0x00, sizeof(szEpollEvent));
    NETFRAME_CONFIG_ACCEPT *pThreadparam = (NETFRAME_CONFIG_ACCEPT *)pThreadParameter;
    ACCEPT_THREAD_CONTEXT  *pAcceptContext = pThreadparam->pAcceptContext;
    IO_THREAD_CONTEXT *pIoThreadContexts = pAcceptContext->pIoThreadContexts;
    void *HashFdListen = pAcceptContext->HashFdListen;
    CNV_UNBLOCKING_QUEUE  *queEventfds = pAcceptContext->queEventfds;  //需要唤醒的队列
    IO_THREAD_CONTEXT *pIoThreadContext = NULL;
    ACCEPT_TO_IO_DATA  AcceptIOData = { 0 };

    int nRet = netframe_init_accept(pAcceptContext, pThreadparam, HashFdListen);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_init_accept failed!");
        if(nRet == AGENT_NET_BIND_FAILED)
        {
            exit(-1);
        }
        return  nRet;
    }

    while(1)
    {
        nCount = epoll_wait(pAcceptContext->Epollfd, szEpollEvent, DEFAULF_EPOLL_SIZE, -1);
        if(nCount > 0)
        {
            for(int i = 0; i < nCount; i++)
            {
                if((szEpollEvent[i].events & EPOLLHUP) && !(szEpollEvent[i].events & EPOLLIN))
                {
                    LOG_SYS_ERROR("%s", strerror(errno));
                }
                if(szEpollEvent[i].events & POLLNVAL)
                {
                    LOG_SYS_ERROR("%s", strerror(errno));
                }
                if(szEpollEvent[i].events & (EPOLLERR | POLLNVAL))
                {
                    LOG_SYS_ERROR("%s", strerror(errno));
                }
                if(szEpollEvent[i].events & (EPOLLIN | EPOLLPRI))
                {
                    lClientfd = accept4(szEpollEvent[i].data.fd, (struct sockaddr *)&tClientAddr, &lClientLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if(lClientfd == -1)
                    {
                        LOG_SYS_ERROR("%s.", strerror(errno));
                        continue;
                    }
                    LOG_SYS_DEBUG("accept, client ip:%s, socket = %d", inet_ntoa(tClientAddr.sin_addr), lClientfd);

                    ACCEPT_THREAD_ITEM *pAcceptItem = NULL;
                    nRet = accept_get_portinfo(szEpollEvent[i].data.fd, HashFdListen, &pAcceptItem);
                    if(nRet != CNV_ERR_OK)
                    {
                        LOG_SYS_ERROR("accept_get_portinfo error!");
                        netframe_close_socket(lClientfd);
                        continue;
                    }

                    nRet = accept_filter_client(pAcceptItem, &tClientAddr);
                    if(nRet != 0)
                    {
                        LOG_SYS_ERROR("not allowed client, ip:%s", inet_ntoa(tClientAddr.sin_addr));
                        netframe_close_socket(lClientfd);
                        continue;
                    }

                    memset(&AcceptIOData, 0, sizeof(ACCEPT_TO_IO_DATA));
                    nRet = accept_set_iodata(lClientfd, pAcceptItem, &tClientAddr, &AcceptIOData);   //写入IO队列的数据
                    if(nRet != CNV_ERR_OK)
                    {
                        LOG_SYS_ERROR("accept_set_iodata error!");
                        netframe_close_socket(lClientfd);
                        continue;
                    }

                    nRet = accept_select_io_thread(pAcceptItem, &pIoThreadContext, queEventfds);  //选择线程
                    if(nRet != CNV_ERR_OK)
                    {
                        netframe_close_socket(lClientfd);
                        continue;
                    }

                    nRet = cnv_fifo_put(pIoThreadContext->accept_io_msgque, (unsigned char *)&AcceptIOData, sizeof(ACCEPT_TO_IO_DATA));
                    if(nRet == 0)
                    {
                        LOG_SYS_ERROR("cnv_fifo_put error!");
                        netframe_close_socket(lClientfd);
                        continue;
                    }
                }
                else
                {
                    LOG_SYS_ERROR("unrecognized error, %s", strerror(errno));
                }
            }
        }
        else if(nCount < 0)
        {
            LOG_SYS_ERROR("%s", strerror(errno));
        }

        int nNumOfEvent = get_unblock_queue_count(queEventfds);
        while(nNumOfEvent--)      //需要唤醒的队列
        {
            char *pEventIndex = (char *)poll_unblock_queue_head(queEventfds);
            int nIndex = atoi(pEventIndex);
            IO_THREAD_CONTEXT *pContext = &(pIoThreadContexts[nIndex - 1]);      //T1:对应0号数组变量，T2:1号 ......
            nRet = write(pContext->accept_io_eventfd, &ulWakeup, sizeof(ulWakeup));
            if(nRet != sizeof(ulWakeup))
            {
                LOG_SYS_FATAL("accept wake up io failed !");
            }
            else
            {
                LOG_SYS_DEBUG("write io thread %d", nIndex);
            }
            cnv_comm_Free(pEventIndex);
        }
    }
    return nRet;
}

int accept_set_io_context(ACCEPT_THREAD_ITEM *pConfigAcceptItem, IO_THREAD_CONTEXT *pIoThreadContexts)
{
    int  lThreadIndex = 0;
    char  DistriTrans[32] = { 0 };
    cnv_comm_string_trans(pConfigAcceptItem->strDistribution, sizeof(DistriTrans), ',', DistriTrans);

    char *pDistribution = strtok(DistriTrans, ",");
    while(pDistribution)
    {
        lThreadIndex = atoi(pDistribution);
        if(lThreadIndex > g_params.tConfigIO.lNumberOfThread)    //  索引比实际有的线程数大,检查配置文件
        {
            LOG_SYS_FATAL("please check the confile file!");
            return  CNV_ERR_PARAM;
        }
        pConfigAcceptItem->szIOThreadContext[lThreadIndex] = &(pIoThreadContexts[lThreadIndex - 1]);
        pDistribution = strtok(NULL, ",");
    }

    return  CNV_ERR_OK;
}

void  accept_thread_uninit(ACCEPT_THREAD_CONTEXT *pAcceptContexts)
{
    int  i = 0;
    int Epollfd = pAcceptContexts->Epollfd;

    for(i = 0; i < g_params.tConfigAccept.lNumberOfPort; ++i)
    {
        free_unblock_queue(pAcceptContexts->pConfigAcceptItem[i].queDistribute);
        cnv_comm_Free(pAcceptContexts->pConfigAcceptItem[i].queDistribute);
    }
    free_unblock_queue(pAcceptContexts->queEventfds);
    cnv_comm_Free(pAcceptContexts->queEventfds);
    cnv_hashmap_free_socket(pAcceptContexts->HashFdListen, &Epollfd);
    netframe_close_socket(pAcceptContexts->TcpListenSocket);
    netframe_close_socket(pAcceptContexts->UnixListenSocket);
    netframe_close_socket(pAcceptContexts->UdpSocket);
    close(pAcceptContexts->Epollfd);
}

int  accept_thread_init(NETFRAME_CONFIG_ACCEPT *ptConfigAccept, IO_THREAD_CONTEXT *pIoThreadContexts, ACCEPT_THREAD_CONTEXT *pAcceptContext)
{
    int  nRet = CNV_ERR_OK;
    int  i = 0;
    pAcceptContext->pIoThreadContexts = pIoThreadContexts;
    netframe_create_epoll(&(pAcceptContext->Epollfd), 5);
    pAcceptContext->queEventfds = (CNV_UNBLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
    if(!pAcceptContext->queEventfds)
    {
        return CNV_ERR_MALLOC;
    }
    initiate_unblock_queue(pAcceptContext->queEventfds, MAX_IO_THREAD);

    nRet = cnv_hashmap_init(&(pAcceptContext->HashFdListen), 20, cnv_hashmap_inthash, cnv_hashmap_intequals);
    if(nRet != CNV_ERR_OK)
    {
        return  CNV_ERR_HASHMAP_INIT;
    }

    for(i = 0; i < ptConfigAccept->lNumberOfPort; i++)
    {
        ptConfigAccept->szConfigAcceptItem[i].queDistribute = (CNV_UNBLOCKING_QUEUE *)cnv_comm_Malloc(sizeof(CNV_UNBLOCKING_QUEUE));
        if(!ptConfigAccept->szConfigAcceptItem[i].queDistribute)
        {
            return CNV_ERR_MALLOC;
        }

        initiate_unblock_queue(ptConfigAccept->szConfigAcceptItem[i].queDistribute, 30);

        nRet = accept_set_io_context(&(ptConfigAccept->szConfigAcceptItem[i]), pIoThreadContexts);
        if(nRet != CNV_ERR_OK)
        {
            return  nRet;
        }
    }

    pAcceptContext->pConfigAcceptItem = ptConfigAccept->szConfigAcceptItem;
    ptConfigAccept->pAcceptContext = pAcceptContext;

    return  nRet;
}

//开启accept线程
int  accept_thread_start(IO_THREAD_CONTEXT *pIoThreadContexts, ACCEPT_THREAD_CONTEXT *pAcceptContext)
{
    int  nRet = CNV_ERR_OK;

    if(g_params.tConfigAccept.lNumberOfPort > 0)
    {
        nRet = accept_thread_init(&g_params.tConfigAccept, pIoThreadContexts, pAcceptContext);
        if(nRet != CNV_ERR_OK)
        {
            return nRet;
        }
        nRet = hmi_plat_CreateThread((pfnCNV_PLAT_THREAD_RECALL)accept_thread_run, &g_params.tConfigAccept, 0, &g_params.tConfigAccept.ulThreadId, &g_params.tConfigAccept.ThreadHandle);
        LOG_SYS_INFO("accept thread start result:%d", nRet);
    }

    return nRet;
}
