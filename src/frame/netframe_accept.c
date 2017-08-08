
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
#include <stdint.h>
#include <arpa/inet.h>
#include <sys/eventfd.h>

extern IO_THREAD_CONTEXT g_szIoThreadContexts[MAX_IO_THREAD];

void  free_accept_lockfreequeue(LOCKFREE_QUEUE  *statis_msgque)
{
    STATISTICS_QUEQUE_DATA *ptStatisQueData = NULL;
    int nCount = lockfree_queue_len(statis_msgque);
    while(nCount--)
    {
        ptStatisQueData = (STATISTICS_QUEQUE_DATA *)lockfree_queue_dequeue(statis_msgque, 1);
        free(ptStatisQueData->pData);
        free(ptStatisQueData);
    }
}

void statistics_data(int nAcceptFd, uint32_t nStatisThreadNum, ACCEPT_THREAD_CONTEXT *pAcceptContext)
{
    int nRet = 0;

    STATISTICS_QUEQUE_DATA *ptStatisQueData = (STATISTICS_QUEQUE_DATA *)lockfree_queue_dequeue(&pAcceptContext->statis_msgque, 1);
    if(ptStatisQueData == NULL)
    {
        return;
    }

    if(pAcceptContext->tStatisCallback.pfncnv_statistics_callback != NULL)
    {
        pAcceptContext->tStatisCallback.pfncnv_statistics_callback(ptStatisQueData, nStatisThreadNum, &(pAcceptContext->queStatisData));
    }

    uint64_t ulWakeup = 1;  //任意值,无实际意义
    K_BOOL bIsWakeIO = K_FALSE;
    int nNumOfPostMsg = get_unblock_queue_count(&(pAcceptContext->queStatisData));
    LOG_SYS_DEBUG("nNumOfPostMsg = %d", nNumOfPostMsg);
    while(nNumOfPostMsg--)      // handle线程单独用的队列,无需加锁
    {
        void *pPostData = poll_unblock_queue_head(&(pAcceptContext->queStatisData));
        nRet = push_block_queue_tail(g_szIoThreadContexts[0].handle_io_msgque, pPostData, 1);  //队列满了把数据丢掉,以免内存泄露
        if(nRet == false)
        {
            LOG_SYS_ERROR("handle_io queue is full!");
            HANDLE_TO_IO_DATA *pHandleIOData = (HANDLE_TO_IO_DATA *)pPostData;
            free(pHandleIOData->pDataSend);
            free(pHandleIOData);
            continue;
        }
        bIsWakeIO = true;
    }

    if(bIsWakeIO)
    {
        nRet = write(g_szIoThreadContexts[0].handle_io_eventfd, &ulWakeup, sizeof(ulWakeup));  //handle唤醒io
        if(nRet != sizeof(ulWakeup))
        {
            LOG_SYS_ERROR("handle wake io failed.");
        }
        bIsWakeIO = K_FALSE;
    }

    free(ptStatisQueData->pData);
    free(ptStatisQueData);

    if(lockfree_queue_len(&pAcceptContext->statis_msgque) <= 0)
    {
        uint64_t ulData = 0;
        nRet = read(nAcceptFd, &ulData, sizeof(uint64_t));   //此数据无实际意义,读出避免重复提醒
    }
}

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
    nRet = iterator_unblock_queuqe(queEventfds, queue_search_int, strKey);  //避免重复唤醒
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
    memcpy(AcceptIOData->strClientIp, inet_ntoa(ClientAddr->sin_addr), sizeof(AcceptIOData->strClientIp) - 1);
    AcceptIOData->uClientPort = ntohs(ClientAddr->sin_port);
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

int accept_client_connect(int nServerFd, ACCEPT_THREAD_CONTEXT *pAcceptContext, CNV_UNBLOCKING_QUEUE *queEventfds)
{
    int nRet = 0;
    int  nClientfd = 0;
    socklen_t  nClientLen = sizeof(struct sockaddr_in);
    struct sockaddr_in  tClientAddr;
    memset(&tClientAddr, 0x00, sizeof(struct sockaddr_in));

    nClientfd = accept4(nServerFd, (struct sockaddr *)&tClientAddr, &nClientLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if(nClientfd == -1)
    {
        LOG_SYS_ERROR("%s.", strerror(errno));
        return -1;
    }
    LOG_SYS_DEBUG("accept, client ip:%s, socket = %d", inet_ntoa(tClientAddr.sin_addr), nClientfd);

    ACCEPT_THREAD_ITEM *pAcceptItem = NULL;
    nRet = accept_get_portinfo(nServerFd, pAcceptContext->HashFdListen, &pAcceptItem);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("accept_get_portinfo error!");
        netframe_close_socket(nClientfd);
        return -1;
    }

    //nRet = accept_filter_client(pAcceptItem, &tClientAddr);
    //if(nRet != 0)
    //{
    //    LOG_SYS_ERROR("not allowed client, ip:%s", inet_ntoa(tClientAddr.sin_addr));
    //    netframe_close_socket(nClientfd);
    //    return -1;
    //}

    ACCEPT_TO_IO_DATA  tAcceptIOData = { 0 };
    nRet = accept_set_iodata(nClientfd, pAcceptItem, &tClientAddr, &tAcceptIOData);   //写入IO队列的数据
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("accept_set_iodata error!");
        netframe_close_socket(nClientfd);
        return -1;
    }

    IO_THREAD_CONTEXT *pIoThreadContext = NULL;
    nRet = accept_select_io_thread(pAcceptItem, &pIoThreadContext, queEventfds);  //选择IO线程
    if(nRet != CNV_ERR_OK)
    {
        netframe_close_socket(nClientfd);
        return -1;
    }

    nRet = cnv_fifo_put(pIoThreadContext->accept_io_msgque, (unsigned char *)&tAcceptIOData, sizeof(ACCEPT_TO_IO_DATA));
    if(nRet == 0)
    {
        LOG_SYS_ERROR("cnv_fifo_put error!");
        netframe_close_socket(nClientfd);
        return -1;
    }

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
        /*pAcceptItem->tCallback.pfncnv_parse_protocol = admin_parse_data;
        pAcceptItem->tCallback.pfncnv_handle_business = admin_handle_data;*/
    }
    else
    {
        set_callback_function(CLIENT_CALLBACK_FUNC, &(pAcceptItem->tCallback));  //设置回调函数
        if(pAcceptItem->tCallback.pfncnv_parse_protocol == NULL || pAcceptItem->tCallback.pfncnv_handle_business == NULL)
        {
            LOG_SYS_ERROR("callback function is abnormal.");
            return -1;
        }
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

        char *pKey = (char *)cnv_comm_Malloc(33);
        if(!pKey)
        {
            return  CNV_ERR_MALLOC;
        }
        memset(pKey, 0, 33);  // 没有memset，后面get不到
        snprintf(pKey, 32, "%d", Socket);

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

    //监听accept_eventfd
    nRet = netframe_setblockopt(pAcceptContext->accept_eventfd, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return  nRet;
    }

    nRet = netframe_add_readevent(pAcceptContext->Epollfd, pAcceptContext->accept_eventfd, NULL);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_add_readevent failed!");
        return  nRet;
    }

    snprintf(pAcceptContext->tStatisCallback.strProtocol, sizeof(pAcceptContext->tStatisCallback.strProtocol) - 1, "statistics");
    set_callback_function(SERVER_CALLBACK_FUNC, &pAcceptContext->tStatisCallback);

    return  CNV_ERR_OK;
}

int  accept_thread_run(NETFRAME_CONFIG_ACCEPT *pThreadparam)
{
    uint64_t ulWakeup = 1;   //任意值,无实际意义
    struct epoll_event szEpollEvent[DEFAULF_EPOLL_SIZE];
    bzero(szEpollEvent, sizeof(szEpollEvent));
    ACCEPT_THREAD_CONTEXT *pAcceptContext = pThreadparam->pAcceptContext;
    IO_THREAD_CONTEXT *pIoThreadContexts = pAcceptContext->pIoThreadContexts;
    CNV_UNBLOCKING_QUEUE *queEventfds = pAcceptContext->queEventfds;  //需要唤醒的队列
    int Epollfd = pAcceptContext->Epollfd;
    int AcceptFd = pAcceptContext->accept_eventfd;
    uint32_t nStatisThreadNum = 0;

    for(int32_t nIndex = 0; nIndex < g_params.tConfigIO.lNumberOfThread; nIndex++)
    {
        if(g_params.tConfigIO.szConfigIOItem[nIndex].nIsStasistics > 0)
        {
            nStatisThreadNum++;
        }
    }

    for(int32_t nIndex = 0; nIndex < g_params.tConfigHandle.lNumberOfThread; nIndex++)
    {
        if(g_params.tConfigHandle.szConfigHandleItem[nIndex].nIsStasistics > 0)
        {
            nStatisThreadNum++;
        }
    }

    int nRet = netframe_init_accept(pAcceptContext, pThreadparam, pAcceptContext->HashFdListen);
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
        int nCount = epoll_wait(Epollfd, szEpollEvent, DEFAULF_EPOLL_SIZE, -1);
        if(nCount > 0)
        {
            for(int i = 0; i < nCount; i++)
            {
                if(szEpollEvent[i].events & (EPOLLIN | EPOLLPRI))
                {
                    if(szEpollEvent[i].data.fd == AcceptFd)  //统计数据
                    {
                        statistics_data(szEpollEvent[i].data.fd, nStatisThreadNum, pAcceptContext);
                    }
                    else    //客户端连接
                    {
                        accept_client_connect(szEpollEvent[i].data.fd, pAcceptContext, queEventfds);
                    }
                }
                else if((szEpollEvent[i].events & EPOLLHUP) && !(szEpollEvent[i].events & EPOLLIN))
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                }
                else if(szEpollEvent[i].events & POLLNVAL)
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                }
                else if(szEpollEvent[i].events & (EPOLLERR | POLLNVAL))
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                }
                else if(errno == EINTR)
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                }
                else
                {
                    LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
                }
            }

            memset(szEpollEvent, 0, sizeof(struct epoll_event)*nCount);
        }
        else if(nCount < 0)
        {
            LOG_SYS_ERROR("epoll_wait abnormal,%s.", strerror(errno));
            exit(0);  //此错误会引起线程死循环,为解决,先退出进程
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
                LOG_SYS_FATAL("accept wake io failed !");
            }

            free(pEventIndex);
        }
    }

    return nRet;
}

int accept_set_io_context(ACCEPT_THREAD_ITEM *pConfigAcceptItem, IO_THREAD_CONTEXT *pIoThreadContexts)
{
    int  lThreadIndex = 0;
    char  DistriTrans[128] = { 0 };
    cnv_comm_string_trans(pConfigAcceptItem->strDistribution, sizeof(pConfigAcceptItem->strDistribution), ',', DistriTrans);

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

void  accept_thread_uninit(ACCEPT_THREAD_CONTEXT *pAcceptContext)
{
    int  i = 0;
    int Epollfd = pAcceptContext->Epollfd;

    for(i = 0; i < g_params.tConfigAccept.lNumberOfPort; ++i)
    {
        free_unblock_queue(pAcceptContext->pConfigAcceptItem[i].queDistribute);
        cnv_comm_Free(pAcceptContext->pConfigAcceptItem[i].queDistribute);
        if(strcmp(pAcceptContext->pConfigAcceptItem[i].strTransmission, "UNIXSOCKET") == 0)
        {
            unlink(pAcceptContext->pConfigAcceptItem[i].strUnixDomainPath);
        }
    }
    free_unblock_queue(pAcceptContext->queEventfds);
    cnv_comm_Free(pAcceptContext->queEventfds);
    cnv_hashmap_free_socket(pAcceptContext->HashFdListen, &Epollfd);
    netframe_close_socket(pAcceptContext->TcpListenSocket);
    netframe_close_socket(pAcceptContext->UnixListenSocket);
    netframe_close_socket(pAcceptContext->UdpSocket);
    free_accept_lockfreequeue(&(pAcceptContext->statis_msgque));
    lockfree_queue_uninit(&(pAcceptContext->statis_msgque));
    destory_unblock_queue(&(pAcceptContext->queStatisData));
    close(pAcceptContext->accept_eventfd);
    close(pAcceptContext->Epollfd);
}

int  accept_thread_init(NETFRAME_CONFIG_ACCEPT *ptConfigAccept, IO_THREAD_CONTEXT *pIoThreadContexts, ACCEPT_THREAD_CONTEXT *pAcceptContext)
{
    int  nRet = CNV_ERR_OK;
    int  i = 0;
    pAcceptContext->pIoThreadContexts = pIoThreadContexts;
    netframe_create_epoll(&(pAcceptContext->Epollfd), 5);
    pAcceptContext->accept_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    initiate_unblock_queue(&(pAcceptContext->queStatisData), DEFAULT_QUEUE_CAPCITY);   //统计数据返回的消息队列
    lockfree_queue_init(&(pAcceptContext->statis_msgque), 1000);
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

        nRet = accept_thread_run(&g_params.tConfigAccept);
        if(nRet != 0)
        {
            return nRet;
        }
        LOG_SYS_INFO("accept thread start result:%d", nRet);
    }

    return nRet;
}
