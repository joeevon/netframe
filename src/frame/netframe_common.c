#include "netframe_common.h"
#include "cnv_hashmap.h"
#include "log/cnv_liblog4cplus.h"
#include "cnv_xml_parse.h"
#include "netframe_net.h"
#include "cnv_base_define.h"
#include "cnv_comm.h"
#include "cnv_net_define.h"
#include "common_type.h"
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <string.h>
#include <stdlib.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>

extern GLOBAL_PARAMS  g_params;      //全局配置参数

K_BOOL printhashmap(void  *pKey, void  *pValue, void  *pContext)
{
    HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)pValue;
    SOCKET_ELEMENT  *pSocketElement = (SOCKET_ELEMENT *)pHashValue->pValue;
    LOG_SYS_DEBUG("key = %s, socket = %d", (char *)pKey, pSocketElement->Socket);
    return true;
}

int netframe_init_path(char *strConfPath)
{
    //配置文件
    /*snprintf(g_params.tConfigPath.strConfigPath, sizeof(g_params.tConfigPath.strConfigPath) - 1, "%s", strConfPath);
    cnv_comm_StrcatA(g_params.tConfigPath.strConfigPath, "net_frame.xml");*/
    memcpy(g_params.tConfigPath.strConfigPath, strConfPath, sizeof(g_params.tConfigPath.strConfigPath) - 1);

    //日志目录
    /*snprintf(g_params.tConfigPath.strLogDir, sizeof(g_params.tConfigPath.strLogDir) - 1, "%s", strConfPath);
    cnv_comm_StrcatA(g_params.tConfigPath.strLogDir, "../logs");*/

    return CNV_ERR_OK;
}

int netframe_init_config()
{
    int  nRet = 0;
    int  lIndex = 0;
    char  *pItemVaule;        //结点值
    void *pDoc = NULL;
    xmlNodePtr  ptNodeRoot = NULL;  //结点指针
    xmlNodePtr  ptNodeItem = NULL;      //结点指针

    //读取xml文件
    nRet = cnv_comm_xml_loadFile(g_params.tConfigPath.strConfigPath, "UTF-8", &pDoc);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("Document not parsed successfully ! ");
        return nRet;
    }

    ptNodeRoot = xmlDocGetRootElement(pDoc);            //确定文档根元素
    if(xmlStrcmp(ptNodeRoot->name, BAD_CAST "root"))
    {
        LOG_SYS_FATAL("document of the wrong type, root node[%s] != root", ptNodeRoot->name);
        xmlFreeDoc(pDoc);
        return CNV_ERR_CONFIG;
    }

    //=================ACCEPT THREAD=====================
    int nAllowedClientIndex = 0;
    xmlNodePtr  ptNodeBind = NULL;
    xmlNodePtr  ptNodeListen = NULL;
    xmlNodePtr  ptNode = NULL;
    ptNodeBind = ptNodeRoot->children;
    while(ptNodeBind)
    {
        if(!xmlStrcmp(ptNodeBind->name, BAD_CAST"bind"))
        {
            ptNodeListen = ptNodeBind->xmlChildrenNode;
            while(ptNodeListen)
            {
                if(!xmlStrcmp(ptNodeListen->name, BAD_CAST"listen"))
                {
                    ptNode = ptNodeListen->xmlChildrenNode;
                    while(ptNode)
                    {
                        if(!xmlStrcmp(ptNode->name, (const xmlChar *)"protocol"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            snprintf(g_params.tConfigAccept.szConfigAcceptItem[lIndex].tCallback.strProtocol, sizeof(g_params.tConfigAccept.szConfigAcceptItem[lIndex].tCallback.strProtocol) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"host"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            snprintf(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strHost, sizeof(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strHost) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"port"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            g_params.tConfigAccept.szConfigAcceptItem[lIndex].ulPort = atoi(pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"maptype"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            g_params.tConfigAccept.szConfigAcceptItem[lIndex].uMapType = atoi(pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"unixdomainpath"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            snprintf(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strUnixDomainPath, sizeof(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strUnixDomainPath) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"transmission"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            snprintf(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strTransmission, sizeof(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strTransmission) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"distribution"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            snprintf(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strDistribution, sizeof(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strDistribution) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"algorithm"))
                        {
                            pItemVaule = (char *)xmlNodeGetContent(ptNode);
                            snprintf(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strAlgorithm, sizeof(g_params.tConfigAccept.szConfigAcceptItem[lIndex].strAlgorithm) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNode->name, (const xmlChar *)"allowedclients"))
                        {
                            ptNodeItem = ptNode->xmlChildrenNode;
                            while(ptNodeItem)
                            {
                                pItemVaule = (char *)xmlNodeGetContent(ptNodeItem);
                                if(!xmlStrcmp(ptNodeItem->name, BAD_CAST"client"))
                                {
                                    snprintf(g_params.tConfigAccept.szConfigAcceptItem[lIndex].tAllowedClients.tHosts[nAllowedClientIndex].strHost, sizeof(g_params.tConfigAccept.szConfigAcceptItem[lIndex].tAllowedClients.tHosts[nAllowedClientIndex].strHost) - 1, "%s", pItemVaule);

                                    nAllowedClientIndex++;
                                    g_params.tConfigAccept.szConfigAcceptItem[lIndex].tAllowedClients.lNumOfHosts++;
                                }
                                xmlFree(pItemVaule);
                                ptNodeItem = ptNodeItem->next;
                            }
                        }
                        if(ptNodeItem)
                        {
                            xmlFree(pItemVaule);
                        }
                        ptNode = ptNode->next;
                    }
                    lIndex++;
                    g_params.tConfigAccept.lNumberOfPort++;
                }
                ptNodeListen = ptNodeListen->next;
                nAllowedClientIndex = 0;
            }
        }
        ptNodeBind = ptNodeBind->next;
    }
    //========================END OF ACCEPT THREAD=======================

    //========================IO THREADS=========================
    xmlNodePtr  ptNodeIO = NULL;
    xmlNodePtr  ptNodeThread = NULL;
    lIndex = 0;
    ptNodeItem = NULL;

    ptNodeIO = ptNodeRoot->xmlChildrenNode;
    while(ptNodeIO)
    {
        if(!xmlStrcmp(ptNodeIO->name, BAD_CAST"io_thread"))
        {
            ptNodeThread = ptNodeIO->xmlChildrenNode;
            while(ptNodeThread)
            {
                if(!xmlStrcmp(ptNodeThread->name, BAD_CAST"thread"))
                {
                    ptNodeItem = ptNodeThread->xmlChildrenNode;
                    while(ptNodeItem)
                    {
                        pItemVaule = (char *)xmlNodeGetContent(ptNodeItem);

                        if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"name"))
                        {
                            snprintf(g_params.tConfigIO.szConfigIOItem[lIndex].strThreadName, sizeof(g_params.tConfigIO.szConfigIOItem[lIndex].strThreadName) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"isstatistics"))
                        {
                            g_params.tConfigIO.szConfigIOItem[lIndex].nIsStasistics = atoi(pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"distribution"))
                        {
                            snprintf(g_params.tConfigIO.szConfigIOItem[lIndex].strDistribution, sizeof(g_params.tConfigIO.szConfigIOItem[lIndex].strDistribution) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"algorithm"))
                        {
                            snprintf(g_params.tConfigIO.szConfigIOItem[lIndex].strAlgorithm, sizeof(g_params.tConfigIO.szConfigIOItem[lIndex].strAlgorithm) - 1, "%s", pItemVaule);
                        }

                        xmlFree(pItemVaule);
                        ptNodeItem = ptNodeItem->next;
                    }
                    lIndex++;
                    g_params.tConfigIO.lNumberOfThread++;
                }
                ptNodeThread = ptNodeThread->next;
            }
        }
        ptNodeIO = ptNodeIO->next;
    }
    //====================END OF IO THREADS=========================

    //========================HANDLE THREADS=========================
    xmlNodePtr  ptNodeBusiness = NULL;
    xmlNodePtr  ptNodeHandle = NULL;
    lIndex = 0;
    ptNodeItem = NULL;

    ptNodeBusiness = ptNodeRoot->xmlChildrenNode;
    while(ptNodeBusiness)
    {
        if(!xmlStrcmp(ptNodeBusiness->name, BAD_CAST"handle_thread"))
        {
            ptNodeHandle = ptNodeBusiness->xmlChildrenNode;
            while(ptNodeHandle)
            {
                if(!xmlStrcmp(ptNodeHandle->name, BAD_CAST"handle"))
                {
                    ptNodeItem = ptNodeHandle->xmlChildrenNode;
                    while(ptNodeItem)
                    {
                        pItemVaule = (char *)xmlNodeGetContent(ptNodeItem);

                        if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"name"))
                        {
                            snprintf(g_params.tConfigHandle.szConfigHandleItem[lIndex].strThreadName, sizeof(g_params.tConfigHandle.szConfigHandleItem[lIndex].strThreadName) - 1, "%s", pItemVaule);
                        }
                        else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"isstatistics"))
                        {
                            g_params.tConfigHandle.szConfigHandleItem[lIndex].nIsStasistics = atoi(pItemVaule);
                        }

                        xmlFree(pItemVaule);
                        ptNodeItem = ptNodeItem->next;
                    }
                    lIndex++;
                    g_params.tConfigHandle.lNumberOfThread++;
                }
                ptNodeHandle = ptNodeHandle->next;
            }
        }
        ptNodeBusiness = ptNodeBusiness->next;
    }
    //====================END OF HANDLE THREADS======================

    //=====================HEART BEAT============================
    xmlNodePtr  ptNodeHeartBeat = NULL;
    ptNodeItem = NULL;

    ptNodeHeartBeat = ptNodeRoot->xmlChildrenNode;
    while(ptNodeHeartBeat)
    {
        if(!xmlStrcmp(ptNodeHeartBeat->name, BAD_CAST"heartbeat"))
        {
            ptNodeItem = ptNodeHeartBeat->xmlChildrenNode;
            while(ptNodeItem)
            {
                pItemVaule = (char *)xmlNodeGetContent(ptNodeItem);

                if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"valuesec"))
                {
                    g_params.tHeartBeat.value_sec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"valuensec"))
                {
                    g_params.tHeartBeat.value_nsec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"intervalsec"))
                {
                    g_params.tHeartBeat.interval_sec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"intervalnsec"))
                {
                    g_params.tHeartBeat.interval_nsec = atoi(pItemVaule);
                }

                xmlFree(pItemVaule);
                ptNodeItem = ptNodeItem->next;
            }
        }
        ptNodeHeartBeat = ptNodeHeartBeat->next;
    }
    //====================END OF HEART BEAT=======================

    //=====================SOCKET CLEAR============================
    xmlNodePtr  ptNodeSocketClear = NULL;
    ptNodeItem = NULL;

    ptNodeSocketClear = ptNodeRoot->xmlChildrenNode;
    while(ptNodeSocketClear)
    {
        if(!xmlStrcmp(ptNodeSocketClear->name, BAD_CAST"clearsocket"))
        {
            ptNodeItem = ptNodeSocketClear->xmlChildrenNode;
            while(ptNodeItem)
            {
                pItemVaule = (char *)xmlNodeGetContent(ptNodeItem);

                if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"valuesec"))
                {
                    g_params.tSocketClear.value_sec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"valuensec"))
                {
                    g_params.tSocketClear.value_nsec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"intervalsec"))
                {
                    g_params.tSocketClear.interval_sec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"intervalnsec"))
                {
                    g_params.tSocketClear.interval_nsec = atoi(pItemVaule);
                }

                xmlFree(pItemVaule);
                ptNodeItem = ptNodeItem->next;
            }
        }
        ptNodeSocketClear = ptNodeSocketClear->next;
    }
    //====================END OF SOCKET CLEAR=======================

    //=========================MONITOR==============================
    xmlNodePtr  ptNodeMonitor = NULL;
    ptNodeItem = NULL;

    ptNodeMonitor = ptNodeRoot->xmlChildrenNode;
    while(ptNodeMonitor)
    {
        if(!xmlStrcmp(ptNodeMonitor->name, BAD_CAST"monitor"))
        {
            ptNodeItem = ptNodeMonitor->xmlChildrenNode;
            while(ptNodeItem)
            {
                pItemVaule = (char *)xmlNodeGetContent(ptNodeItem);

                if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"valuesec"))
                {
                    g_params.tMonitor.value_sec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"valuensec"))
                {
                    g_params.tMonitor.value_nsec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"intervalsec"))
                {
                    g_params.tMonitor.interval_sec = atoi(pItemVaule);
                }
                else if(!xmlStrcmp(ptNodeItem->name, (const xmlChar *)"intervalnsec"))
                {
                    g_params.tMonitor.interval_nsec = atoi(pItemVaule);
                }

                xmlFree(pItemVaule);
                ptNodeItem = ptNodeItem->next;
            }
        }
        ptNodeMonitor = ptNodeMonitor->next;
    }
    //====================END OF MONITOR=======================

    //=====================HANDLEIOMSGSIZE============================
    char nodeValue[128] = { 0 };
    char errMsg[64] = { 0 };

    nRet = cnv_comm_xml_GetValue_ByPath(pDoc, "/root/handleiomsgsize", nodeValue, sizeof(nodeValue), errMsg, sizeof(errMsg));
    if(nRet != 0)
    {
        LOG_SYS_FATAL("read config node /root/handleiomsgsize value failed :%s,errcode:%d", errMsg, nRet);
        xmlFreeDoc(pDoc);
        return nRet;
    }
    g_params.tConfigIO.lHandleIoMsgSize = atoi(nodeValue);
    //====================HANDLEIOMSGSIZE==============================

    //=====================IOHANDLEMSGSIZE============================
    bzero(nodeValue, sizeof(nodeValue));
    bzero(errMsg, sizeof(errMsg));

    nRet = cnv_comm_xml_GetValue_ByPath(pDoc, "/root/iohandlemsgsize", nodeValue, sizeof(nodeValue), errMsg, sizeof(errMsg));
    if(nRet  != 0)
    {
        LOG_SYS_FATAL("read config node /root/iohandlemsgsize value failed :%s,errcode:%d", errMsg, nRet);
        xmlFreeDoc(pDoc);
        return nRet;
    }
    g_params.tConfigHandle.lIoHandleMsgSize = atoi(nodeValue);
    //====================IOHANDLEMSGSIZE==============================

    xmlFreeDoc(pDoc);
    return CNV_ERR_OK;
}

int  netframe_init_timer(int Epollfd, int  timerfd, TIMER_STRUCT  *ptTimer)
{
    int  nRet = CNV_ERR_OK;
    struct itimerspec tTimerspec = {{0}, {0}};

    nRet = netframe_add_readevent(Epollfd, timerfd, NULL);
    if(nRet != CNV_ERR_OK)
    {
        return  nRet;
    }

    tTimerspec.it_value.tv_sec = ptTimer->value_sec;
    tTimerspec.it_value.tv_nsec = ptTimer->value_nsec;
    tTimerspec.it_interval.tv_sec = ptTimer->interval_sec;
    tTimerspec.it_interval.tv_nsec = ptTimer->interval_nsec;
    timerfd_settime(timerfd, 0, &tTimerspec, NULL);

    return  CNV_ERR_OK;
}

K_BOOL hashmap_earase_socket(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase)
{
    int Epollfd = *(int *)pContext;
    int Socket = *(int *)pKey;
    netframe_delete_event(Epollfd, Socket);
    netframe_close_socket(Socket);

    cnv_comm_Free(pKey);
    HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)pValue;
    cnv_comm_Free(pHashValue->pValue);
    cnv_comm_Free(pValue);

    *bIsEarase = true;
    return  true;
}

K_BOOL hashmap_earase_callback(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase)
{
    cnv_comm_Free(pKey);
    HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)pValue;
    cnv_comm_Free(pHashValue->pValue);
    cnv_comm_Free(pValue);

    *bIsEarase = true;
    return true;
}

int netframe_long_connect_(IO_THREAD_CONTEXT *pIoThreadContext, SERVER_SOCKET_DATA *pSvrSockData)
{
    if(cnv_comm_get_utctime() - pIoThreadContext->nLastConnectTime > pSvrSockData->nReconIntercal || pIoThreadContext->nReconTimes == 0)
    {
        pIoThreadContext->nReconTimes = 0;
        pIoThreadContext->nLastConnectTime = cnv_comm_get_utctime();
    }

    if(pIoThreadContext->nReconTimes <= pSvrSockData->nMaxReconTimes)
    {
        pIoThreadContext->nReconTimes++;
        int nSocket = 0, nRet = -1;
        int nEveryTimeOut = 300, nReconTimes = 1;

        do
        {
            nRet = netframe_tcp_connect(&nSocket, pSvrSockData->strServerIp, pSvrSockData->nPort, nEveryTimeOut);    //创建连接
            nEveryTimeOut *= 2;
        }
        while(nRet != CNV_ERR_OK && nReconTimes++ < 5);

        if(nRet != CNV_ERR_OK)
        {
            return -1;
        }

        nRet = hash_add_addrsocket(nSocket, pSvrSockData, pIoThreadContext->HashAddrFd); //服务端hashmap
        if(nRet != CNV_ERR_OK)
        {
            LOG_SYS_ERROR("hash_add_addrsocket failed, ip:%s, port:%d", pSvrSockData->strServerIp, pSvrSockData->nPort);
            netframe_close_socket(nSocket);
            return -1;
        }

        //if(pSvrSockData->isRecvSvrData == 1)   //需要接收服务端数据
        //{
        //    nRet = hash_add_conidfd(nSocket, pSvrSockData, pIoThreadContext);  //客户端hashmap
        //    if(nRet != CNV_ERR_OK)
        //    {
        //        LOG_SYS_ERROR("hash_add_conidfd failed, ip:%s, port:%d", pSvrSockData->strServerIp, pSvrSockData->nPort);
        //        netframe_close_socket(nSocket);
        //        return -1;
        //    }
        //}

        //if(pSvrSockData->isReqLogin == 1)      //是否发送登录验证
        //{
        //    nRet = netframe_req_login(nSocket, pSvrSockData);
        //    if(nRet != CNV_ERR_OK)
        //    {
        //        LOG_SYS_ERROR("netframe_req_login failed!");
        //        exit(-1);
        //    }
        //}
    }
    else
    {
        return -1;
    }

    return CNV_ERR_OK;
}

K_BOOL  hashmap_iterator_socketclear(void  *in_pKey, void  *pValue, void *socketclear)
{
    CNV_UNBLOCKING_QUEUE  *queuesocketclear = (CNV_UNBLOCKING_QUEUE *)socketclear;
    SOCKET_ELEMENT  *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pValue)->pValue);
    if(pSocketElement->bIsToclear && pSocketElement->lAction == 2 && pSocketElement->Time + g_params.tSocketClear.interval_sec <= cnv_comm_get_utctime())    //上一次收发数据与当前相比大于心跳间隔,须发心跳包
    {
        push_unblock_queue_tail(queuesocketclear, in_pKey);
    }
    return 1;  // 返回0时hashmap里迭代会中断,需返回非0值
}

int  netframe_heart_beat(int timerfd_hearbeat, IO_THREAD_CONTEXT *pIoThreadContext)
{
    LOG_SYS_DEBUG("netframe_heart_beat.");

    if(pIoThreadContext->queServer && get_unblock_queue_count(pIoThreadContext->queServer) > 0)
    {
        struct queue_entry_t *queuenode = get_unblock_queue_first(pIoThreadContext->queServer);
        while(queuenode && queuenode->data_)
        {
            SERVER_SOCKET_DATA *pSvrSockData = (SERVER_SOCKET_DATA *)queuenode->data_;
            char strKey[32] = "";
            snprintf(strKey, sizeof(strKey) - 1, "%s", pSvrSockData->strServerIp);
            cnv_comm_StrcatA(strKey, ":");
            char  strPort[10] = { 0 };
            snprintf(strPort, sizeof(strPort) - 1, "%d", pSvrSockData->nPort);
            cnv_comm_StrcatA(strKey, strPort);

            void *pOutValue = NULL;
            int nRet = cnv_hashmap_get(pIoThreadContext->HashAddrFd, strKey, &pOutValue);
            if(nRet == K_SUCCEED)   //能找得到连接而且心跳包数据不为空
            {
                if(pSvrSockData->pHeartBeat != NULL && pSvrSockData->nHeartBeatLen > 0)
                {
                    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);
                    nRet = netframe_write(pSocketElement->Socket, pSvrSockData->pHeartBeat, pSvrSockData->nHeartBeatLen, NULL);
                    if(nRet == AGENT_NET_CONNECTION_ABNORMAL)
                    {
                        netframe_reconnect_server(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData, pIoThreadContext);
                    }
                }
            }
            else  //没有连接信息了,直接重连
            {
                netframe_long_connect_(pIoThreadContext, pSvrSockData);
            }

            queuenode = get_unblock_queue_next(queuenode);
        }
    }

    uint64_t ulData = 0;
    read(timerfd_hearbeat, &ulData, sizeof(uint64_t));   //此数无用,读出缓存消息,避免epoll重复提醒
    LOG_SYS_DEBUG("netframe_heart_beat end.");
    return  CNV_ERR_OK;
}

int  netframe_socket_clear(int Epollfd, int fd, void *HashConnidFd)
{
    LOG_SYS_DEBUG("netframe_socket_clear.");
    int  nRet = CNV_ERR_OK;
    uint64_t  ulData = 0;
    CNV_UNBLOCKING_QUEUE queuesocketclear;

    initiate_unblock_queue(&queuesocketclear, DEFAULT_QUEUE_CAPCITY);
    cnv_hashmap_iterator(HashConnidFd, hashmap_iterator_socketclear, (void *)&queuesocketclear);
    int  lSizeOfQueue = get_unblock_queue_count(&queuesocketclear);
    LOG_SYS_DEBUG("size of clear:%d", lSizeOfQueue);
    while(lSizeOfQueue--)
    {
        char *pKey = (char *)poll_unblock_queue_head(&queuesocketclear);
        remove_client_socket_hashmap(Epollfd, HashConnidFd, pKey);
    }

    nRet = read(fd, &ulData, sizeof(uint64_t));   //此数无用,读出缓存消息,避免epoll重复提醒
    if(nRet != sizeof(uint64_t))
    {
        LOG_SYS_FATAL("socket clear read eventfd failed !");
    }
    LOG_SYS_DEBUG("netframe_socket_clear end.");
    return  CNV_ERR_OK;
}

int cnv_parse_distribution(char *strAlgorithm, char *strDistribution, CNV_UNBLOCKING_QUEUE *queDistribute)
{
    int  i = 1;
    char  DistriTrans[32] = {0};
    cnv_comm_string_trans(strDistribution, sizeof(DistriTrans), ',', DistriTrans);

    if(!strcmp(strAlgorithm, "0") || !strcmp(strAlgorithm, ""))   //algorithm为0或没有值,即平均轮询
    {
        char  *pDistribution = strtok(DistriTrans, ",");     //挂一个或多个线程都可用此方法赋值
        while(pDistribution)
        {
            char  *pValue = (char *)cnv_comm_Malloc(sizeof(int) + 1);
            if(!pValue)
            {
                return  CNV_ERR_MALLOC;
            }
            snprintf(pValue, sizeof(int) - 1, "%s", pDistribution);
            push_unblock_queue_tail(queDistribute, pValue);
            pDistribution = strtok(NULL, ",");
        }
    }
    else       //将对应的线程号加入队列比例次数(如, T1:T2:T3 = 2:3:1,则1入队2次,2入队3次,3入队1次)
    {
        char *pAlgoTmp = NULL;
        char *pDistriTmp = NULL;
        char *pAlgorithm = strtok_r(strAlgorithm, ":", &pAlgoTmp);
        char *pDistribution = strtok_r(DistriTrans, ",", &pDistriTmp);
        while(pAlgorithm)
        {
            while(pDistribution)
            {
                char  *pIndex = (char *)cnv_comm_Malloc(sizeof(int) + 1);
                if(!pIndex)
                {
                    return  CNV_ERR_MALLOC;
                }
                snprintf(pIndex, sizeof(int) - 1, "%s", pDistribution);
                i = atoi(pAlgorithm);
                while(i--)
                {
                    push_unblock_queue_tail(queDistribute, pIndex);
                }
                break;
            }
            pAlgorithm = strtok_r(NULL, ":", &pAlgoTmp);
            pDistribution = strtok_r(NULL, ",", &pDistriTmp);
        }
    }

    return  CNV_ERR_OK;
}

int  netframe_get_hashkey(void *Hashmap, int *pSeedOfKey)
{
    int  lSeedOfKey = *pSeedOfKey;
    K_BOOL  bisExist = true;

    while(bisExist)      //key值已存在,重新构造key值
    {
        lSeedOfKey++;
        if(lSeedOfKey >= MAX_INT_VALUE)        //达到最大值，从0开始
        {
            lSeedOfKey = 1;
        }
        char  strKey[33] = "";
        snprintf(strKey, sizeof(strKey) - 1, "%d", lSeedOfKey);
        bisExist = cnv_hashmap_containsKey(Hashmap, strKey);
    }
    *pSeedOfKey = lSeedOfKey;

    return  lSeedOfKey;
}

//发送登录请求
int netframe_req_login(int Socket, SERVER_SOCKET_DATA *pSvrSockData)
{
    int nRet = CNV_ERR_OK;
    nRet = netframe_write(Socket, pSvrSockData->strLoginReqInfo, strlen(pSvrSockData->strLoginReqInfo), 0);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("netframe_req_login failed!");
        return nRet;
    }

    return CNV_ERR_OK;
}

int  hash_add_addrsocket(int Socket, SERVER_SOCKET_DATA *pSvrSockData, void *HashAddrFd)
{
    char  strPort[10] = "";

    char *pKey = (char *)cnv_comm_Malloc(32);
    if(!pKey)
    {
        return  CNV_ERR_MALLOC;
    }
    memset(pKey, 0x00, 32);
    snprintf(pKey, 31, "%s", pSvrSockData->strServerIp);
    cnv_comm_StrcatA(pKey, ":");
    snprintf(strPort, 9, "%d", pSvrSockData->nPort);
    cnv_comm_StrcatA(pKey, strPort);

    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)cnv_comm_Malloc(sizeof(SOCKET_ELEMENT));
    if(!pSocketElement)
    {
        cnv_comm_Free(pKey);
        return  CNV_ERR_MALLOC;
    }
    memset(pSocketElement, 0x00, sizeof(SOCKET_ELEMENT));
    pSocketElement->pConnId = pKey;
    pSocketElement->Socket = Socket;
    pSocketElement->Time = cnv_comm_get_utctime();
    SERVER_SOCKET_ELEMENT *pSvrSockElement = &(pSocketElement->uSockElement.tSvrSockElement);
    pSvrSockElement->msg.msg_name = &(pSvrSockElement->tServerAddr);
    pSvrSockElement->msg.msg_namelen = sizeof(struct sockaddr_in);
    pSvrSockElement->msg.msg_iov = pSvrSockElement->szIovecSvrData;
    pSvrSockElement->msg.msg_iovlen = 2;
    pSvrSockElement->pSvrSockData = pSvrSockData;
    HASHMAP_VALUE *pHashVaule = (HASHMAP_VALUE *)cnv_comm_Malloc(sizeof(HASHMAP_VALUE));
    if(!pHashVaule)
    {
        cnv_comm_Free(pKey);
        cnv_comm_Free(pSocketElement);
        return  CNV_ERR_MALLOC;
    }
    pHashVaule->lSize = sizeof(SOCKET_ELEMENT);
    pHashVaule->pValue = (char *)pSocketElement;
    cnv_hashmap_put(HashAddrFd, pKey, pHashVaule, NULL);

    return  CNV_ERR_OK;
}

int hash_add_conidfd(int Socket, SERVER_SOCKET_DATA *pSvrSockData, IO_THREAD_CONTEXT *pIoThreadContext)
{
    int nRet = CNV_ERR_OK;
    void *pOldValue = NULL;

    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)malloc(sizeof(SOCKET_ELEMENT));
    if(!pSocketElement)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pSocketElement, 0x00, sizeof(SOCKET_ELEMENT));
    pSocketElement->Socket = Socket;
    pSocketElement->Time = cnv_comm_get_utctime();
    pSocketElement->bIsToclear = false;

    pSocketElement->uSockElement.tClnSockElement.msg.msg_name = &(pSocketElement->uSockElement.tClnSockElement.tClientAddr);
    pSocketElement->uSockElement.tClnSockElement.msg.msg_namelen = sizeof(struct sockaddr_in);
    pSocketElement->uSockElement.tClnSockElement.msg.msg_iov = &(pSocketElement->uSockElement.tClnSockElement.tIovecClnData);
    pSocketElement->uSockElement.tClnSockElement.msg.msg_iovlen = 1;
    pSocketElement->uSockElement.tClnSockElement.msg.msg_control = pSocketElement->uSockElement.tClnSockElement.strControl;
    pSocketElement->uSockElement.tClnSockElement.msg.msg_controllen = sizeof(pSocketElement->uSockElement.tClnSockElement.strControl);
    set_callback_function(SERVER_CALLBACK_FUNC, &(pSvrSockData->tCallback));
    snprintf(pSocketElement->uSockElement.tClnSockElement.strServiceName, sizeof(pSocketElement->uSockElement.tClnSockElement.strServiceName) - 1, "%s", pSvrSockData->strServiceName);
    pSocketElement->uSockElement.tClnSockElement.pfncnv_parse_protocol = pSvrSockData->tCallback.pfncnv_parse_protocol;
    pSocketElement->uSockElement.tClnSockElement.pfncnv_handle_business = pSvrSockData->tCallback.pfncnv_handle_business;

    if(pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer == NULL)     //接收数据缓存
    {
        pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer = (char *)malloc(g_params.nMaxBufferSize);
        if(pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer == NULL)
        {
            return  CNV_ERR_MALLOC;
        }
    }

    int ConnId = netframe_get_hashkey(pIoThreadContext->HashConnidFd, &(pIoThreadContext->SeedOfKey));
    char *pKey = (char *)malloc(64);
    if(!pKey)
    {
        cnv_comm_Free(pSocketElement);
        return CNV_ERR_MALLOC;
    }
    memset(pKey, 0, 64);
    snprintf(pKey, 63, "%d", ConnId);
    pSocketElement->pConnId = pKey;
    LOG_SYS_DEBUG("ConnId = %d, Socket :%d", ConnId, Socket);

    HASHMAP_VALUE  *pHashValue = (HASHMAP_VALUE *)malloc(sizeof(HASHMAP_VALUE));
    if(!pHashValue)
    {
        cnv_comm_Free(pSocketElement);
        cnv_comm_Free(pKey);
        return CNV_ERR_MALLOC;
    }
    pHashValue->lSize = sizeof(HASHMAP_VALUE);
    pHashValue->pValue = (char *)pSocketElement;
    nRet = cnv_hashmap_put(pIoThreadContext->HashConnidFd, pKey, pHashValue, &pOldValue);
    if(nRet != K_SUCCEED)
    {
        LOG_SYS_DEBUG("cnv_hashmap_put failed!");
    }
    else
    {
        if(pOldValue)
        {
            LOG_SYS_FATAL("threadid:%d,hashmap already exists!", pIoThreadContext->threadindex);
            cnv_comm_Free(pOldValue);
        }

        nRet = netframe_add_readevent(pIoThreadContext->Epollfd, Socket, pKey);  //把客户端连接句柄加入读监听
        if(nRet != CNV_ERR_OK)
        {
            LOG_SYS_ERROR("netframe_add_readevent failed!");
            remove_client_socket_hashmap(pIoThreadContext->Epollfd, pIoThreadContext->HashConnidFd, pKey);
        }
    }

    return nRet;
}

int  netframe_long_connect(IO_THREAD_CONTEXT *pIoThreadContext, CNV_UNBLOCKING_QUEUE *queServer)
{
    if(queServer && get_unblock_queue_count(queServer) > 0)  //遍历
    {
        struct queue_entry_t  *queuenode = get_unblock_queue_first(queServer);
        while(queuenode)
        {
            SERVER_SOCKET_DATA *pSvrSockData = (SERVER_SOCKET_DATA *)queuenode->data_;

            //check
            if(pSvrSockData->nReconIntercal <= 0 || pSvrSockData->nReconIntercal > 300)
            {
                LOG_SYS_ERROR("ReconIntercal is illegal,%d, server type:%s , set it default 10s.", pSvrSockData->nReconIntercal, pSvrSockData->strServiceName);
                pSvrSockData->nReconIntercal = 10;
            }

            if(pSvrSockData->nMaxReconTimes <= 0 || pSvrSockData->nMaxReconTimes > 10)
            {
                LOG_SYS_ERROR("MaxReconTimes is illegal,%d, server type:%s , set it default 3 times.", pSvrSockData->nMaxReconTimes, pSvrSockData->strServiceName);
                pSvrSockData->nMaxReconTimes = 3;
            }

            netframe_long_connect_(pIoThreadContext, pSvrSockData);
            queuenode = get_unblock_queue_next(queuenode);
        }
    }

    return  CNV_ERR_OK;
}

int netframe_reconnect_server(SERVER_SOCKET_DATA *pSvrSockData, IO_THREAD_CONTEXT *pIoThreadContext)
{
    char strHashKey[64] = { 0 };
    snprintf(strHashKey, sizeof(strHashKey) - 1, "%s", pSvrSockData->strServerIp);
    cnv_comm_StrcatA(strHashKey, ":");
    char strPort[10] = { 0 };
    snprintf(strPort, sizeof(strPort) - 1, "%d", pSvrSockData->nPort);
    cnv_comm_StrcatA(strHashKey, strPort);
    remove_server_socket_hashmap(pIoThreadContext->Epollfd, pIoThreadContext->HashAddrFd, strHashKey);

    return netframe_long_connect_(pIoThreadContext, pSvrSockData);
}

int  get_current_hashkey(IO_THREAD_CONTEXT  *pIoThreadContext)
{
    return  pIoThreadContext->SeedOfKey;
}

void add_timer(void *HashTimerAdd, int Epollfd, char *pKey, int nInterval, pfnCNV_TIMER_CALLBACK pfncb, void *arg)
{
    char *pHashKey = (char *)malloc(128);
    assert(pHashKey);
    memset(pHashKey, 0, 128);
    snprintf(pHashKey, 127, "%s", pKey);

    TIMER_TASK_STRUCT *ptTimerStr = (TIMER_TASK_STRUCT *)malloc(sizeof(TIMER_TASK_STRUCT));
    assert(ptTimerStr);
    ptTimerStr->pfnTIMER_CALLBACK = pfncb;
    ptTimerStr->timerfd = timerfd_create(CLOCK_REALTIME, 0);
    ptTimerStr->arg = arg;

    HASHMAP_VALUE *pHashValue = (HASHMAP_VALUE *)malloc(sizeof(HASHMAP_VALUE));
    assert(pHashValue);
    pHashValue->pKey = pHashKey;
    pHashValue->pValue = (char *)ptTimerStr;

    cnv_hashmap_put(HashTimerAdd, pHashKey, pHashValue, NULL);

    int nRet = netframe_setblockopt(ptTimerStr->timerfd, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("netframe_setblockopt failed!");
        return;
    }

    nRet = netframe_add_readevent(Epollfd, ptTimerStr->timerfd, pHashKey);
    if(nRet != CNV_ERR_OK)
    {
        return;
    }

    struct itimerspec tTimerspec = { { 0 }, { 0 } };
    tTimerspec.it_value.tv_sec = nInterval;
    timerfd_settime(ptTimerStr->timerfd, 0, &tTimerspec, NULL);
}

K_BOOL queue_search_int(void *nodedata, void *searchdata)
{
    int a = *((int *) nodedata);
    int b = *((int *) searchdata);
    return a == b;
}

K_BOOL queue_search_string(void *nodedata, void *searchdata)
{
    char *a = (char *)nodedata;
    char *b = (char *)searchdata;
    return strcmp(a, b);
}

void  remove_client_socket_hashmap(int Epollfd, void *HashConnidFd, void *pKey)
{
    void *pOutValue = NULL;
    if(cnv_hashmap_get(HashConnidFd, pKey, &pOutValue) != K_SUCCEED)    //已被删除
    {
        return;
    }
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);

    cnv_hashmap_remove(HashConnidFd, pKey, NULL);
    netframe_delete_event(Epollfd, pSocketElement->Socket);
    netframe_close_socket(pSocketElement->Socket);
    cnv_comm_Free(pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer);
    cnv_comm_Free(pSocketElement->pConnId);  //hashkey
    cnv_comm_Free(pSocketElement);
    cnv_comm_Free(pOutValue);  //hashvalue
}

void  remove_server_socket_hashmap(int Epollfd, void *HashAddrFd, void *pKey)
{
    void *pOutValue = NULL;
    if(cnv_hashmap_get(HashAddrFd, pKey, &pOutValue) != K_SUCCEED)     //已被删除
    {
        return;
    }
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pOutValue)->pValue);

    cnv_hashmap_remove(HashAddrFd, pKey, NULL);
    netframe_close_socket(pSocketElement->Socket);
    /*if(strcmp(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strProtocol, "UNIXSOCKET") == 0)
    {
        unlink(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strUnixDomainPath);
    }*/
    cnv_comm_Free(pSocketElement->pConnId);   //hashkey
    cnv_comm_Free(pSocketElement);
    cnv_comm_Free(pOutValue);  //hash value
}

void  cnv_hashmap_free_socket(void *Hashmap, void *Epollfd)
{
    cnv_hashmap_erase(Hashmap, hashmap_earase_socket, Epollfd);
    cnv_hashmap_uninit(Hashmap);
}

K_BOOL earase_client_socket_hashmap(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase)
{
    int  Epollfd = *(int *)pContext;

    cnv_comm_Free(pKey);   //hashkey
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pValue)->pValue);
    netframe_delete_event(Epollfd, pSocketElement->Socket);
    netframe_close_socket(pSocketElement->Socket);
    cnv_comm_Free(pSocketElement->uSockElement.tClnSockElement.SocketData.pDataBuffer);
    cnv_comm_Free(pSocketElement);
    cnv_comm_Free(pValue);  //hash value

    *bIsEarase = true;
    return  true;
}

K_BOOL earase_server_socket_hashmap(void *pKey, void *pValue, void *pContext, K_BOOL *bIsEarase)
{
    cnv_comm_Free(pKey);
    SOCKET_ELEMENT *pSocketElement = (SOCKET_ELEMENT *)(((HASHMAP_VALUE *)pValue)->pValue);
    netframe_close_socket(pSocketElement->Socket);
    /*if(strcmp(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strProtocol, "UNIXSOCKET") == 0)
    {
        unlink(pSocketElement->uSockElement.tSvrSockElement.pSvrSockData->strUnixDomainPath);
    }*/
    cnv_comm_Free(pSocketElement);
    cnv_comm_Free(pValue);

    *bIsEarase = true;
    return  true;
}

void free_unblock_queue(CNV_UNBLOCKING_QUEUE *queue)
{
    void  *pOutqueue = NULL;
    int  lCount = get_unblock_queue_count(queue);
    while(lCount--)
    {
        pOutqueue = poll_unblock_queue_head(queue);
        cnv_comm_Free(pOutqueue);
    }
}

void free_handleio_unblockqueue(CNV_UNBLOCKING_QUEUE *unblock_queue)
{
    HANDLE_TO_IO_DATA *HandleIoData = NULL;
    int lCount = get_unblock_queue_count(unblock_queue);
    while(lCount--)
    {
        HandleIoData = (HANDLE_TO_IO_DATA *)poll_unblock_queue_head(unblock_queue);
        cnv_comm_Free(HandleIoData->pDataSend);
        cnv_comm_Free(HandleIoData);
    }
}

K_BOOL  printDistribution(void *nodedata, void *in_pContext)
{
    LOG_SYS_DEBUG("%s", ((char *)nodedata));
    return true;
}

K_BOOL  printAllowedClients(void *nodedata, void *in_pContext)
{
    LOG_SYS_DEBUG("Allowed Client:%s", ((char *)nodedata));
    return true;
}
