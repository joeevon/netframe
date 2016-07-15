/****************************
    FileName:cnv_position.c
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        cnv_position  C文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-19
*****************************/

#include "agent.h"
#include "cnv_comm.h"
#include "cnv_liblog4cplus.h"
#include "cnv_xml_parse.h"
#include "netframe_main.h"
#include "netframe_net.h"
#include <string.h>

static  POSITION_PARAMS  g_tPosiParams;

int cnv_init_agent(char *strConfPath, CNV_UNBLOCKING_QUEUE  *queueConfig)
{
    //LOG_SYS_DEBUG("cnv_init_agent");
    int lRet = CNV_ERR_OK;
    char strMsg[MAX_PATH] = "";
    memset(&g_tPosiParams, 0, sizeof(g_tPosiParams));

    lRet = agent_init_path(strConfPath);       //初始化配置文件路径
    if(lRet != CNV_ERR_OK)
    {
        return  lRet;
    }

    set_config(g_tPosiParams.tParamConfigPath.strLog, strMsg, sizeof(strMsg));
    LOG_SYS_DEBUG("position path: %s",g_tPosiParams.tParamConfigPath.strPosition);

    lRet = agent_init_config();    //加载配置文件
    if(lRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("agent_init_config failed!");
        return  lRet;
    }

    lRet = init_svrconf_queue(queueConfig);  //把配置加到队列里
    if(lRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("init_svrconf_queue failed!");
        return  lRet;
    }

    LOG_SYS_DEBUG("cnv_init_agent end");
    return CNV_ERR_OK;
}

int agent_init_path(char *strConfPath)
{
    //位置服务
    snprintf(g_tPosiParams.tParamConfigPath.strPosition, sizeof(g_tPosiParams.tParamConfigPath.strPosition)-1, "%s", strConfPath);
    cnv_comm_StrcatA(g_tPosiParams.tParamConfigPath.strPosition, "position.xml");

    //日志配置
    snprintf(g_tPosiParams.tParamConfigPath.strLog, sizeof(g_tPosiParams.tParamConfigPath.strLog)-1, "%s", strConfPath);
    cnv_comm_StrcatA(g_tPosiParams.tParamConfigPath.strLog, "urconfig.properties");

    return CNV_ERR_OK;
}

extern int agent_init_config()
{
    int  lRet = CNV_ERR_OK;

    //初始服务端配置
    lRet = agent_init_server();
    if(lRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("agent_init_server failed !");
        return lRet;
    }

    return  CNV_ERR_OK;
}

int init_svrconf_queue(CNV_UNBLOCKING_QUEUE  *queueConfig)
{
    int lIndex = 0;
    int lReqHeaderOffset = sizeof(NAVI_DATA_REQUEST_HEADER);

    char  *pHeartBeat = (char *)cnv_comm_Malloc(lReqHeaderOffset);  //  内存有框架释放
    if(!pHeartBeat)
    {
        return  CNV_ERR_MALLOC;
    }

    NAVI_DATA_REQUEST_HEADER *pReqHeader = (NAVI_DATA_REQUEST_HEADER *)pHeartBeat;
    pReqHeader->BusinessCode = HEART_BEAT_CODE;
    pReqHeader->DataOffset = sizeof(NAVI_DATA_REQUEST_HEADER);

    for(lIndex = 0; lIndex < g_tPosiParams.tConfigPosition.lNumberOfBalanceItem; lIndex++)
    {
        SERVER_SOCKET_DATA  *pSvrSockData = (SERVER_SOCKET_DATA *)cnv_comm_Malloc(sizeof(SERVER_SOCKET_DATA));
        if(!pSvrSockData)
        {
            return CNV_ERR_MALLOC;
        }
        snprintf(pSvrSockData->strServerIp, sizeof(pSvrSockData->strServerIp) - 1, "%s", g_tPosiParams.tConfigPosition.szConfigBalanceItem[lIndex].strServIp);
        pSvrSockData->lPort = g_tPosiParams.tConfigPosition.szConfigBalanceItem[lIndex].ulPort;
        pSvrSockData->pHeartBeat = pHeartBeat;
        pSvrSockData->lHeartBeatLen = lReqHeaderOffset;
        strncpy(pSvrSockData->strServiceName, "POSITION" ,sizeof(pSvrSockData->strServiceName) - 1);

        push_unblock_queue_tail(queueConfig, pSvrSockData);
    }

    return  CNV_ERR_OK;
}

int agent_init_server()
{
    int  lRet = CNV_ERR_OK;
    int  lIndex = 0;
    void  *pDoc = NULL;
    char  *pItemVaule = NULL;        //结点值
    xmlNodePtr  ptNodePosition = NULL;      //结点指针
    xmlNodePtr  ptNodeBalance = NULL;
    xmlNodePtr  ptNodeItem = NULL;

    lRet = cnv_comm_xml_loadFile(g_tPosiParams.tParamConfigPath.strPosition,"UTF-8",&pDoc);  //解析文件
    if(lRet != CNV_ERR_OK)
    {
        LOG_SYS_FATAL("cnv_comm_xml_loadFile[%s] result:%d",g_tPosiParams.tParamConfigPath.strPosition,lRet);
        return lRet;
    }

    ptNodePosition = xmlDocGetRootElement(pDoc);  //确定文档根元素
    if(xmlStrcmp(ptNodePosition->name, BAD_CAST "position"))
    {
        LOG_SYS_FATAL("document of the wrong type, root node[%s] != balance", ptNodePosition->name);
        xmlFreeDoc(pDoc);
        return CNV_ERR_CONFIG;
    }

    //清空配置
    memset(&g_tPosiParams.tConfigPosition, 0x00, sizeof(NAVI_BALANCE_PARAM));

    ptNodeBalance = ptNodePosition->xmlChildrenNode;
    while(ptNodeBalance)
    {
        if(!xmlStrcmp(ptNodeBalance->name, BAD_CAST"balance"))
        {
            ptNodeItem = ptNodeBalance->xmlChildrenNode;
            while(ptNodeItem)
            {
                pItemVaule = (char *)xmlNodeGetContent(ptNodeItem);

                if(!xmlStrcmp(ptNodeItem->name, BAD_CAST"ip"))
                {
                    snprintf(g_tPosiParams.tConfigPosition.szConfigBalanceItem[lIndex].strServIp,
                             sizeof(g_tPosiParams.tConfigPosition.szConfigBalanceItem[lIndex].strServIp) - 1, "%s", pItemVaule);
                }
                if(!xmlStrcmp(ptNodeItem->name, BAD_CAST"port"))
                {
                    g_tPosiParams.tConfigPosition.szConfigBalanceItem[lIndex].ulPort = atoi(pItemVaule);
                }

                xmlFree(pItemVaule);
                ptNodeItem = ptNodeItem->next;
            }
            lIndex++;
            g_tPosiParams.tConfigPosition.lNumberOfBalanceItem++;
        }
        ptNodeBalance = ptNodeBalance->next;
    }

    xmlFreeDoc(pDoc);
    return CNV_ERR_OK;
}

int cnv_parse_client(char **pDataBuff, unsigned int *nDataSize, char **pPacket, unsigned int  *nPacketSize, void **auxiliary)
{
    if(*nDataSize < POSITION_PACKAGE_SIZE)    //接收长度小于一个完整的包长度
    {
        LOG_SYS_DEBUG("imcompleted data.");
        return CNV_PARSE_FINISH;
    }

    char  *pDataTmp = (char *)cnv_comm_Malloc(POSITION_PACKAGE_SIZE);
    if(!pDataTmp)
    {
        return CNV_PARSE_ERROR;
    }
    memcpy(pDataTmp, *pDataBuff, POSITION_PACKAGE_SIZE);
    *pPacket = pDataTmp;
    *nPacketSize = POSITION_PACKAGE_SIZE;

    return  CNV_PARSE_SUCCESS;
}

void cnv_handle_clientdata(const IO_TO_HANDLE_DATA *req, CNV_UNBLOCKING_QUEUE *queuerespond)
{
    LOG_SYS_DEBUG("cnv_handle_clientdata");

    HANDLE_TO_IO_DATA  *pHandletoIOData = (HANDLE_TO_IO_DATA *)cnv_comm_Malloc(sizeof(HANDLE_TO_IO_DATA));
    if(!pHandletoIOData)
    {
        return;
    }
    memset(pHandletoIOData, 0x00, sizeof(HANDLE_TO_IO_DATA));

    snprintf(pHandletoIOData->strServIp, sizeof(pHandletoIOData->strServIp) - 1, "%s", g_tPosiParams.tConfigPosition.szConfigBalanceItem[0].strServIp);
    pHandletoIOData->ulPort = g_tPosiParams.tConfigPosition.szConfigBalanceItem[0].ulPort;
    pHandletoIOData->lAction = REQUEST_SERVICE;  //请求服务
    pHandletoIOData->lDataLen = req->lDataLen;
    pHandletoIOData->pDataSend = req->pDataSend;
    NAVI_KP_POSITION *pPosition = (NAVI_KP_POSITION *)(req->pDataSend + REQUEST_HEADER_SIZE);
    pPosition->Kuid = req->lConnectID;   //此字段当做客户端连接ID

    push_unblock_queue_tail(queuerespond, pHandletoIOData);
    LOG_SYS_DEBUG("cnv_handle_clientdata end");
}

int cnv_parse_server(char **pDataBuff, unsigned int *nDataSize, char **pPacket, unsigned int  *nPacketSize, void **auxiliary)
{
    if(*nDataSize < POSITION_PACKAGE_SIZE)
    {
        LOG_SYS_DEBUG("imcompleted data.");
        return CNV_PARSE_FINISH;
    }

    *pPacket = (char *)cnv_comm_Malloc(POSITION_PACKAGE_SIZE);
    if(!*pPacket)
    {
        return CNV_PARSE_ERROR;
    }
    memcpy(*pPacket, *pDataBuff, POSITION_PACKAGE_SIZE);
    *nPacketSize = POSITION_PACKAGE_SIZE;

    return  CNV_PARSE_SUCCESS;
}

void cnv_handle_serverrespond(const IO_TO_HANDLE_DATA *req, CNV_UNBLOCKING_QUEUE *queuerespond)
{
    LOG_SYS_DEBUG("cnv_handle_serverrespond");

    HANDLE_TO_IO_DATA  *pHandletoIOData = (HANDLE_TO_IO_DATA *)cnv_comm_Malloc(sizeof(HANDLE_TO_IO_DATA));
    if(!pHandletoIOData)
    {
        return;
    }

    memset(pHandletoIOData, 0x00, sizeof(HANDLE_TO_IO_DATA));
    pHandletoIOData->lAction = RESPOND_CLIENT;  //应答客户端
    pHandletoIOData->lDataLen = req->lDataLen;
    pHandletoIOData->pDataSend = req->pDataSend;
    NAVI_KP_POSITION *pPosition = (NAVI_KP_POSITION *)(req->pDataSend + REQUEST_HEADER_SIZE);
    pHandletoIOData->lConnectID = pPosition->Kuid;

    push_unblock_queue_tail(queuerespond, pHandletoIOData);
    LOG_SYS_DEBUG("cnv_handle_serverrespond end");
}

void cnv_uninit_service()
{

}

//如果不需要解析函数或业务处理函数，此处可略其中一个或两个
void  set_callback_function(int nCallbackType, CALLBACK_STRUCT_T *pCallbackStruct)
{
    if(nCallbackType == CLIENT_CALLBACK_FUNC)  //处理客户端数据的解析函数和业务处理函数
    {
        if(!strcmp(pCallbackStruct->strProtocol, "careland.position"))
        {
            pCallbackStruct->pfncnv_parse_protocol = cnv_parse_client;
            pCallbackStruct->pfncnv_handle_business = cnv_handle_clientdata;
        }
    }
    else if(nCallbackType == SERVER_CALLBACK_FUNC)  //接收服务端数据的解析函数和业务处理函数
    {
        pCallbackStruct->pfncnv_parse_protocol = cnv_parse_server;
        pCallbackStruct->pfncnv_handle_business = cnv_handle_serverrespond;
    }
}

int main(int argc,char *argv[])
{
    int lRet = CNV_ERR_OK;
    char  strConfPath[MAX_PATH] = "";

    //初始化配置文件路径
    lRet = cnv_comm_GetCurDirectory(argv[0], strConfPath);
    if(lRet != CNV_ERR_OK)
    {
        return lRet;
    }
    cnv_comm_StrcatA(strConfPath, "/../conf/");

    //获取队列指针，由框架清理
    CNV_UNBLOCKING_QUEUE  *queueConfig = NULL;
    get_svrconf_queue(&queueConfig);

    lRet = cnv_init_agent(strConfPath, queueConfig);  //初始化服务
    if(lRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("cnv_init_agent failed!");
        return  lRet;
    }

    lRet = initial_netframe(strConfPath, queueConfig, POSITION_PACKAGE_SIZE);  //加载框架
    if(lRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("cnv_interface_initial_frame failed!");
        exit(0);
    }

    return  CNV_ERR_OK;
}
