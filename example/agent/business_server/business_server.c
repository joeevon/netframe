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

#include "business_server.h"
#include "cnv_comm.h"
#include "cnv_liblog4cplus.h"
#include "cnv_xml_parse.h"
#include "netframe_main.h"
#include "netframe_net.h"
#include <string.h>

static  POSITION_PARAMS  g_tPosiParams;

int cnv_init_otherservice(char *strConfPath, CNV_UNBLOCKING_QUEUE  *queueConfig)
{
    int lRet = CNV_ERR_OK;
    char strMsg[MAX_PATH] = "";
    memset(&g_tPosiParams, 0, sizeof(g_tPosiParams));

    lRet = otherservice_init_path(strConfPath);       // 初始化配置文件路径
    if(lRet != CNV_ERR_OK)
    {
        return  lRet;
    }

    set_config(g_tPosiParams.tParamConfigPath.strLog, strMsg, sizeof(strMsg));

    LOG_SYS_DEBUG("cnv_init_otherservice end");
    return CNV_ERR_OK;
}

int otherservice_init_path(char *strConfPath)
{
    //日志配置
    snprintf(g_tPosiParams.tParamConfigPath.strLog, sizeof(g_tPosiParams.tParamConfigPath.strLog)-1, "%s", strConfPath);
    cnv_comm_StrcatA(g_tPosiParams.tParamConfigPath.strLog, "urconfig.properties");

    return CNV_ERR_OK;
}

int cnv_parse_client(char **pDataBuff, unsigned int *nDataSize, char **pPacket, unsigned int  *nPacketSize, void **auxiliary)
{
    if(*nDataSize < POSITION_PACKAGE_SIZE)    //接收长度小于一个完整的包长度
    {
        LOG_SYS_DEBUG("imcompleted data.");
        return CNV_PARSE_FINISH;
    }

    *pPacket = (char *)cnv_comm_Malloc(POSITION_PACKAGE_SIZE);
    if(0 == *pPacket)
    {
        return CNV_PARSE_ERROR;
    }
    memcpy(*pPacket, *pDataBuff, POSITION_PACKAGE_SIZE);
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

    pHandletoIOData->lAction = RESPOND_CLIENT;
    pHandletoIOData->lDataLen = req->lDataLen;
    pHandletoIOData->pDataSend = req->pDataSend;
    pHandletoIOData->lConnectID = req->lConnectID;

    push_unblock_queue_tail(queuerespond, pHandletoIOData);
    LOG_SYS_DEBUG("cnv_handle_clientdata end");
}

void cnv_uninit_service()
{

}

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
}

int  main(int argc,char *argv[])
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

    lRet = cnv_init_otherservice(strConfPath, queueConfig);  //初始化服务
    if(lRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("cnv_init_otherservice failed!");
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
