#include "server.h"
#include "cnv_comm.h"
#include "cnv_liblog4cplus.h"
#include "cnv_xml_parse.h"
#include "netframe_main.h"
#include "netframe_net.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

static  POSITION_PARAMS  g_tPosiParams;

void handle_clientdata(const IO_TO_HANDLE_DATA *req, CNV_UNBLOCKING_QUEUE *queuerespond, void *pBusinessParams)
{
    LOG_SYS_DEBUG("handle_clientdata");

    HANDLE_TO_IO_DATA *pHandletoIOData = (HANDLE_TO_IO_DATA *)cnv_comm_Malloc(sizeof(HANDLE_TO_IO_DATA));
    if(!pHandletoIOData)
    {
        return;
    }
    bzero(pHandletoIOData, sizeof(HANDLE_TO_IO_DATA));

    pHandletoIOData->lAction = RESPOND_CLIENT;
    pHandletoIOData->lDataLen = req->lDataLen;
    pHandletoIOData->pDataSend = (char *)cnv_comm_Malloc(pHandletoIOData->lDataLen);
    memcpy(pHandletoIOData->pDataSend, req->pDataSend, pHandletoIOData->lDataLen);
    pHandletoIOData->lConnectID = req->lConnectID;

    NAVI_KP_POSITION *pPosiData = (NAVI_KP_POSITION *)(req->pDataSend + REQUEST_HEADER_SIZE);
    LOG_APP_INFO("Duid:%d", pPosiData->Duid);

    push_unblock_queue_tail(queuerespond, pHandletoIOData);
    LOG_SYS_DEBUG("handle_clientdata end");
}

int parse_clientdata(char **ppDataBuff, unsigned int *pnDataSize, char **ppPacket, unsigned int  *pnPacketSize, void **ppAuxiliary)
{
    if(*pnDataSize < POSITION_PACKAGE_SIZE)     //接收长度小于一个完整的包长度
    {
        LOG_SYS_DEBUG("imcompleted data.");
        return CNV_PARSE_FINISH;
    }

    *pnPacketSize = POSITION_PACKAGE_SIZE;
    *ppPacket = (char *)cnv_comm_Malloc(*pnPacketSize);
    if(!*ppPacket)
    {
        return CNV_PARSE_ERROR;
    }
    memcpy(*ppPacket, *ppDataBuff, *pnPacketSize);

    if(!*ppAuxiliary)
    {
        NAVI_KP_POSITION *pPosiData = (NAVI_KP_POSITION *)(*ppDataBuff + REQUEST_HEADER_SIZE);
        *ppAuxiliary = cnv_comm_Malloc(sizeof(int));
        *((int *)*ppAuxiliary) = pPosiData->Duid;
    }

    return  CNV_PARSE_SUCCESS;
}

void set_callback_function(int nCallbackType, CALLBACK_STRUCT_T *pCallbackStruct)
{
    if(nCallbackType == CLIENT_CALLBACK_FUNC)  //处理客户端数据的解析函数和业务处理函数
    {
        if(!strcmp(pCallbackStruct->strProtocol, "netframe_test"))
        {
            pCallbackStruct->pfncnv_parse_protocol = parse_clientdata;
            pCallbackStruct->pfncnv_handle_business = handle_clientdata;
        }
    }
}

int init_handle_params(void **ppHandleParams)
{
    return 0;
}

int init_business_service(char *strConfPath)
{
    char strMsg[MAX_PATH] = "";
    memset(&g_tPosiParams, 0, sizeof(g_tPosiParams));

    snprintf(g_tPosiParams.tParamConfigPath.strLog, sizeof(g_tPosiParams.tParamConfigPath.strLog) - 1, "%s", strConfPath);
    cnv_comm_StrcatA(g_tPosiParams.tParamConfigPath.strLog, "urconfig.properties");
    set_config(g_tPosiParams.tParamConfigPath.strLog, strMsg, sizeof(strMsg));

    LOG_SYS_DEBUG("init_business_service end");
    return CNV_ERR_OK;
}

int main(int argc, char *argv[])
{
    int nRet = CNV_ERR_OK;
    char strConfPath[MAX_PATH] = "";

    //初始化配置文件路径
    nRet = cnv_comm_GetCurDirectory(argv[0], strConfPath);
    if(nRet != CNV_ERR_OK)
    {
        return nRet;
    }
    cnv_comm_StrcatA(strConfPath, "/../conf/");

    nRet = init_business_service(strConfPath);  //初始化服务
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("init_business_service failed!");
        return nRet;
    }

    nRet = initial_netframe(strConfPath, NULL, 8192);  //加载框架
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("cnv_interface_initial_frame failed!");
        return nRet;
    }

    return  CNV_ERR_OK;
}
