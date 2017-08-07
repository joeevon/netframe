#include "netframe_main.h"
#include "cnv_queue.h"
#include "common_type.h"
#include "log/cnv_liblog4cplus.h"
#include <string.h>
#include <stdint.h>
#include <libgen.h>
#include <string>
#include <assert.h>

void handle_client_data(const IO_TO_HANDLE_DATA *ptIOHandleData, CNV_UNBLOCKING_QUEUE *queueRespond, void *HashTimer, int  Epollfd, void *pBusiHandleParam)
{
    HANDLE_TO_IO_DATA *pHandletoIOData = (HANDLE_TO_IO_DATA *)malloc(sizeof(HANDLE_TO_IO_DATA));
    assert(pHandletoIOData);
    memset(pHandletoIOData, 0, sizeof(HANDLE_TO_IO_DATA));

    pHandletoIOData->lAction = RESPOND_CLIENT;
    pHandletoIOData->lDataLen = ptIOHandleData->lDataLen;
    pHandletoIOData->pDataSend = (char *)malloc(pHandletoIOData->lDataLen);
    assert(pHandletoIOData->pDataSend);
    memcpy(pHandletoIOData->pDataSend, ptIOHandleData->pDataSend, pHandletoIOData->lDataLen);
    pHandletoIOData->lConnectID = ptIOHandleData->lConnectID;

    LOG_APP_DEBUG("DataLen:%d.", pHandletoIOData->lDataLen);
    push_unblock_queue_tail(queueRespond, pHandletoIOData);
}

int parse_client_data(char **ppDataBuff, uint32_t *pnDataSize, char **ppPacket, uint32_t  *pnPacketSize, uint32_t  *pnMoveSize)
{
    *pnMoveSize = *pnDataSize;
    *pnPacketSize = *pnDataSize;
    *ppPacket = (char *)malloc(*pnPacketSize);
    assert(*ppPacket);
    memcpy(*ppPacket, *ppDataBuff, *pnPacketSize);

    LOG_APP_DEBUG("PacketSize:%u.", *pnPacketSize);
    return  CNV_PARSE_SUCCESS;
}

void set_callback_function(int nCallbackType, CALLBACK_STRUCT_T *pCallbackStruct)
{
    if(nCallbackType == CLIENT_CALLBACK_FUNC)  //处理客户端数据的解析函数和业务处理函数
    {
        if(!strcmp(pCallbackStruct->strProtocol, "netframe_test"))
        {
            pCallbackStruct->pfncnv_parse_protocol = parse_client_data;
            pCallbackStruct->pfncnv_handle_business = handle_client_data;
        }
    }
}

int init_handle_params(void **ppHandleParams)
{
    return 0;
}

bool init_business_service(std::string &strConfDir)
{
    std::string strLogPath = strConfDir + "urconfig.properties";
    char strMsg[MAX_PATH] = "";
    set_config(strLogPath.c_str(), strMsg, sizeof(strMsg));

    return true;
}

int main(int argc, char *argv[])
{
    std::string strConfDir = std::string(dirname(argv[0])) + "/../conf/";
    init_business_service(strConfDir);

    std::string strNetConf = strConfDir + "net_frame.xml";
    if(initial_netframe((char *)strNetConf.c_str(), NULL, 8192) != CNV_ERR_OK) //加载框架
    {
        LOG_SYS_ERROR("initial_netframe failed.");
        return -1;
    }

    return  CNV_ERR_OK;
}
