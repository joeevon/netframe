/****************************
FileName:netframe_admin.c
(C) Copyright 2015 by Careland
凯立德秘密信息
Description:
主要功能简述
    netframe_admin C文件
Note:
    Author:Wang Zhiyong
    Create Date: 2015-11-18
    *****************************/

#include "netframe_admin.h"
#include "netframe_structdefine.h"
#include "netframe/cnv_comm.h"
#include "log/cnv_liblog4cplus.h"
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

extern GLOBAL_PARAMS  g_params;

int admin_responed_acclog(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_acclog begin.");

    char strTmpFile[256] = { 0 }, strCommand[256] = { 0 };
    snprintf(strTmpFile, sizeof(strTmpFile) - 1, "%s/netframe_admin.log", g_params.tConfigPath.strLogDir);
    snprintf(strCommand, sizeof(strCommand) - 1, "ls -lhrt %s/acclog/ | tail -n 1 | awk '{print $9}' > %s", g_params.tConfigPath.strLogDir, strTmpFile);   //获取最新文件名
    system(strCommand);

    char strFileName[256] = { 0 };
    FILE  *pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }
    fgets(strFileName, sizeof(strFileName) - 1, pFile);
    strFileName[strlen(strFileName) - 1] = 0;   //去掉末尾的'\n'，才能正确执行shell
    fclose(pFile);

    char strFilePath[256] = { 0 };
    snprintf(strFilePath, sizeof(strFilePath) - 1, "%s/acclog/%s", g_params.tConfigPath.strLogDir, strFileName);
    memset(strCommand, 0, 256);
    snprintf(strCommand, sizeof(strCommand) - 1, "tail -n 50 %s > %s", strFilePath, strTmpFile);
    system(strCommand);

    pFile = NULL;
    pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }

    fseek(pFile, 0, SEEK_END);
    unsigned long ulFileSize = ftell(pFile);
    rewind(pFile);

    char strBuffer[8192] = { 0 };
    unsigned long ulResult = fread(strBuffer, 1, ulFileSize, pFile);
    if(ulResult != ulFileSize)
    {
        return CNV_ERR_READ_FILE;
    }

    pHandleIOData->pDataSend = (char *)cnv_comm_Malloc(strlen(strBuffer) + 1);
    if(!pHandleIOData->pDataSend)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pHandleIOData->pDataSend, 0, strlen(strBuffer) + 1);
    memcpy(pHandleIOData->pDataSend, strBuffer, strlen(strBuffer));
    pHandleIOData->lAction = RESPOND_CLIENT;
    pHandleIOData->lDataLen = strlen(strBuffer);

    fclose(pFile);

    LOG_SYS_DEBUG("admin_responed_acclog end.");
    return CNV_ERR_OK;
}

int admin_responed_syslog(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_syslog begin.");

    char strTmpFile[256] = { 0 }, strCommand[256] = { 0 };
    snprintf(strTmpFile, sizeof(strTmpFile) - 1, "%s/netframe_admin.log", g_params.tConfigPath.strLogDir);
    snprintf(strCommand, sizeof(strCommand) - 1, "ls -lhrt %s/syslog/ | tail -n 1 | awk '{print $9}' > %s", g_params.tConfigPath.strLogDir, strTmpFile);   //获取最新文件名
    system(strCommand);

    char strFileName[256] = { 0 };
    FILE  *pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }
    fgets(strFileName, sizeof(strFileName) - 1, pFile);
    strFileName[strlen(strFileName) - 1] = 0;   //去掉末尾的'\n'，才能正确执行shell
    fclose(pFile);

    char strFilePath[256] = { 0 };
    snprintf(strFilePath, sizeof(strFilePath) - 1, "%s/syslog/%s", g_params.tConfigPath.strLogDir, strFileName);
    memset(strCommand, 0, 256);
    snprintf(strCommand, sizeof(strCommand) - 1, "tail -n 50 %s > %s", strFilePath, strTmpFile);
    system(strCommand);

    pFile = NULL;
    pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }

    fseek(pFile, 0, SEEK_END);
    unsigned long ulFileSize = ftell(pFile);
    rewind(pFile);

    char strBuffer[8192] = { 0 };
    unsigned long ulResult = fread(strBuffer, 1, ulFileSize, pFile);
    if(ulResult != ulFileSize)
    {
        return CNV_ERR_READ_FILE;
    }

    pHandleIOData->pDataSend = (char *)cnv_comm_Malloc(strlen(strBuffer) + 1);
    if(!pHandleIOData->pDataSend)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pHandleIOData->pDataSend, 0, strlen(strBuffer) + 1);
    memcpy(pHandleIOData->pDataSend, strBuffer, strlen(strBuffer));
    pHandleIOData->lAction = RESPOND_CLIENT;
    pHandleIOData->lDataLen = strlen(strBuffer);

    fclose(pFile);

    LOG_SYS_DEBUG("admin_responed_syslog end.");
    return CNV_ERR_OK;
}

int admin_responed_applog(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_applog begin.");

    char strTmpFile[256] = { 0 }, strCommand[256] = { 0 };
    snprintf(strTmpFile, sizeof(strTmpFile) - 1, "%s/netframe_admin.log", g_params.tConfigPath.strLogDir);
    snprintf(strCommand, sizeof(strCommand) - 1, "ls -lhrt %s/applog/ | tail -n 1 | awk '{print $9}' > %s", g_params.tConfigPath.strLogDir, strTmpFile);   //获取最新文件名
    system(strCommand);

    char strFileName[256] = { 0 };
    FILE  *pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }
    fgets(strFileName, sizeof(strFileName) - 1, pFile);
    strFileName[strlen(strFileName) - 1] = 0;   //去掉末尾的'\n'，才能正确执行shell
    fclose(pFile);

    char strFilePath[256] = { 0 };
    snprintf(strFilePath, sizeof(strFilePath) - 1, "%s/applog/%s", g_params.tConfigPath.strLogDir, strFileName);
    memset(strCommand, 0, 256);
    snprintf(strCommand, sizeof(strCommand) - 1, "tail -n 50 %s > %s", strFilePath, strTmpFile);
    system(strCommand);

    pFile = NULL;
    pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }

    fseek(pFile, 0, SEEK_END);
    unsigned long ulFileSize = ftell(pFile);
    rewind(pFile);

    char strBuffer[8192] = { 0 };
    unsigned long ulResult = fread(strBuffer, 1, ulFileSize, pFile);
    if(ulResult != ulFileSize)
    {
        return CNV_ERR_READ_FILE;
    }

    pHandleIOData->pDataSend = (char *)cnv_comm_Malloc(strlen(strBuffer) + 1);
    if(!pHandleIOData->pDataSend)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pHandleIOData->pDataSend, 0, strlen(strBuffer) + 1);
    memcpy(pHandleIOData->pDataSend, strBuffer, strlen(strBuffer));
    pHandleIOData->lAction = RESPOND_CLIENT;
    pHandleIOData->lDataLen = strlen(strBuffer);

    fclose(pFile);
    LOG_SYS_DEBUG("admin_responed_applog end.");
    return CNV_ERR_OK;
}

int admin_responed_procinfo_thread(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_procinfo_thread begin.");

    long lProcId = getpid();
    char strCommand[256] = { 0 }, strTmpFile[256] = { 0 };
    snprintf(strTmpFile, sizeof(strTmpFile) - 1, "%s/netframe_admin.log", g_params.tConfigPath.strLogDir);
    snprintf(strCommand, sizeof(strCommand) - 1, "ps -efT | grep %ld | grep -v grep > %s", lProcId, strTmpFile);
    system(strCommand);

    FILE  *pFile = NULL;
    pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }

    fseek(pFile, 0, SEEK_END);
    unsigned long ulFileSize = ftell(pFile);
    rewind(pFile);

    char strBuffer[1024] = { 0 };
    unsigned long ulResult = fread(strBuffer, 1, ulFileSize, pFile);
    if(ulResult != ulFileSize)
    {
        return CNV_ERR_READ_FILE;
    }
    fclose(pFile);

    pHandleIOData->pDataSend = (char *)cnv_comm_Malloc(strlen(strBuffer) + 1);
    if(!pHandleIOData->pDataSend)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pHandleIOData->pDataSend, 0, strlen(strBuffer) + 1);
    memcpy(pHandleIOData->pDataSend, strBuffer, strlen(strBuffer));
    pHandleIOData->lAction = RESPOND_CLIENT;
    pHandleIOData->lDataLen = strlen(strBuffer);

    LOG_SYS_DEBUG("admin_responed_procinfo_thread end.");
    return CNV_ERR_OK;
}

int admin_responed_procinfo_memory(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_procinfo_memory begin.");

    long lProcId = getpid();
    char strCommand[256] = { 0 }, strTmpFile[256] = { 0 };
    snprintf(strTmpFile, sizeof(strTmpFile) - 1, "%s/netframe_admin.log", g_params.tConfigPath.strLogDir);
    snprintf(strCommand, sizeof(strCommand) - 1, "ps aux | grep %ld | grep -v grep > %s", lProcId, strTmpFile);
    system(strCommand);

    FILE  *pFile = NULL;
    pFile = fopen(strTmpFile, "rb");
    if(!pFile)
    {
        return CNV_ERR_OPEN_FILE;
    }

    fseek(pFile, 0, SEEK_END);
    unsigned long ulFileSize = ftell(pFile);
    rewind(pFile);

    char strBuffer[1024] = { 0 };
    unsigned long ulResult = fread(strBuffer, 1, ulFileSize, pFile);
    if(ulResult != ulFileSize)
    {
        return CNV_ERR_READ_FILE;
    }
    fclose(pFile);

    pHandleIOData->pDataSend = (char *)cnv_comm_Malloc(strlen(strBuffer) + 1);
    if(!pHandleIOData->pDataSend)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pHandleIOData->pDataSend, 0, strlen(strBuffer) + 1);
    memcpy(pHandleIOData->pDataSend, strBuffer, strlen(strBuffer));
    pHandleIOData->lAction = RESPOND_CLIENT;
    pHandleIOData->lDataLen = strlen(strBuffer);

    LOG_SYS_DEBUG("admin_responed_procinfo_memory end.");
    return CNV_ERR_OK;
}

int admin_responed_help(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_help begin.");
    char strRespMsg[1024] = { 0 };
    sprintf(strRespMsg + strlen(strRespMsg), "all command:\n");
    sprintf(strRespMsg + strlen(strRespMsg), "help\n");
    sprintf(strRespMsg + strlen(strRespMsg), "quit\n");
    sprintf(strRespMsg + strlen(strRespMsg), "applog\n");
    sprintf(strRespMsg + strlen(strRespMsg), "syslog\n");
    sprintf(strRespMsg + strlen(strRespMsg), "acclog\n");
    sprintf(strRespMsg + strlen(strRespMsg), "procinfo-mem\n");
    sprintf(strRespMsg + strlen(strRespMsg), "procinfo-thd\n\n");

    pHandleIOData->pDataSend = (char *)cnv_comm_Malloc(strlen(strRespMsg) + 1);
    if(!pHandleIOData->pDataSend)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pHandleIOData->pDataSend, 0, strlen(strRespMsg) + 1);
    memcpy(pHandleIOData->pDataSend, strRespMsg, strlen(strRespMsg));
    pHandleIOData->lAction = RESPOND_CLIENT;
    pHandleIOData->lDataLen = strlen(strRespMsg);

    LOG_SYS_DEBUG("admin_responed_help end.");
    return CNV_ERR_OK;
}

int admin_responed_quit(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_quit begin.");

    char strCommand[256] = { 0 }, strTmpFile[256] = { 0 };
    snprintf(strTmpFile, sizeof(strTmpFile) - 1, "%s/netframe_admin.log", g_params.tConfigPath.strLogDir);
    snprintf(strCommand, sizeof(strCommand) - 1, "rm -f %s", strTmpFile);
    system(strCommand);  //删除临时文件

    pHandleIOData->lAction = CLOSE_CLIENT;    //关闭客户端

    LOG_SYS_DEBUG("admin_responed_quit end.");
    return CNV_ERR_OK;
}

int admin_responed_error(HANDLE_TO_IO_DATA *pHandleIOData)
{
    LOG_SYS_DEBUG("admin_responed_error begin.");
    char strRespMsg[256] = { 0 };
    snprintf(strRespMsg, sizeof(strRespMsg) - 1, "ERROR.Input help to get menu.\n");

    pHandleIOData->pDataSend = (char *)cnv_comm_Malloc(strlen(strRespMsg) + 1);
    if(!pHandleIOData->pDataSend)
    {
        return CNV_ERR_MALLOC;
    }
    memset(pHandleIOData->pDataSend, 0, strlen(strRespMsg) + 1);
    memcpy(pHandleIOData->pDataSend, strRespMsg, strlen(strRespMsg));
    pHandleIOData->lAction = RESPOND_CLIENT;   //应答客户端
    pHandleIOData->lDataLen = strlen(strRespMsg) + 1;

    LOG_SYS_DEBUG("admin_responed_error end.");
    return CNV_ERR_OK;
}

void admin_handle_data(const IO_TO_HANDLE_DATA *req, CNV_UNBLOCKING_QUEUE *queuerespond, void *pHandleParam)
{
    int nRet = CNV_ERR_OK;

    HANDLE_TO_IO_DATA *pHandleIOData = (HANDLE_TO_IO_DATA *)cnv_comm_Malloc(sizeof(HANDLE_TO_IO_DATA));
    if(!pHandleIOData)
    {
        return;
    }
    memset(pHandleIOData, 0x00, sizeof(HANDLE_TO_IO_DATA));

    if(!strncmp(req->pDataSend, "help", req->lDataLen - 2))    //telnet客户端发过来的数据尾部默认添加"\r\n"
    {
        nRet = admin_responed_help(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }
    else if(!strncmp(req->pDataSend, "quit", req->lDataLen - 2))
    {
        nRet = admin_responed_quit(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }
    else if(!strncmp(req->pDataSend, "applog", req->lDataLen - 2))
    {
        nRet = admin_responed_applog(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }
    else if(!strncmp(req->pDataSend, "syslog", req->lDataLen - 2))
    {
        nRet = admin_responed_syslog(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }
    else if(!strncmp(req->pDataSend, "acclog", req->lDataLen - 2))
    {
        nRet = admin_responed_acclog(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }
    else if(!strncmp(req->pDataSend, "procinfo-mem", req->lDataLen - 2))
    {
        nRet = admin_responed_procinfo_memory(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }
    else if(!strncmp(req->pDataSend, "procinfo-thd", req->lDataLen - 2))
    {
        nRet = admin_responed_procinfo_thread(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }
    else
    {
        nRet = admin_responed_error(pHandleIOData);
        if(nRet != CNV_ERR_OK)
        {
            cnv_comm_Free(pHandleIOData);
            return;
        }
    }

    pHandleIOData->lConnectID = req->lConnectID;
    push_unblock_queue_tail(queuerespond, pHandleIOData);
}

int admin_parse_data(char **ppDataBuff, unsigned int *pnDataSize, char **ppPacket, unsigned int *pnPacketSize, void **ppAuxiliary)
{
    *ppPacket = (char *)cnv_comm_Malloc(*pnDataSize);
    if(!*ppPacket)
    {
        return CNV_PARSE_ERROR;
    }
    memcpy(*ppPacket, *ppDataBuff, *pnDataSize);
    *pnPacketSize = *pnDataSize;

    return CNV_PARSE_SUCCESS;
}
