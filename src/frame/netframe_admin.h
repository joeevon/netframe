/****************************
FileName:netframe_admin.h
(C) Copyright 2015 by Careland
凯立德秘密信息
Description:
主要功能简述
    netframe_admin 头文件
Note:
    Author:Wang Zhiyong
    Create Date: 2015-11-18
*****************************/

#ifndef __NETFRAME_ADMIN_H__
#define __NETFRAME_ADMIN_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "common_type.h"

    /*=======================================================
    功能:
        解析数据
    参数:
    返回值:
    =========================================================*/
    extern int admin_parse_data(char **ppDataBuff, unsigned int *pnDataSize, char **ppPacket, unsigned int *pnPacketSize);

    /*=======================================================
    功能:
        业务处理
    参数:
    返回值:
    =========================================================*/
    extern void admin_handle_data(const IO_TO_HANDLE_DATA *req, CNV_UNBLOCKING_QUEUE *queuerespond, void *pHandleParam);

#ifdef __cplusplus
};
#endif

#endif  //__NETFRAME_ADMIN_H__
