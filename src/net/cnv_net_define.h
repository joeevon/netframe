/****************************
    FileName:netframe_net.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_net 头文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-11
*****************************/

#ifndef __CNV_NET__DEFINE_H__
#define __CNV_NET__DEFINE_H__

#include "cnv_core_typedef.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define   CONNECT_TIMEOUT  5000  //客户端连接超时
#define BASE 65521UL    /* largest prime smaller than 65536 */
#define NMAX 5552
    //网络通讯错误码起始值
#define   AGENT_NET_BASE   24000

    //网络通讯错误码定义
    typedef enum __enumAGENT_NET_ERRCODE
    {
        //创建socket失败
        AGENT_NET_CREATE_SOCKET_FAILED   = (AGENT_NET_BASE + 1),
        AGENT_NET_ADDR_IN_USE   = (AGENT_NET_BASE + 2),
        AGENT_NET_PARAM_INVALID   = (AGENT_NET_BASE + 3),
        AGENT_NET_SETBLOKOPT_FAILED   = (AGENT_NET_BASE + 4),
        AGENT_NET_BIND_FAILED   = (AGENT_NET_BASE + 5),
        AGENT_NET_LISTEN_FAILED   = (AGENT_NET_BASE + 6),
        AGENT_NET_CREATE_EPOLL   = (AGENT_NET_BASE + 7),
        AGENT_NET_ADD_EPOLL   = (AGENT_NET_BASE + 8),
        AGENT_NET_ADD_READEPOLL   = (AGENT_NET_BASE + 9),
        AGENT_NET_ADD_WRITEEPOLL   = (AGENT_NET_BASE + 10),
        AGENT_NET_ERR_READ   = (AGENT_NET_BASE + 11),
        AGENT_NET_READ_INCOMPLETED   = (AGENT_NET_BASE + 12),
        AGENT_NET_ERR_WRITE   = (AGENT_NET_BASE + 13),
        AGENT_NET_WRITE_INCOMPLETED   = (AGENT_NET_BASE + 14),
        AGENT_NET_CONNECT_FAILED   = (AGENT_NET_BASE + 15),
        AGENT_NET_PROTOBUFFER_PACK   = (AGENT_NET_BASE + 16),
        AGENT_NET_CLIENT_CLOSED   = (AGENT_NET_BASE + 17),
        AGENT_NET_READ_BUSY   = (AGENT_NET_BASE + 18),
        AGENT_NET_NOT_CONNECTED   = (AGENT_NET_BASE + 19),
        AGENT_NET_WRITE_BUSY   = (AGENT_NET_BASE + 20),
        AGENT_NET_CONNECTION_RESET   = (AGENT_NET_BASE + 21),
        AGENT_NET_CONNECTION_ABNORMAL   = (AGENT_NET_BASE + 22),
        AGENT_NET_WRITE_ABNORMAL   = (AGENT_NET_BASE + 23),
        AGENT_NET_READ_ABNORMAL   = (AGENT_NET_BASE + 24),
        AGENT_NET_SHUT_DOWN   = (AGENT_NET_BASE + 25),
        AGENT_NET_ILLEGAL_DATA = (AGENT_NET_BASE + 26),
        AGENT_NET_NONBLOCK_CONNECT_SELECT = (AGENT_NET_BASE + 27),
        AGENT_NET_NONBLOCK_CONNECT_TIMEOUT = (AGENT_NET_BASE + 28),
        AGENT_NET_CONNECT_SELF = (AGENT_NET_BASE + 29),
        AGENT_NET_CONNECT_REFUSED = (AGENT_NET_BASE + 30),
        AGENT_NET_ILLEGAL_LENGTH = (AGENT_NET_BASE + 31),
    }
    AGENT_NET_ERRCODE;

#ifdef __cplusplus
};
#endif

#endif  //__CNV_NET__DEFINE_H__
