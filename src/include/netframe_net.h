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

#ifndef __CNV_AGENT_NET_H__
#define __CNV_AGENT_NET_H__

#include "cnv_core_typedef.h"
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/un.h>

#ifdef __cplusplus
extern "C"
{
#endif


#define DO1(buf,i)  {adler += (buf)[i]; sum2 += adler;}
#define DO2(buf,i)  DO1(buf,i); DO1(buf,i+1);
#define DO4(buf,i)  DO2(buf,i); DO2(buf,i+2);
#define DO8(buf,i)  DO4(buf,i); DO4(buf,i+4);
#define DO16(buf)   DO8(buf,0); DO8(buf,8);

#  define MOD4(a) \
    do { \
        if (a >= (BASE << 4)) a -= (BASE << 4); \
        if (a >= (BASE << 3)) a -= (BASE << 3); \
        if (a >= (BASE << 2)) a -= (BASE << 2); \
        if (a >= (BASE << 1)) a -= (BASE << 1); \
        if (a >= BASE) a -= BASE; \
    } while (0)

#  define MOD(a) \
    do { \
        if (a >= (BASE << 16)) a -= (BASE << 16); \
        if (a >= (BASE << 15)) a -= (BASE << 15); \
        if (a >= (BASE << 14)) a -= (BASE << 14); \
        if (a >= (BASE << 13)) a -= (BASE << 13); \
        if (a >= (BASE << 12)) a -= (BASE << 12); \
        if (a >= (BASE << 11)) a -= (BASE << 11); \
        if (a >= (BASE << 10)) a -= (BASE << 10); \
        if (a >= (BASE << 9)) a -= (BASE << 9); \
        if (a >= (BASE << 8)) a -= (BASE << 8); \
        if (a >= (BASE << 7)) a -= (BASE << 7); \
        if (a >= (BASE << 6)) a -= (BASE << 6); \
        if (a >= (BASE << 5)) a -= (BASE << 5); \
        if (a >= (BASE << 4)) a -= (BASE << 4); \
        if (a >= (BASE << 3)) a -= (BASE << 3); \
        if (a >= (BASE << 2)) a -= (BASE << 2); \
        if (a >= (BASE << 1)) a -= (BASE << 1); \
        if (a >= BASE) a -= BASE; \
    } while (0)

    typedef struct __tagNAVI_SVC_PROTOBUFFER
    {
        int     lLen;  //数据包总长度,不包括自身
        int     lNameLen;   //消息名称的长度,建议采用namespace.name 的方式
        char     *pTypeName;   //消息名称,长度由lNameLen决定
        char      *pProtoBufData;  //实际的消息数据,放置业务数据。长度为(lLen - lNameLen - 8)
        int     lCheckSum;    //数据校验和,使用 alder32 和 CRC32算法都可，建议 alder32算法，运算速度快
        short    flags;
    } NAVI_SVC_PROTOBUFFER, *PNAVI_SVC_PROTOBUFFER;

    /*=======================================================
    功能:
        建立一个tcp服务端的监听fd
    参数:
        [out]  pSocket  建立的连接句柄
        [in]  pSockAddr  服务器地址
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_init_tcpserver(int *pSocket, struct sockaddr_in *pSockAddr, int lMaxCout);

    /*=======================================================
    功能:
        建立一个udp服务端的监听fd
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_init_udpserver(int *pSocket, struct sockaddr_in *pSockAddr);

    /*=======================================================
    功能:
        建立一个unixsocket服务端的监听fd
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_init_unixsocket(int *pSocket, struct sockaddr_un *pSockAddr);

    /*=======================================================
    功能:
        连接到服务器
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_connect(int *pSocket, char  *pAddrIP, unsigned int  ulPort, int nMaxTimeout);

    /*=======================================================
    功能:
        关闭socket连接
    参数:
        [in]  pSocket  要关闭的连接句柄
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_close_socket(int Socket);

    /*=======================================================
    功能:
        设置fd的阻塞属性
    参数:
        [in]   pSocket  网络句柄
        [in]    bIsBlocking   阻塞标志(K_FALSE:不阻塞，true:阻塞)
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_setblockopt(int  Socket, K_BOOL  bIsBlocking);

    /*=======================================================
    功能:
        创建Epoll
    参数:
        [in]   nEpfd  epoll句柄
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_create_epoll(int *pEpfd, int  lSize);

    /*=======================================================
    功能:
        添加epoll事件
    参数:
        [in]   Epollfd  epoll句柄
        [in]    Socket  待添加句柄
        [in]    ulEventType   待添加的事件类型
        [in]    ConnId  连接ID，默认为NULL
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_add_event(int Epollfd, int Socket, unsigned int ulEventType, char *pConnId);

    /*=======================================================
    功能:
        添加epoll读事件
    参数:
        [in]   nEpfd  epoll句柄
        [in]    fd  待添加句柄
        [in]    ConnId  连接ID，默认为NULL
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_add_readevent(int Epollfd, int Socket, void *pConnId);

    /*=======================================================
    功能:
        添加epoll写事件
    参数:
        [in]   nEpfd  epoll句柄
        [in]    fd  待添加句柄
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_add_writeevent(int Epollfd, int Socket, void *pConnId);

    /*=======================================================
    功能:
        读事件
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_read(int Socket, char *pDataBuffer, int  *pDataLen);

    /*=======================================================
    功能:
        读事件
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/

    int netframe_recvmsg(int Socket, struct msghdr *pmsg, int *pDataLen);

    /*=======================================================
    功能:
        写事件
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_write(int Socket, char *pDataBuffer, int lDataLen, int *lLenAlreadyWrite);

    /*=======================================================
    功能:
        写事件
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/

    int netframe_sendmsg(int Socket, struct msghdr *pmsg, int nDataLen, int *pnLenAlreadyWrite);

    /*=======================================================
    功能:
        写事件
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_modify_event(int Epollfd, struct epoll_event *pepollevent);

    /*=======================================================
    功能:
       读事件修改
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_modify_readevent(int Epollfd, int  Socket, char *pConnId);

    /*=======================================================
    功能:
       写事件修改
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_modify_writeevent(int Epollfd, int  Socket, char *pConnId);

    /*=======================================================
    功能:
      写事件修改
    参数:
    返回值:
       0    成功
       其它 失败
    =========================================================*/
    int netframe_delete_event_ex(int Epollfd, struct epoll_event *pepollevent);

    /*=======================================================
    功能:
        取错误状态
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_delete_event_ex(int Epollfd, struct epoll_event *pepollevent);

    /*=======================================================
    功能:
    取错误状态
    参数:
    返回值:
    0    成功
    其它 失败
    =========================================================*/
    int netframe_get_sokceterror(int Sockfd);

    /*=======================================================
    功能:
        取本地地址
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    struct sockaddr_in netframe_get_localaddr(int Sockfd);

    /*=======================================================
    功能:
        取对端地址
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    struct sockaddr_in netframe_get_peeraddr(int Sockfd);

    /*=======================================================
    功能:
        取错误状态
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    K_BOOL netframe_is_selfconnected(int Sockfd);

    /*=======================================================
    功能:
       事件删除
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    int netframe_delete_event(int Epollfd, int  Socket);

    /*=======================================================
           功能:
                数据打包
           参数:
               [in]
           返回值:
               0    成功
               其它 失败
      =========================================================*/
    extern int svc_protobuffer_packex(short in_iFlags, char *in_pTypeName,  int in_lNameLen, char *in_pData,
                                      int in_lDataLen,    char **out_pProtoBuf, int *out_plProtoBufLen);

    /*=======================================================
      功能:
           数据校验
      参数:
          [in]
      返回值:
          0    成功
          其它 失败
      =========================================================*/
    extern K_BOOL svc_protobuffer_check(char *in_pProtoBufData,   int in_lProtoBufDataLen);

    extern int svc_protobuffer_unpackex(char *in_pProtoBufData, int in_plProtoBufDataLen, short *out_piFlags,
                                        char **out_pTypeName, int *out_plNameLen, char **out_pData, int *out_plDataLen);

#ifdef __cplusplus
};
#endif

#endif  //__CNV_AGENT_NET_H__
