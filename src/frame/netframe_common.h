/****************************
    FileName:netframe_common.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_common 头文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-07
*****************************/

#ifndef __NETFRAME_COMMON_H__
#define __NETFRAME_COMMON_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "netframe_structdefine.h"

    //全局配置参数
    extern GLOBAL_PARAMS  g_params;

    /*=======================================================
    功能:
        初始化路径
    参数:
        [in] strFileName  完整的文件路径
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int netframe_init_path(char *strConfPath);

    /*=======================================================
    功能:
        初始化配置文件
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int netframe_init_config();

    /*=======================================================
       功能:
           初始化心跳间隔
       参数:
           无
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern int  netframe_init_timer(int Epollfd, int  timerfd, TIMER_STRUCT  *ptTimer);

    /*=======================================================
       功能:
           hashmap删除数据
       参数:
           无
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern K_BOOL hashmap_earase_socket(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase);

    /*=======================================================
       功能:
           hashmap删除数据回调函数
       参数:
           无
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern K_BOOL hashmap_earase_callback(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase);

    /*=======================================================
       功能:
           发送心跳包
       参数:
           无
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern int  netframe_heart_beat(int  timerfd_hearbeat, IO_THREAD_CONTEXT *pIoThreadContext);

    /*=======================================================
           功能:
               socket清理
           参数:
               无
           返回值:
               0    成功
               其它 失败
           =========================================================*/
    extern int  netframe_socket_clear(int Epollfd, int  fd, void   *HashConnidFd);

    /*=======================================================
    功能:
        解析负载线程
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int cnv_parse_distribution(char  *strAlgorithm, char  *strDistribution, CNV_UNBLOCKING_QUEUE  *queDistribute);

    /*=======================================================
    功能:
       获取哈希key值
    参数:
       [in]        pSeedOfKey  获取key值的种子
    [out]        pKey    返回的key值
    返回值:
       0    成功
       其它 失败
    =========================================================*/
    extern  int  netframe_get_hashkey(void   *Hashmap, int *pSeedOfKey);

    /*=======================================================
    功能:
            发送登录请求
    参数:
    返回值:
    0    成功
    其它 失败
    =========================================================*/
    extern int netframe_req_login(int Socket, SERVER_SOCKET_DATA *pSvrSockData);

    /*=======================================================
    功能:
        建立长连接
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int  netframe_long_connect(IO_THREAD_CONTEXT *pIoThreadContext, CNV_UNBLOCKING_QUEUE  *queServer);

    extern int netframe_reconnect_server(SERVER_SOCKET_DATA *pSvrSockData, IO_THREAD_CONTEXT *pIoThreadContext);

    /*=======================================================
       功能:
       hashmap 添加映射
       参数:
           [in]
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern int  hash_add_addrsocket(int Socket, SERVER_SOCKET_DATA *pSvrSockData, void *HashAddrFd, void *pConnId);

    /*=======================================================
      功能:
      参数:
          [in]
      返回值:
          0    成功
          其它 失败
      =========================================================*/
    extern int hash_add_conidfd(int Socket, SERVER_SOCKET_DATA  *pSvrSockData, IO_THREAD_CONTEXT *pIoThreadContext, char **out_pConnId);

    /*=======================================================
       功能:
       获取hashmap的当前key值
       参数:
           [in]
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern int  get_current_hashkey(IO_THREAD_CONTEXT  *pIoThreadContext);

    /*=======================================================
       功能:
       queue迭代查找回调函数
       参数:
           [in]
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern K_BOOL queue_search_int(void *nodedata, void *searchdata);

    /*=======================================================
       功能:
       queue迭代查找回调函数
       参数:
           [in]
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern K_BOOL queue_search_string(void *nodedata, void *searchdata);

    /*=======================================================
       功能:
       删除hashmap某个key、value，释放对应内存
       参数:
           [in]
       返回值:
           0    成功
           其它 失败
       =========================================================*/
    extern void  remove_client_socket_hashmap(int Epollfd, void *HashConnidFd, void *pKey);

    /*=======================================================
     功能:
           删除服务端hashmap
     参数:
         [in]
     返回值:
         0    成功
         其它 失败
     =========================================================*/
    extern void remove_server_socket_hashmap(int Epollfd, void *HashAddrFd, void *pKey);

    /*=======================================================
      功能:

      参数:
          [in]
      返回值:
          0    成功
          其它 失败
      =========================================================*/
    extern void cnv_hashmap_free_socket(void *Hashmap, void *Epollfd);

    /*=======================================================
      功能:
      参数:
          [in]
      返回值:
          0    成功
          其它 失败
      =========================================================*/
    extern  K_BOOL earase_client_socket_hashmap(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase);

    /*=======================================================
      功能:
      参数:
          [in]
      返回值:
          0    成功
          其它 失败
      =========================================================*/
    extern  K_BOOL earase_server_socket_hashmap(void  *pKey, void  *pValue, void  *pContext, K_BOOL *bIsEarase);

    /*=======================================================
      功能:
       释放queue内存,销毁队列
      参数:
          [in]
      返回值:
          0    成功
          其它 失败
      =========================================================*/
    extern void  free_unblock_queue(CNV_UNBLOCKING_QUEUE  *queue);

    /*=======================================================
     功能:
     参数:
         [in]
     返回值:
         0    成功
         其它 失败
     =========================================================*/
    extern void free_handleio_unblockqueue(CNV_UNBLOCKING_QUEUE *unblock_queue);

    extern K_BOOL printhashmap(void  *pKey, void  *pValue, void  *pContext);
    extern  K_BOOL printDistribution(void *nodedata, void *in_pContext);
    extern K_BOOL printAllowedClients(void *nodedata, void *in_pContext);

#ifdef __cplusplus
};
#endif

#endif  //__NETFRAME_COMMON_H__
