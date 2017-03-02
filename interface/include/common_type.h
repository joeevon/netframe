/****************************
    FileName:common_type.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        common_type 头文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-06-17
*****************************/

#ifndef  __COMMON_TYPE_H__
#define   __COMMON_TYPE_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "cnv_core_typedef.h"
#include "cnv_base_define.h"
#include "cnv_unblock_queue.h"
#include "cnv_blocking_queue.h"
#include "cnv_lock_free_queue.h"
#include <stdint.h>

#define  DEFAULT_HASHMAP_CAPCITY  30000    //默认hashmap大小 
#define  DEFAULT_ARRAY_SIZE  32     //默认数组大小 
#define  HEART_BEAT_CODE  99999     //heartbeat business code
#define  CLIENT_CALLBACK_FUNC  1    //客户端回调
#define  SERVER_CALLBACK_FUNC  2     //服务端回调
#define  REQUEST_SERVICE  1    //请求服务
#define  RESPOND_CLIENT  2    //应答客户端
#define  CLOSE_CLIENT  3  //断开客户端 
#define  REFRESH_CONNECT  4    //刷新与服务端的长连接
#define  NOTICE_CLIENT 5  //服务端主动通知客户端

    typedef enum _CNV_PARSE_RETURN
    {
        CNV_PARSE_SUCCESS = 0,    //正确解析
        CNV_PARSE_FINISH = 1,     //结束解析
        CNV_PARSE_ERROR = 2,     //解析错误
        CNV_PARSE_SHUTDOWN = 3,     //关闭客户端
        CNV_PARSE_MOVE = 4,     //数据偏移
    }
    CNV_PARSE_RETURN;

    typedef struct MONITOR_ELEMENT
    {
        char strStartTime[64];
        uint16_t  nThreadIndex;
        uint32_t  nClientConNum;  //客户端连接数
        uint64_t  lRecvLength;
        uint64_t  lRecvPackNum;  //总收包数
        uint64_t  lParsePackNum;  //解析出来的包数
        uint32_t  nRespondTimes;
        uint64_t  lSendLength;
        uint64_t  lSvrPackNum;    //发送给服务端的包数
        uint64_t  lSvrFailedNum;  //发送给服务端失败包数
        uint32_t  nSvrConnNum;
        uint32_t  nIoMsgQueCount;
        uint16_t  nHandleThreadCount;
        uint32_t  szHanldeMsgQueCount[32];
    } MONITOR_ELEMENT;

    struct  __IO_TO_HANDLE_DATA;
    struct __HANDLE_TO_IO_DATA;
    struct __STATISTICS_QUEQUE_DATA;

    /*=======================================================
    功能:
        协议解析回调
    参数:
        [in]
            pDataBuff:待解析的数据
            nDataSize:待解析数据的长度
        [out]
            pPacket:  解析好的一个数据包(由解析函数分配内存,不用释放,由框架释放)
            nPacketSize:  每一个解析好的数据包的长度
            auxiliary:  辅助参数
    返回值:
        0    成功
        其它 失败
        =========================================================*/
    typedef int (*pfnCNV_PARSE_PROTOCOL)(char **ppDataBuff, int *pnDataSize, char **ppPacket, unsigned int  *pnPacketSize);

    /*=======================================================
    功能:
        业务处理回调函数
        queuerespond里的数据有业务申请内存，框架释放
    参数:
      =========================================================*/
    typedef void (*pfnCNV_HANDLE_BUSINESS)(const struct __IO_TO_HANDLE_DATA *ptIoHandleData, CNV_UNBLOCKING_QUEUE *queuerespond, void *pHandleParam);

    /*=======================================================
    功能:
        辅助线程回调函数
        queuerespond里的数据有业务申请内存，框架释放
    参数:
    =========================================================*/
    typedef void(*pfnCNV_STATISTICS_CALLBACK)(const struct __STATISTICS_QUEQUE_DATA *ptStatisQueData, uint32_t nStatisThreadNum, CNV_UNBLOCKING_QUEUE *queuerespond);

    /*=======================================================
    功能:
        handle定时触发回调函数
     =========================================================*/
    typedef void (*pfnCNV_HANDLE_CALLBACK)(struct __STATISTICS_QUEQUE_DATA **ptStatisQueData, void *pBusiHandleParam);

    /*=======================================================
    功能:
        发送失败回调函数
        queuerespond里的数据有业务申请内存，框架释放
    =========================================================*/
    typedef void (*pfnSEND_FAILED_CALLBACK)(CNV_UNBLOCKING_QUEUE *queuerespond, const struct __HANDLE_TO_IO_DATA *pHandleIOData);

    /*=======================================================
    功能:
        监控回调函数
        此处是io调用的回调函数，用handle给io的结构体也使用，便于请求服务的处理
        ptHandleIoData由业务申请内存，框架释放
    =========================================================*/
    typedef void(*pfnCNV_MONITOR_CALLBACK)(MONITOR_ELEMENT *ptMonitorElement, struct __STATISTICS_QUEQUE_DATA **ptStatisQueData);

    /*=======================================================
    功能:
        设置协议解析回调函数、业务处理函数,框架初始化时调用(由业务自己实现)
    参数:
        nCallbackType: 1.设置客户端数据的解析和业务处理函数; 2.设置服务端数据的解析和业务处理函数
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    struct __CALLBACK_STRUCT_T;
    extern void set_callback_function(int nCallbackType, struct  __CALLBACK_STRUCT_T *pCallback);

    /*=======================================================
    功能:
        设置handle业务处理函数保留变量,框架初始化时调用(由业务自己实现)
    参数:
        pHandleParam:保留参数(如果有申请内存，业务自己释放)
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int init_handle_params(void **pHandleParams);

    //hashmap value
    typedef  struct  __HASHMAP_VALUE
    {
        int  lSize;     //长度
        char  *pKey;     //key地址
        char  *pValue;    //value
    } HASHMAP_VALUE;

    //CALLBACK_T
    typedef struct  __CALLBACK_STRUCT_T
    {
        char strProtocol[DEFAULT_ARRAY_SIZE];
        char strServiceName[DEFAULT_ARRAY_SIZE];  //服务名字,供找同类服务器使用
        pfnCNV_PARSE_PROTOCOL pfncnv_parse_protocol;
        pfnCNV_HANDLE_BUSINESS pfncnv_handle_business;
        pfnCNV_MONITOR_CALLBACK pfncnv_monitor_callback;
        pfnCNV_STATISTICS_CALLBACK pfncnv_statistics_callback;
    } CALLBACK_STRUCT_T;

    typedef  struct  __SERVER_SOCKET_DATA
    {
        int  isReqLogin;   //是否发送登录请求
        int  isRecvSvrData;  //服务端向客户端推送数据
        int nMaxReconTimes;  //最大重连次数  0 ~ 7
        int nTimeOut;  //连接超时时长,microsecond  0 ~ 70000
        int  nPort;   //端口
        int  nHeartBeatLen;   //心跳包长度
        char strProtocol[16];   //协议 tcp/unixsocket,默认tcp
        char strUnixDomainPath[128];   //unixsocket domain路径
        char *pHeartBeat;    //心跳包数据
        char strServerIp[DEFAULT_ARRAY_SIZE];   //服务器IP
        char strServiceName[DEFAULT_ARRAY_SIZE];  //服务名字,供找同类服务器使用
        char strLoginReqInfo[256];     //登录请求信息
        CALLBACK_STRUCT_T tCallback;    //设置回调函数结构体
    } SERVER_SOCKET_DATA;

    //HANDLE_CALLBACK_T   IO -> HANDLE
    typedef  struct  __IO_TO_HANDLE_DATA
    {
        int lDataLen;     //数据长度
        int lConnectID;    //短连接ID
        char strServIp[DEFAULT_ARRAY_SIZE];   //服务器IP
        unsigned int ulPort;         //服务器端口
        char *pDataSend;   // 解析后可发送的数据
        int handle_io_eventfd;  //handle唤醒io
        int io_thread_index;  //IO线程ID
        CNV_BLOCKING_QUEUE *handle_io_msgque;  // handle写io消息队列
        pfnCNV_HANDLE_BUSINESS pfncnv_handle_business;  //业务处理回调函数
        pfnSEND_FAILED_CALLBACK pfnsend_failed_callback;  //发送失败对调函数
    } IO_TO_HANDLE_DATA;

    //HANDLE_RESPOND_T   HANDLE -> IO
    typedef  struct  __HANDLE_TO_IO_DATA
    {
        int lAction;   //请求动作(1:发送服务请求 2:客户端应答 3:关闭客户端)
        int lConnectID;    //连接ID
        int lDataLen;     //数据长度
        char strServIp[DEFAULT_ARRAY_SIZE];   //服务器IP
        unsigned int ulPort;         //服务器端口
        char *pDataSend;  //要发送的数据
        void *pCallbackPara;   //回调参数
        int io_thread_index;  //IO线程ID
        pfnSEND_FAILED_CALLBACK pfnsend_failed_callback;   //发送失败回调函数
    } HANDLE_TO_IO_DATA;

    //STATISTICS QUEUE DATA
    typedef struct __STATISTICS_QUEQUE_DATA
    {
        char *pData;
        int32_t nDataLen;
    } STATISTICS_QUEQUE_DATA;

    //TIMER STRUCT
    typedef struct  __TIMER_STRUCT
    {
        uint32_t  value_sec;
        uint64_t  value_nsec;
        uint32_t  interval_sec;
        uint64_t  interval_nsec;
    } TIMER_STRUCT;

    //timer task
    typedef  struct __TIMER_TASK
    {
        K_CHAR strTaskName[64];
        TIMER_STRUCT tTimer;
    } TIMER_TASK;

    //timer tasks
    typedef  struct __TIMER_TASKS
    {
        K_INT32 nTaskNumber;
        TIMER_TASK  szTimerTask[16];
    } TIMER_TASKS;

    //handle timer task
    typedef  struct __HANDLE_TIMER_TASK
    {
        TIMER_STRUCT  tTimer;
        char strTaskName[64];
        pfnCNV_HANDLE_CALLBACK  pfn_timertask_cb;   //定时任务回调函数
    } HANDLE_TIMER_TASK;

    //handle线程参数
    typedef  struct __HANDLE_PARAMS
    {
        CNV_UNBLOCKING_QUEUE queParamFrameUse;   //框架handle使用的
        void *pBusinessParams;    //业务使用的
    } HANDLE_PARAMS;

#ifdef __cplusplus
};
#endif

#endif   // __COMMON_TYPE_H__