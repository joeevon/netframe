#ifndef __CNV_STRUCTDEFINE_H__
#define __CNV_STRUCTDEFINE_H__

#include "common_type.h"
#include "cnv_queue.h"
#include "cnv_base_define.h"
#include "cnv_fifo.h"
#include "cnv_blocking_queue.h"
#include "cnv_lock_free_queue.h"
#include <stdint.h>
#include <netinet/in.h>
#include <sys/socket.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define  MAX_INT_VALUE  2147483647  // int 最大值     
#define  MAX_LISTEN_PORT  32    // 最大监听端口个数   
#define  MAX_IO_THREAD    32    // I/O线程个数    
#define  MAX_HANDLE_THREAD  32    // Handle线程个数     
#define  MAX_AUXILIARY_THREAD  32    //Auxiliary线程个数     
#define  DEFAULT_FIFO_CAPCITY  65536    //默认FIFO大小，必须为2的整数次方  2^16
#define  DEFAULT_QUEUE_CAPCITY  300000    //默认queue大小    
#define  DEFAULF_EPOLL_SIZE   30000      //epoll size   
#define  DEFAULF_FILTED_CLIENTS   32
#define  MAX_PACKET_SZIE  1024*1024   //每个数据包的最大值  1M
#define  REMAIN_BUFFER_SIZE  1024     //remain size

    typedef enum __enumDISTRIBUTE_TYPE
    {
        DISTRIBUTE_TYPE_WEIGHT = 1,     //权重
        DISTRIBUTE_TYPE_HASH = 2,     //哈希
        DISTRIBUTE_TYPE_PRESSURE = 3,     //负载
    }
    enumDISTRIBUTE_TYPE;

    //HANDLE CONTEXT
    typedef struct  __HANDLE_THREAD_CONTEXT
    {
        int  lthreadindex;
        char  threadname[20];
        int  Epollfd;
        int  io_handle_eventfd;  //io唤醒handle
        void *HashTimerAdd;    //key:unique key   valude:cb function
        void *HashTimerTask;    //key:taskname   valude:cb function
        CNV_UNBLOCKING_QUEUE *queDistribute;    //存放负载的线程
        CNV_UNBLOCKING_QUEUE *queParamFrames;   //框架handle使用的参数,有业务初始化好传进,所以由业务释放
        LOCKFREE_QUEUE  io_handle_msgque;
        CNV_UNBLOCKING_QUEUE  queuerespond;   //返回IO的数据
        void *pHandleParam;    //handle参数
    } HANDLE_THREAD_CONTEXT;

    //HANDLE  ITEM
    typedef  struct  __HANDLE_THREAD_ITEM
    {
        char  strThreadName[DEFAULT_ARRAY_SIZE];
        int  lThreadIndex;     //开启线时自定义的序号
        int nIsStasistics;   //是否统计
        pthread_t  ulThreadId;    //线程ID
        void   *ThreadHandle;
        HANDLE_THREAD_CONTEXT  *pHandleContext;  //线程环境
    } HANDLE_THREAD_ITEM;

    // HANDLE CONFIG
    typedef  struct  __NETFRAME_CONFIG_HANDLE
    {
        int  lNumberOfThread;
        int  lIoHandleMsgSize;
        HANDLE_THREAD_ITEM  szConfigHandleItem[MAX_HANDLE_THREAD];
    } NETFRAME_CONFIG_HANDLE;

    // IO CONTEXT
    typedef struct __IO_THREAD_CONTEXT
    {
        char strStartTime[64];
        int  nIsStasistics;   //是否统计
        int  Epollfd;
        int  nDistributeType;   //分发类型  1、权重   2、哈希   3、负载
        int  timerfd_socketclear;     //socket清理
        int  timerfd_hearbeat;     //心跳
        int  timerfd_monitor;     //服务监视
        int  accept_io_eventfd;  //accept唤醒io
        int  handle_io_eventfd;  //handle唤醒io
        unsigned int nLastConnectTime;  //上一次计时时间
        unsigned int nReconTimes;  //已经重连次数
        char  threadname[16];
        short  threadindex;
        int  SeedOfKey;
        int  szIoRespHandle[32];   //io hanlde对应的线程关系
        int nHandleThreadCount;  //io持有的hanlde线程个数
        void  *HashConnidFd;   //key:connid  valude:socket element
        void  *HashAddrFd;    //key:addr(ip:port)   valude:socket element
        CNV_UNBLOCKING_QUEUE *queServer;   //服务器配置
        CNV_UNBLOCKING_QUEUE *queDistribute;    //存放负载的线程
        cnv_fifo *accept_io_msgque;  //accept -> io
        CNV_BLOCKING_QUEUE  *handle_io_msgque;    //handle -> io
        CNV_UNBLOCKING_QUEUE  *handle_msgque_one;    //handle -> io  self
        CNV_UNBLOCKING_QUEUE  *handle_msgque_two;    //handle -> io  self
        HANDLE_THREAD_CONTEXT  *szHandleContext[MAX_HANDLE_THREAD];
        pfnCNV_MONITOR_CALLBACK  pfncnv_monitor_callback;
        MONITOR_ELEMENT tMonitorElement;
    } IO_THREAD_CONTEXT;

    // IO  ITEM
    typedef  struct  __IO_THREAD_ITEM
    {
        int  lThreadIndex;     //开启线时自定义的序号
        int  nIsStasistics;   //是否统计
        char  strThreadName[DEFAULT_ARRAY_SIZE];
        char  strDistribution[128];
        char  strAlgorithm[DEFAULT_ARRAY_SIZE];
        pthread_t  ulThreadId;    //线程ID
        void   *ThreadHandle;
        IO_THREAD_CONTEXT  *pIoThreadContext;
    } IO_THREAD_ITEM;

    // IO  CONFIG
    typedef  struct  __NETFRAME_CONFIG_IO
    {
        int lNumberOfThread;
        int lHandleIoMsgSize;
        IO_THREAD_ITEM szConfigIOItem[MAX_IO_THREAD];
    } NETFRAME_CONFIG_IO;

    //HOST
    typedef struct  __NETFRAME_HOST
    {
        char  strHost[DEFAULT_ARRAY_SIZE];
    } NETFRAME_HOST;

    //HOSTS
    typedef struct  __NETFRAME_HOSTS
    {
        int  lNumOfHosts;
        NETFRAME_HOST tHosts[DEFAULF_FILTED_CLIENTS];
    } NETFRAME_HOSTS;

    // ACCEPT  ITEM
    typedef  struct  __ACCEPT_THREAD_ITEM
    {
        char  strHost[DEFAULT_ARRAY_SIZE];        //服务ip
        unsigned int  ulPort;     //端口
        char  strUnixDomainPath[256];     //unix.domain路径
        char  strTransmission[DEFAULT_ARRAY_SIZE];     //传输协议
        char  strDistribution[DEFAULT_ARRAY_SIZE];        //负载分发
        char  strAlgorithm[DEFAULT_ARRAY_SIZE];         //分发比例
        NETFRAME_HOSTS tAllowedClients;        //白名单
        IO_THREAD_CONTEXT  *szIOThreadContext[MAX_IO_THREAD];   //所有IO线程的CONTEXT
        CALLBACK_STRUCT_T  tCallback;
        CNV_UNBLOCKING_QUEUE  *queDistribute;    //存放负载的线程
    } ACCEPT_THREAD_ITEM;

    // ACCEPT CONTEXT
    typedef struct  __ACCEPT_THREAD_CONTEXT
    {
        int TcpListenSocket;
        int UdpSocket;
        int UnixListenSocket;
        int  Epollfd;
        int  accept_eventfd;  //其他线程唤醒
        void *HashFdListen;      //  key:socket  value:listen item
        CALLBACK_STRUCT_T  tStatisCallback;  //数据统计回调函数
        CNV_UNBLOCKING_QUEUE *queEventfds;  //需要唤醒队列的fd
        IO_THREAD_CONTEXT *pIoThreadContexts;
        ACCEPT_THREAD_ITEM  *pConfigAcceptItem;
        LOCKFREE_QUEUE  statis_msgque;  //统计数据的队列
        CNV_UNBLOCKING_QUEUE queStatisData;  //统计数据返回的消息队列
    } ACCEPT_THREAD_CONTEXT;

    // ACCEPT CONFIG
    typedef  struct  __NETFRAME_CONFIG_ACCEPT
    {
        int  lNumberOfPort;
        pthread_t  ulThreadId;    //线程ID
        void   *ThreadHandle;
        ACCEPT_THREAD_CONTEXT  *pAcceptContext;
        ACCEPT_THREAD_ITEM  szConfigAcceptItem[MAX_LISTEN_PORT];
    } NETFRAME_CONFIG_ACCEPT;

    //  IO_CALLBACK_T   ACCEPT -> IO
    typedef  struct  __ACCEPT_TO_IO_DATA
    {
        int  fd;
        char strClientIp[DEFAULT_ARRAY_SIZE];
        unsigned short uClientPort;
        char  strTransmission[DEFAULT_ARRAY_SIZE];     // 传输协议,IO需根据传输协议来收数据
        char  strProtocol[DEFAULT_ARRAY_SIZE]; //  服务服务协议
        pfnCNV_PARSE_PROTOCOL  pfncnv_parse_protocol;     //协议解析回调函数
        pfnCNV_HANDLE_BUSINESS  pfncnv_handle_business;   //业务处理回调函数
    } ACCEPT_TO_IO_DATA;

//配置文件路径
    typedef  struct  __NETFRAME_CONFIG_PATH
    {
        char   strConfigPath[MAX_PATH];         //通用服务
        char   strLogDir[MAX_PATH];                   //日志
    } NETFRAME_CONFIG_PATH;

    typedef  struct  __CLIENT_SOCKET_DATA
    {
        unsigned int  lDataRemain;       //剩下的数据长度(不完整的包)
        char  *pDataBuffer;       //数据缓冲
        char  *pMovePointer;   //移动遍历的指针
    } CLIENT_SOCKET_DATA;

    // CLIENT_SOCKET_ELEMENT
    typedef  struct  __CLIENT_SOCKET_ELEMENT
    {
        char  strTransmission[DEFAULT_ARRAY_SIZE];     //传输协议,IO需根据传输协议来收数据
        char  strProtocol[DEFAULT_ARRAY_SIZE]; //服务协议
        char strServiceName[DEFAULT_ARRAY_SIZE];  //服务名字,供找同类服务器使用
        char strClientIp[DEFAULT_ARRAY_SIZE];
        unsigned short uClientPort;
        struct sockaddr_in tClientAddr;
        struct iovec tIovecClnData;
        struct msghdr msg;       //数据内容
        char strControl[CMSG_SPACE(sizeof(struct in_pktinfo))];
        CLIENT_SOCKET_DATA SocketData;  //相关读数据
        pfnCNV_PARSE_PROTOCOL  pfncnv_parse_protocol;   //协议解析回调函数
        pfnCNV_HANDLE_BUSINESS  pfncnv_handle_business;   //业务处理回调函数
    } CLIENT_SOCKET_ELEMENT;

    typedef  struct  __SERVER_SOCKET_ELEMENT
    {
        unsigned int  lWriteRemain;
        char *pWriteRemain;
        struct sockaddr_in tServerAddr;
        struct iovec szIovecSvrData[2];
        struct msghdr msg;       //数据内容
        SERVER_SOCKET_DATA *pSvrSockData;
    } SERVER_SOCKET_ELEMENT;

    //socket element
    typedef  struct  __SOCKET_ELEMENT
    {
        void *pConnId;   //连接id指针
        int Socket;     //句柄
        int lAction;   //1.请求 2.应答
        unsigned int Time;    //时间戳
        K_BOOL bIsToclear;    //是否清理
        union
        {
            CLIENT_SOCKET_ELEMENT  tClnSockElement;
            SERVER_SOCKET_ELEMENT tSvrSockElement;
        } uSockElement;
    } SOCKET_ELEMENT;

    //cb function struct
    typedef  struct  _TIMER_TASK_STRUCT
    {
        int timerfd;
        void *arg;
        pfnCNV_HANDLE_CALLBACK  pfnHADLE_CALLBACK;
        pfnCNV_TIMER_CALLBACK pfnTIMER_CALLBACK;
    } TIMER_TASK_STRUCT;

    // 全局配置
    typedef  struct  __GLOBAL_PARAMS
    {
        NETFRAME_CONFIG_PATH   tConfigPath;
        NETFRAME_CONFIG_ACCEPT  tConfigAccept;
        NETFRAME_CONFIG_IO  tConfigIO;
        NETFRAME_CONFIG_HANDLE  tConfigHandle;
        TIMER_STRUCT  tHeartBeat;
        TIMER_STRUCT  tSocketClear;
        TIMER_STRUCT  tMonitor;
        int nMaxBufferSize;
    } GLOBAL_PARAMS;

#ifdef __cplusplus
};
#endif

#endif  //__CNV_STRUCTDEFINE_H__
