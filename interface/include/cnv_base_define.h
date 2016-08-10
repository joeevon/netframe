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

#ifndef  __CNV_BASE_DEFINE_H__
#define   __CNV_BASE_DEFINE_H__

#ifdef __cplusplus
extern "C"
{
#endif


#define ERRORCODE_BASE          100000

//CRC校验用到的错误码起始值
#define ERRORCODE_CRC32_BASE        (ERRORCODE_BASE + 8000)

//网络通讯公共接口用到的错误码定义
    typedef enum __enumNAVI_CRC32_ERRORCODE
    {
        ERRORCODE_CRC32_PARAMS_INVALID  = (ERRORCODE_CRC32_BASE + 1),
        ERRORCODE_CRC32_MALLOC_FAILED   = (ERRORCODE_CRC32_BASE + 2),

    }
    enumNAVI_CRC32_ERRORCODE;


#define  MAX_PATH 260  //路径最大长度    

    //错误码枚举定义
    typedef enum __enumCNV_BASE_ERROR
    {
        CNV_ERR_OK             = 0,     //服务成功
        CNV_ERR_FAIL           = 1,     //服务失败
        CNV_ERR_MALLOC         = 2,     //内存申请失败
        CNV_ERR_NET_FAIL       = 3,     //网络模块错误
        CNV_ERR_PATH               = 4,     //路径异常
        CNV_ERR_CONFIG          = 5,     //配置不正确
        CNV_ERR_HASHMAP_INIT  = 6,     //hashtable初始化错误
        CNV_ERR_EVENTFD  = 7,     //创建eventfd句柄失败
        CNV_ERR_INIT_QUEUE  = 8,     //初始化队列失败
        CNV_ERR_PUSH_QUEUE  = 9,     //插入队列失败
        CNV_ERR_PARAM  = 10,     //参数错误
        CNV_ERR_WRITE  = 11,     //参数错误
        CNV_ERR_SELECT_THREAD  = 12,     //选择线程出错
        CNV_ERR_HASHMAP_GET  = 13,     //HASHMAP取值出错
        CNV_ERR_FILE = 14,     //文件操作失败
        CNV_ERR_ACCEPT_IO_SOCKET = 15,     //accept向io写udp socket
        CNV_ERR_REQ_LOGIN = 16,     //请求登录错误
        CNV_ERR_OPEN_FILE = 17,  //打开文件失败
        CNV_ERR_READ_FILE = 18,  //读取文件错误
    }
    CNV_BASE_ERROR;


#define BASE 65521UL    /* largest prime smaller than 65536 */
#define NMAX 5552

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

#ifdef __cplusplus
};
#endif

#endif   // __CNV_BASE_DEFINE_H__
