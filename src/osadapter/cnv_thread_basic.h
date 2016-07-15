/****************************
    FileName:cnv_thread_basic.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        cnv_thread_basic 头文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-11
    *****************************/
#ifndef __CNV_THREAD_BASIC_H__
#define __CNV_THREAD_BASIC_H__

#ifdef __cplusplus
extern "C" {
#endif

#define ERR_PLAT_BASE                     (21000)

// 本模块的错误定义
#define ERR_PLAT_PARAM                    (ERR_PLAT_BASE + 1) //非法参数
#define ERR_PLAT_FILE_OPEN                (ERR_PLAT_BASE + 2) //打开文件失败
#define ERR_PLAT_FILE_COPY                (ERR_PLAT_BASE + 3) //拷贝文件失败
#define ERR_PLAT_FILE_DELETE              (ERR_PLAT_BASE + 4) //删除文件失败
#define ERR_PLAT_FILE_MOVE                (ERR_PLAT_BASE + 5) //移动文件失败
#define ERR_PLAT_FILE_CREATEDIR           (ERR_PLAT_BASE + 6) //创建文件失败
#define ERR_PLAT_FILE_SET_ATTR            (ERR_PLAT_BASE + 7) //设置文件属性失败
#define ERR_PLAT_OUT_OF_RANGE             (ERR_PLAT_BASE + 8) //数据越界
#define ERR_PLAT_CREATE_THREAD            (ERR_PLAT_BASE + 9) //创建线程失败
#define ERR_PLAT_STOP_THREAD              (ERR_PLAT_BASE + 10)//停止线程失败
#define ERR_PLAT_CREATE_EVENT             (ERR_PLAT_BASE + 11)//创建事件对象失败
#define ERR_PLAT_MEMORY_ALLOC             (ERR_PLAT_BASE + 12)//内存分配失败
#define ERR_PLAT_NET_DIAL                 (ERR_PLAT_BASE + 13)//网络拨号失败

#include "cnv_thread.h"

#ifdef __cplusplus
}
#endif

#endif  //__CNV_THREAD_BASIC_H__
