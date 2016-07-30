/********************************************************
FileName:cnv_comm.h
(C) Copyright 2014 by Careland
凯立德秘密信息
Description:
    公共函数头文件
Note:
Author:  WangZhiyong
    Create Date: 2015-05-07
*********************************************************/
#ifndef __CNV_COMM_H__
#define __CNV_COMM_H__
#ifdef __cplusplus
extern "C"
{
#endif

#include <string.h>

    // 基本时间信息
    typedef struct __NAVI_TIME
    {
        unsigned short unYear;
        unsigned short unMonth;
        unsigned short unDay;
        unsigned short unHour;
        unsigned short unMinute;
        unsigned short unSecond;
        unsigned short unWeek;
    } NAVI_TIME;

    /*=======================================================
    功能:
       分配内存
    参数:
       [in]    lSize  内存大小
    返回值:
       分配的内存指针  成功
       其它 失败
    =========================================================*/
    extern void *cnv_comm_Malloc(int lSize);

    /*=======================================================
    功能:
       释放内存
    参数:
       [in]    pAddr  内存地址
    返回值:
       无
    =========================================================*/
    extern void cnv_comm_Free(void *pAddr);

    /*=======================================================
    功能:
        字符串拷贝
    参数:
        无
    返回值:
        原目标指针    成功
        其它 失败
    =========================================================*/
    extern char *cnv_comm_StrncpyA(char *strDestination, const char *strSource, int lSize);

    /*=======================================================
    功能:
        字符串拷贝
    参数:
        无
    返回值:
        原目标指针    成功
        其它 失败
    =========================================================*/
    extern char  *cnv_comm_StrcatA(char *strDestination, char *strSource);

    /*=======================================================
    功能:
        获取当前目录
    参数:
        [in]    strFilePath  完整文件路径
        [out]      strDirectory  当前目录

    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int cnv_comm_GetCurDirectory(char *strFilePath, char *strCurDirectory);

    /*=======================================================
    功能:
        线程休眠
    参数:
        [in]
            ulMicroseconds   休眠时间(毫秒)
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int Sleep(unsigned int  ulMicroseconds);

    /*=======================================================
    功能:
        获取系统时间戳
    参数:
        [in]
            ulMicroseconds   休眠时间(毫秒)
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern long cnv_comm_get_utctime();

    /*=======================================================
    功能:
       字符替换
    参数:
       [in]
           ulMicroseconds   休眠时间(毫秒)
    返回值:
       0    成功
       其它 失败
    =========================================================*/
    extern  char *cnv_comm_strrpl(char *strSrc, char oldchar, char newchar);

    /*=======================================================
    功能:
      字符串转化
    参数:
      [in]
          ulMicroseconds   休眠时间(毫秒)
    返回值:
      0    成功
      其它 失败
    =========================================================*/
    extern void cnv_comm_string_trans(char *pSource, int len, char sepachar, char *pDest);

    extern char *cnv_comm_strstr(const char *pStr1, int nLen1, const char *pStr2, int nLen2);

    //功能：获取系统时间戳
    //extern unsigned int cnv_comm_get_utctime();

    extern long cnv_comm_get_sys_time();
    extern long cnv_comm_get_sys_time2();

    //功能：将时间转为UTC
    extern int cnv_comm_dateTime2utc(NAVI_TIME *in_pTime, unsigned int *out_pulUTC);

    //功能：将UTC转为时间
    extern int cnv_comm_utc2DateTime(unsigned int in_ulUTC, NAVI_TIME *out_pTime);

    /*获取本机ip ipv4
    strNetCardName  网卡名,例:eth0
    nIpVersion  ip版本 : AF_INET  AF_INET6
    */
    int cnv_get_localhost(char *strNetCardName, int nIpVersion, char *strHostIp, int nHostSize);

    //功能：读取xml里面的内容
    extern int cnv_comm_xml_GetValue_ByPath(void *doc, char *xpath, char *value, int len, char *errmsg, int errlen);

#ifdef __cplusplus
};
#endif

#endif  //__CNV_COMM_H__
