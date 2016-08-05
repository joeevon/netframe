/********************************************************
FileName:cnv_comm.c
(C) Copyright 2014 by Careland
凯立德秘密信息
Description:
    公共函数C文件
Note:
Author:  WangZhiyong
    Create Date: 2015-05-07
*********************************************************/

#include "cnv_comm.h"
#include "cnv_base_define.h"

#include <iconv.h>
#include <sys/time.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>


short NAVI_COMM_MonthDays[2][13] =
{
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366},
};

void *cnv_comm_Malloc(int lSize)
{
    return malloc(lSize);
}

void cnv_comm_Free(void *pAddr)
{
    if(pAddr)
    {
        free(pAddr);
    }
}

char *cnv_comm_StrncpyA(char *strDestination, const char *strSource, int lSize)
{
    return  strncpy(strDestination, strSource, lSize);
}

char *cnv_comm_StrcatA(char *strDestination, char *strSource)
{
    char *pdest = strDestination;
    char *psrc = strSource;
    if(pdest == 0 || psrc == 0)
        return  strDestination;
    while(*pdest != 0)
    {
        pdest++;
    }
    while(*psrc != 0)
    {
        *pdest = *psrc;
        pdest++;
        psrc++;
    }
    *pdest = 0;
    return  strDestination;
}

int cnv_comm_GetCurDirectory(char *strFilePath, char *strCurDirectory)
{
    char *strTmp = NULL;

    strTmp = strrchr(strFilePath, '/');
    if(!strTmp)
    {
        return CNV_ERR_PATH;
    }
    cnv_comm_StrncpyA(strCurDirectory, strFilePath, strTmp - strFilePath);
    strCurDirectory[strTmp - strFilePath + 1] = '\0';

    return CNV_ERR_OK;
}

int Sleep(unsigned int  ulMicroseconds)
{
    fd_set fdsck;
    FD_ZERO(&fdsck);

    struct timeval timeOut;
    timeOut.tv_sec = (long)(ulMicroseconds / 1000);
    timeOut.tv_usec = (long)((ulMicroseconds % 1000) * 1000);
    select(0, &fdsck, &fdsck, &fdsck, &timeOut);

    return  CNV_ERR_OK;
}

long cnv_comm_get_utctime()
{
    time_t loc_utc = 0;
    time(&loc_utc);
    return loc_utc;
}

char *cnv_comm_strrpl(char *strSrc, char oldchar, char newchar)
{
    char *head = strSrc;
    while(*strSrc != '\0')
    {
        if(*strSrc == oldchar)
        {
            *strSrc = newchar;
        }
        strSrc++;
    }
    return head;
}

void cnv_comm_string_trans(char *pSource, int len, char sepachar, char *pDest)
{
    int i;
    int j = 0;
    char *pDistriTmp = pSource;

    for(i = 0; i < len; i++)
    {
        if((pDistriTmp[i] > '0' && pDistriTmp[i] < '9') || pDistriTmp[i] == sepachar)
        {
            pDest[j++] = pDistriTmp[i];
        }
    }
}

char *cnv_comm_strstr(const char *pStr1, int nLen1, const char *pStr2, int nLen2)
{
    if(nLen1 <= 0)
        return NULL;

    if(nLen2 <= 0)
        return (char *)pStr1;

    int l1 = nLen1;
    const char *s1 = pStr1;

    while(l1 >= nLen2)
    {
        if(!memcmp(s1, pStr2, nLen2))
            return (char *)s1;
        l1--;
        s1++;
    }

    return NULL;
}

/*
unsigned int cnv_comm_get_utctime()
{
    unsigned int loc_utc = 0;
    time(&loc_utc);
    return loc_utc;
}
*/
long cnv_comm_get_sys_time()
{
    long time = 0;
    struct timeval now;
    gettimeofday(&now , NULL);

    time = now.tv_sec;
    time = time * 1000000;
    time += now.tv_usec;

    return time / 1000;
}

/*=========================================
功能：
    计算从1970年1月1日到 xx年1月1日的天数
参数：
    [in] 年份
返回值：
    void
==========================================*/
int cnv_comm_getUTCDays(int in_lYear)
{
    in_lYear -= 1;

    return in_lYear * 365 + (in_lYear / 4 - in_lYear / 100 + in_lYear / 400) - 719162;
}

/*=========================================
功能：
    计算年份是否润年
参数：
    [in] 年份
返回值：
    void
==========================================*/
int cnv_comm_isLeapYear(int in_lYear)
{
    if(((in_lYear % 4 == 0) && (in_lYear % 100 != 0)) || (in_lYear % 400 == 0))
    {
        return 1;
    }

    return 0;
}

long cnv_comm_get_sys_time2()
{
    long time = 0;

    struct timeval now;
    gettimeofday(&now , NULL);

    time = now.tv_sec;
    time = time * 1000000;
    time += now.tv_usec;

    return time;
}

/*=======================================
* 名称：Cnv_Time_DateTime2UTC
* 功能：
*       将日期时间转为UTC时间（自1970.1.1 0:0:0:000 以来的秒数）
* 参数:
*   in_pTime [in] : 输入的时间
*   out_pulUTC [out]：输出的对应的UTC时间
* 返回值:
*   0   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int cnv_comm_dateTime2utc(NAVI_TIME *in_pTime, unsigned int *out_pulUTC)
{
    unsigned int year = in_pTime->unYear;
    unsigned int mon = in_pTime->unMonth;
    unsigned int day = in_pTime->unDay;
    unsigned int hour = in_pTime->unHour;
    unsigned int min = in_pTime->unMinute;
    unsigned int sec = in_pTime->unSecond;

    if(0 >= (int)(mon -= 2))        /* 1..12 -> 11,12,1..10 */
    {
        mon += 12;                  /* Puts Feb last since it has leap day */
        year -= 1;
    }

    *out_pulUTC = (((
                        (unsigned int)
                        (year / 4 - year / 100 + year / 400 + 367 * mon / 12 + day) +
                        year * 365 - 719499
                    ) * 24 + hour   /* now have hours */
                   ) * 60 + min    /* now have minutes */
                  ) * 60 + sec;   /* finally seconds */

    return 0;
}

/*=======================================
* 名称：Cnv_Time_UTC2DateTime
* 功能：
*       将UTC时间转换成日期时间
* 参数:
*   in_ulUTC [in] : UTC时间
*   out_pTime [out]: 输出的日期
* 返回值:
*   0   => 成功
*   other      => 失败(参考错误代号) 时区
========================================*/
int cnv_comm_utc2DateTime(unsigned int in_ulUTC, NAVI_TIME *out_pTime)
{
    int lTemp = 0;
    int lYear = 0;
    int lMon = 0;
    int lDay = 0;
    int lHour = 0;
    int lMin = 0;
    int lSec = 0;
    int lWeekDay = 0;

    int lDayCount = 0;
    int lYearCount = 0;
    short i = 0;

    lTemp = in_ulUTC % 86400;
    lHour = lTemp / 3600;   // 时
    lTemp = lTemp % 3600;
    lMin = lTemp / 60;      // 分
    lSec = lTemp % 60;      // 秒

    lDayCount = in_ulUTC / 86400; //从1970.1.1到in_ulUTC的天数
    lWeekDay = (lDayCount + 4) % 7; // 星期几
    lYearCount = lDayCount / 365; // 大致的年数
    lYear = lYearCount + 1970;
    while(1)
    {
        if(cnv_comm_getUTCDays(lYear) > lDayCount)
        {
            break;
        }
        lYear++;
    }

    lYear -= 1;

    lTemp = lDayCount - cnv_comm_getUTCDays(lYear);
    for(i = 0; i < 13; i++)
    {
        if(NAVI_COMM_MonthDays[cnv_comm_isLeapYear(lYear)][i] > lTemp)
        {
            lMon = i;
            lDay = lTemp - NAVI_COMM_MonthDays[cnv_comm_isLeapYear(lYear)][i - 1] + 1;
            break;
        }
    }

    out_pTime->unYear = (unsigned short)lYear;
    out_pTime->unMonth = (unsigned short)lMon;
    out_pTime->unDay = (unsigned short)lDay;
    out_pTime->unHour = (unsigned short)lHour;
    out_pTime->unMinute = (unsigned short)lMin;
    out_pTime->unSecond = (unsigned short)lSec;
    out_pTime->unWeek = (unsigned short)lWeekDay;

    return 0;
}

int cnv_get_localhost(char *strNetCardName, int nIpVersion, char *strHostIp, int nHostSize)
{
    if(!strcmp(strNetCardName, "") || (nIpVersion != AF_INET && nIpVersion != AF_INET6) || !strHostIp)
    {
        return -1;
    }

    struct ifaddrs *ifAddrStruct = NULL;
    void *tmpAddrPtr = NULL;
    getifaddrs(&ifAddrStruct);

    while(ifAddrStruct != NULL)
    {
        if(!strcmp(ifAddrStruct->ifa_name, strNetCardName) && (ifAddrStruct->ifa_addr->sa_family == nIpVersion))  //ipv4
        {
            tmpAddrPtr = &((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            snprintf(strHostIp, nHostSize - 1, "%s", addressBuffer);
            return 0;
        }
        else if(!strcmp(ifAddrStruct->ifa_name, strNetCardName) && (ifAddrStruct->ifa_addr->sa_family == nIpVersion))   //ipv6
        {
            tmpAddrPtr = &((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr;
            char addressBuffer[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
            snprintf(strHostIp, nHostSize - 1, "%s", addressBuffer);
            return 0;
        }

        ifAddrStruct = ifAddrStruct->ifa_next;
    }

    return -1;
}

//XML读取
xmlXPathObjectPtr cnv_comm_xml_Get_XPathObj(xmlDocPtr doc, xmlChar *xpath, char *errmsg, int errlen)
{
    xmlXPathObjectPtr xpathObj = NULL;
    xmlXPathContextPtr xpathCtx = NULL;
    xpathCtx = xmlXPathNewContext(doc);
    if(xpathCtx == NULL)
    {
        sprintf(errmsg, "%s", "不能创建xpath");
        return xpathObj;
    }

    xpathObj = xmlXPathEvalExpression(xpath, xpathCtx);
    if(xpathObj == NULL)
    {
        sprintf(errmsg, "xpath 表达式错误,%s\n", xpath);
        xmlXPathFreeContext(xpathCtx);
        return xpathObj;
    }

    if(xmlXPathNodeSetIsEmpty(xpathObj->nodesetval))
    {
        sprintf(errmsg, "路径下没有节点,%s", xpath);
        xmlXPathFreeObject(xpathObj);
        xpathObj = NULL;
    }
    xmlXPathFreeContext(xpathCtx);

    return xpathObj;
}

//根据xpath读取XML
int cnv_comm_xml_GetValue_ByPath(void *doc, char *xpath, char *value, int len, char *errmsg, int errlen)
{
    xmlXPathObjectPtr xpathObj = NULL;
    xmlNodeSetPtr nodeset = {0};
    xmlNodePtr cur = {0};
    int size;
    char *valueptr = NULL;
    xpathObj = cnv_comm_xml_Get_XPathObj((xmlDocPtr)doc, (xmlChar *)xpath, errmsg, errlen);
    if(xpathObj == NULL)
    {

        return -1;
    }

    nodeset = xpathObj->nodesetval;
    size = nodeset ? nodeset->nodeNr : 0;
    if(size != 1)
    {
        return -2;
    }

    cur = nodeset->nodeTab[0];
    valueptr = (char *)xmlNodeGetContent(cur);
    if(valueptr)
    {
        strncpy(value, valueptr, len);
        xmlFree(valueptr);
    }
    if(xpathObj)
    {
        xmlXPathFreeObject(xpathObj);
    }
    return 0;
}
