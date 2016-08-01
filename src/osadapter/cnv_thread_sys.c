#include "cnv_thread_sys.h"
#include "cnv_thread_basic.h"
#include "cnv_comm.h"
#include <pthread.h>
#include <signal.h>
#include <semaphore.h>
#include <stdlib.h>
#include <stdint.h>

typedef struct __tagNAVI_PLAT_NIX_EVENT
{
    sem_t            semaphore;         //linux用信号量来模拟事件
    int          eventType;         //事件的类型    0：自动事件     1：手动事件

} NAVI_PLAT_NIX_EVENT, *PNAVI_PLAT_NIX_EVENT;

/*=======================================
* 功能：
*       创建事件，事件有两种状态：有信号状态 ，无信号状态,用于过程同步机制
* 参数:
*   in_lManualFlag [in] : 是手动还是自动，0 - 自动，事件会自动复位，1 - 手动，事件不会自动复位，必须手动调用Reset函数
*   in_lInitState [in] : 事件的初始状态 ，0 - 无信号 ，1 - 有信号
*   out_pEventHandle [out] : 输出创建的事件句柄
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_CreateEvent(int in_lManualFlag, int in_lInitState, K_HANDLE *out_pEventHandle)
{
    int nRet = 0;

    NAVI_PLAT_NIX_EVENT *pEvent = (NAVI_PLAT_NIX_EVENT *)cnv_comm_Malloc(sizeof(NAVI_PLAT_NIX_EVENT));
    if(in_lInitState == 0)          //初始化信号量为无信号状态
    {
        nRet = sem_init(&(pEvent->semaphore), 0, 0);
    }
    else                            //初始化信号量为有信号状态
    {
        nRet = sem_init(&(pEvent->semaphore), 0, 1);
    }

    if(0 != nRet)
    {
        cnv_comm_Free(pEvent);
        return ERR_PLAT_CREATE_EVENT;
    }
    pEvent->eventType = in_lManualFlag;
    *out_pEventHandle = (K_HANDLE)pEvent;

    return K_SUCCEED;
}

/*=======================================
* 功能：
*       销毁事件
* 参数:
*   in_EventHandle [in] : 要销毁的事件句柄
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_DeleteEvent(K_HANDLE in_EventHandle)
{
    NAVI_PLAT_NIX_EVENT *pEvent  = (NAVI_PLAT_NIX_EVENT *)in_EventHandle;
    if(pEvent != K_NULL)
    {
        sem_destroy(&(pEvent->semaphore));
        cnv_comm_Free(pEvent);
    }
    return K_SUCCEED;
}

/*=======================================
* 功能：
*       设置事件，使事件的状态变成有信号
* 参数:
*   in_EventHandle [in] : 要设置的事件句柄
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_SetEvent(K_HANDLE in_EventHandle)
{
    NAVI_PLAT_NIX_EVENT *pEvent  = (NAVI_PLAT_NIX_EVENT *)in_EventHandle;
    if(pEvent != K_NULL)
    {
        sem_post(&(pEvent->semaphore));
    }
    return K_SUCCEED;
}

/*=======================================
* 功能：
*       重置事件，使事件的状态变成无信号状态
* 参数:
*   in_EventHandle [in] : 要重置的事件
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_ResetEvent(K_HANDLE in_EventHandle)
{
    NAVI_PLAT_NIX_EVENT *pEvent  = (NAVI_PLAT_NIX_EVENT *)in_EventHandle;
    if(pEvent != K_NULL)
    {
        sem_wait(&(pEvent->semaphore));
    }
    //while(!sem_trywait(&(event->semaphore)));  // ???
    return K_SUCCEED;
}

/*=======================================
* 功能：
*       超时等待一个事件
* 参数:
*   in_lType [in] : 等待的类型，为了与异平台兼容（windows平台不需要此参数）
*   in_EventHandle [in] ：要等待的事件
*   in_lWaitTime [in] : 超时设置 ，毫秒, -1表示永远等待下去
*   out_plResult [out]: 返回的等待的结果，1 - 等到了 ，0 - 超时了
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_WaitEvent(enumNAVI_PLAT_WAITOBJECTTYPE in_lType, K_HANDLE in_EventHandle, int in_lWaitTime, int *out_plResult)
{
    if(WOT_THREAD == in_lType)
    {
        unsigned int lastTime = 0;
        unsigned int currentTime = 0;
        unsigned int runTime = 0;
        K_HANDLE pThreadHandler = in_EventHandle;

        hmi_plat_Clock(&currentTime);
        lastTime = currentTime;

        runTime = currentTime - lastTime;
        while(runTime < in_lWaitTime)
        {
            int nRet = hmi_plat_IsThreadAlive(pThreadHandler);
            if(nRet == 1)
            {
                hmi_plat_Sleep(20);
                hmi_plat_Clock(&currentTime);
                runTime = currentTime - lastTime;

                if(in_lWaitTime == -1)
                {
                    in_lWaitTime = runTime + 1;
                }

                continue;
            }
            else
            {
                *out_plResult = 1;
                return K_SUCCEED;
            }
        }

        *out_plResult = 0;
    }
    else if(WOT_EVENT == in_lType)
    {
        NAVI_PLAT_NIX_EVENT *pEvent  = (NAVI_PLAT_NIX_EVENT *)in_EventHandle;
        struct timespec timeout;
        struct timeval tt;

        gettimeofday(&tt, K_NULL);
        timeout.tv_sec = tt.tv_sec;
        timeout.tv_nsec = tt.tv_usec + in_lWaitTime * 1000;//此时tv_nsec当中间变量用，存的是微妙值
        timeout.tv_sec += timeout.tv_nsec / (1000 * 1000);
        timeout.tv_nsec %= (1000 * 1000);               //去掉大于一秒的部分
        timeout.tv_nsec *= 1000;                    //微妙转为纳秒

        int nRet = sem_timedwait(&(pEvent->semaphore), &timeout);

        if(nRet != 0)
        {
            *out_plResult = 0;
        }
        else
        {
            if(1 == pEvent->eventType)
            {
                sem_post(&(pEvent->semaphore));
            }
            *out_plResult = 1;
        }
    }
    return K_SUCCEED;
}

/*=======================================
* 功能：
*       使线程休眠in_lSleepTime毫秒
* 参数:
*   in_lSleepTime [in] : 要休眠的时间 （毫秒）
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_Sleep(int in_lSleepTime)
{
    usleep(in_lSleepTime * 1000);
    return K_SUCCEED;
}

/*=======================================
* 功能：
*       取得当前时刻点，开机以来的毫秒数
* 参数:
*   out_pulTickCount [out] : 输出的毫秒数
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_Clock(unsigned int *out_pulTickCount)
{
    if(!out_pulTickCount) return ERR_PLAT_PARAM;

    static unsigned int isFist = 1;
    static unsigned int ulTodayTime = 0;

    double dCurrentTime = 0;
    time_t  nowUTCTime = 0;

    time(&nowUTCTime);

    if(isFist)
    {
        ulTodayTime = nowUTCTime - (nowUTCTime % (24 * 60 * 60));
        isFist = 0;
        dCurrentTime = (nowUTCTime % (24 * 60 * 60)) * 1000;
    }
    else
    {
        dCurrentTime = (nowUTCTime - ulTodayTime) * 1000;
    }

    //处理溢出
    if(dCurrentTime > ((double)UINT32_MAX))
    {
        isFist = 1;
        dCurrentTime = (nowUTCTime % (24 * 60 * 60)) * 1000;
    }

    *out_pulTickCount = dCurrentTime;

    return K_SUCCEED;
}

/*=======================================
* 功能：
*       取得本地时间
* 参数:
*   out_pLocTime [out] : 输出的本地时间
* 返回值:
*   K_SUCCEED   => 成功
*   other      => 失败(参考错误代号)
========================================*/
int hmi_plat_GetLocalTime(K_TIME *out_pLocTime)
{
    time_t  nowUTCTime = 0;
    struct tm *nowLocalTime = K_NULL;
    time(&nowUTCTime);
    nowLocalTime = localtime(&nowUTCTime);

    out_pLocTime->unYear = nowLocalTime->tm_year + 1900;
    out_pLocTime->unMonth = nowLocalTime->tm_mon + 1;
    out_pLocTime->unDay = nowLocalTime->tm_mday;
    out_pLocTime->unHour = nowLocalTime->tm_hour;
    out_pLocTime->unMinute = nowLocalTime->tm_min;
    out_pLocTime->unSecond = nowLocalTime->tm_sec;
    out_pLocTime->unWeek = nowLocalTime->tm_wday;

    return K_SUCCEED;
}
