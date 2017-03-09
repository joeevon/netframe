#ifndef __CNV_THREAD_SYS_H__
#define __CNV_THREAD_SYS_H__

#include "cnv_core_typedef.h"
#include <sys/time.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif
/////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
// 事件、临界区、互斥量 等系统相关接口
//等待的对象的类型
    typedef enum __enumNAVI_PLAT_WAITOBJECTTYPE
    {
        WOT_EVENT = 0,      // 事件对象
        WOT_THREAD = 1,     // 线程对象

    }
    enumNAVI_PLAT_WAITOBJECTTYPE;

    /*=======================================
    * 功能：
    *       创建事件，事件有两种状态：有信号状态 ，无信号状态,用于过程同步机制
    * 参数:
    *   in_lManualFlag [in] : 是手动还是自动，0 - 自动，事件会自动复位，1 - 手动，事件不会自动复位，必须手动调用Reset函数
    *   in_lInitState [in] : 事件的初始状态 ，0 - 无信号 ，1 - 有信号
    *   out_pEventHandle [out] : 输出创建的事件句柄
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_CreateEvent(int in_lManualFlag, int in_lInitState, K_HANDLE *out_pEventHandle);

    /*=======================================
    * 功能：
    *       销毁事件
    * 参数:
    *   in_EventHandle [in] : 要销毁的事件句柄
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_DeleteEvent(K_HANDLE in_EventHandle);

    /*=======================================
    * 功能：
    *       设置事件，使事件的状态变成有信号
    * 参数:
    *   in_EventHandle [in] : 要设置的事件句柄
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_SetEvent(K_HANDLE in_EventHandle);

    /*=======================================
    * 功能：
    *       重置事件，使事件的状态变成无信号状态
    * 参数:
    *   in_EventHandle [in] : 要重置的事件
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_ResetEvent(K_HANDLE in_EventHandle);

    /*=======================================
    * 功能：
    *       超时等待一个事件
    * 参数:
    *   in_lType [in] : 等待的类型，为了与异平台兼容（windows平台不需要此参数）
    *   in_EventHandle [in] ：要等待的事件
    *   in_lWaitTime [in] : 超时设置 ，毫秒, -1表示永远等待下去
    *   out_plResult [out]: 返回的等待的结果，0 - 等到了 ，1 - 超时了
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_WaitEvent(enumNAVI_PLAT_WAITOBJECTTYPE in_lType, K_HANDLE in_EventHandle, int in_lWaitTime, int *out_plResult);

    /*=======================================
    * 功能：
    *       使线程休眠in_lSleepTime毫秒
    * 参数:
    *   in_lSleepTime [in] : 要休眠的时间 （毫秒）
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_Sleep(int in_lSleepTime);

    /*=======================================
    * 功能：
    *       取得当前时刻点，开机以来的毫秒数
    * 参数:
    *   out_pulTickCount [out] : 输出的毫秒数
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_Clock(unsigned int *out_pulTickCount);

//////////////////////////////////////////////////////////////////////////
// 时间操作

// 基本时间信息结构
    typedef struct tagK_TIME
    {
        unsigned short unYear;
        unsigned short unMonth;
        unsigned short unDay;
        unsigned short unHour;
        unsigned short unMinute;
        unsigned short unSecond;
        unsigned short unWeek;  //0-星期日，（1~6）-星期（1~6）
        unsigned short unR;     // 保留

    } K_TIME, *PK_TIME;

    /*=======================================
    * 功能：
    *       取得本地时间
    * 参数:
    *   out_pLocTime [out] : 输出的本地时间
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_GetLocalTime(K_TIME *out_pLocTime);

    /*=======================================
    * 功能：
    *       取得可执行文件所在的目录（完整路径）
    * 参数:
    *   out_pszPath [out] : 输出的路劲，空间由外部分配
    *   in_lBufSize [in] : out_pszPath缓冲区的大小
    * 返回值:
    *   ERR_NONE   => 成功
    *   other      => 失败(参考错误代号)
    ========================================*/
    int hmi_plat_GetExecutePath(char *out_pszPath, int in_lBufSize);


/////////////////////////////////////////////////////////////////////////////////////////////////////////////
#ifdef __cplusplus
}
#endif

#endif //__CNV_THREAD_SYS_H__
