#include "cnv_thread_basic.h"
#include "cnv_thread_sys.h"
#include "cnv_comm.h"
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef struct __tagNAVI_PLAT_NIX_THREAD_HANDLE
{
    pthread_t   hThread; // 线程句柄
    char   cStopFlag; // 是否退出线程过程的标志，主要是为了防止线程被强制终止，这个东东只对于那些循环执行的线程过程有用
    char   cReserve1; // 保留
    char   cReserve2; // 保留
    char   cReserve3; // 保留

} NAVI_PLAT_NIX_THREAD_HANDLE, *PNAVI_PLAT_NIX_THREAD_HANDLE;

typedef  unsigned int(*fThreadPro)(void *lpThreadParameter);

typedef struct __tagNAVI_PLAT_NIX_THREAD_PARAMS
{
    pfnCNV_PLAT_THREAD_RECALL fThreadFunc;
    void *params;

} NAVI_PLAT_NIX_THREAD_PARAMS;


#define MYSIG_MSG SIGUSR1+2

//信号处理函数，每一个线程收到MYSIG_MSG信号后就调用它
static void SigalFunc(int sig)
{
    //置线程状态为unjoined
    pthread_detach(pthread_self());
    pthread_exit(K_NULL);
}

//线程函数，在里面调用处理函数
static unsigned int CreateThreadFun(void *pParam)
{
    if(K_NULL == pParam) return ERR_PLAT_PARAM;

    //设置MYSIG_MSG信号的处理函数
    signal(MYSIG_MSG, (sig_t)SigalFunc);

    NAVI_PLAT_NIX_THREAD_PARAMS tempParam = {0};
    memcpy(&tempParam, pParam, sizeof(NAVI_PLAT_NIX_THREAD_PARAMS));
    cnv_comm_Free(pParam);//释放创建线程时候产生的内存

    (tempParam.fThreadFunc)(tempParam.params);

    return K_SUCCEED;
}

int hmi_plat_CreateThread(pfnCNV_PLAT_THREAD_RECALL in_pFun, void *in_param,  int in_attribute, pthread_t *out_threadId, K_HANDLE *out_pThread)
{
    NAVI_PLAT_NIX_THREAD_HANDLE *pThread = (NAVI_PLAT_NIX_THREAD_HANDLE *)cnv_comm_Malloc(sizeof(NAVI_PLAT_NIX_THREAD_HANDLE));

    *out_pThread = pThread;
    if(!pThread || !in_pFun || !out_threadId) return ERR_PLAT_PARAM;

    memset(pThread, 0, sizeof(NAVI_PLAT_NIX_THREAD_HANDLE));

    NAVI_PLAT_NIX_THREAD_PARAMS *pParams = (NAVI_PLAT_NIX_THREAD_PARAMS *)cnv_comm_Malloc(sizeof(NAVI_PLAT_NIX_THREAD_PARAMS));
    pParams->fThreadFunc = in_pFun;
    pParams->params = in_param;

    int nRet = pthread_create(&pThread->hThread, K_NULL, (void *)CreateThreadFun, pParams);
    if(0 != nRet)
    {
        cnv_comm_Free(pParams);
        return ERR_PLAT_CREATE_THREAD;
    }
    *out_threadId = pThread->hThread;
    return K_SUCCEED;
}

int hmi_plat_StopThread(K_HANDLE  in_pthread, int in_lWaitTime)
{
    int lResult = 0;

    NAVI_PLAT_NIX_THREAD_HANDLE *pThread = (NAVI_PLAT_NIX_THREAD_HANDLE *)in_pthread;

    if(K_NULL == pThread) return ERR_PLAT_PARAM;
    if(0 == pThread->hThread) return K_SUCCEED;

    pThread->cStopFlag = 1;

    hmi_plat_WaitEvent(WOT_THREAD, (K_HANDLE)pThread, in_lWaitTime, &lResult);
    if(lResult == 0)
    {
        pthread_detach(pThread->hThread);
    }

    pThread->hThread = 0;    //
    cnv_comm_Free(pThread);

    return K_SUCCEED;
}

int hmi_plat_IsThreadAlive(K_HANDLE in_pthread)
{
    NAVI_PLAT_NIX_THREAD_HANDLE *pThread = (NAVI_PLAT_NIX_THREAD_HANDLE *)in_pthread;

    if(K_NULL == pThread || 0 == pThread->hThread) return 0;

    if(0 == pthread_kill(pThread->hThread, 0))
    {
        return 1;
    }
    return 0;
}

int hmi_plat_GetCurrentThreadId()
{
    unsigned int currentThreadId = 0;
    currentThreadId = pthread_self();
    return (int)currentThreadId;
}

int hmi_plat_SetThreadPriority(K_HANDLE in_pthread, enumNAVI_AGENT_THREAD_PRIORITY in_ePriority)
{
    NAVI_PLAT_NIX_THREAD_HANDLE *pThread = (NAVI_PLAT_NIX_THREAD_HANDLE *)in_pthread;

    if(K_NULL == pThread || 0 == pThread->hThread) return ERR_PLAT_PARAM;

    //获取线程的调度策略
    int policy = 0;     //线程的调度策略
    pthread_attr_t attribute;
    pthread_attr_init(&attribute);
    pthread_attr_getschedpolicy(&attribute, (int *)&policy);

    //linux下有三种线程调度策略，如果调度策略为SCHED_OTHER，则不能使用优先级
    //只有SCHED_FIFO、SCHED_RR这两种调度策略才可以使用优先级
    if(SCHED_OTHER == policy || !((SCHED_FIFO == policy) || (SCHED_RR == policy)))
    {
        return K_SUCCEED;
    }

    //获取线程的最大优先级和最小优先级，在linux下面优先级数值越大，优先级越高
    int maxPriority = sched_get_priority_max(policy);
    int minPriority = sched_get_priority_min(policy);
    int priority = (int)(maxPriority * (1 - (in_ePriority / 8.0)));


    if(priority < minPriority)
    {
        priority = minPriority;
    }

    if(priority > maxPriority)
    {
        priority = maxPriority;
    }
    struct sched_param param;
    pthread_attr_getschedparam(&attribute, &param);
    param.sched_priority = priority;
    pthread_attr_setschedparam(&attribute, &param);

    return K_SUCCEED;
}
