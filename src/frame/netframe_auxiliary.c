#include "netframe_auxiliary.h"
#include "cnv_thread.h"
#include "log/cnv_liblog4cplus.h"
#include <unistd.h>

void auxiliary_thread_run(void *pThreadParameter)
{
    int nRet = CNV_ERR_OK;
    AUXILIARY_THREAD_ITEM *pTheadparam = (AUXILIARY_THREAD_ITEM *)pThreadParameter;
    AUXILIARY_THREAD_CONTEXT *pAuxiliaryThreadContext = pTheadparam->pAuxiliaryThreadContext;
    CNV_UNBLOCKING_QUEUE *queuerespond = &(pAuxiliaryThreadContext->queuerespond);
    IO_THREAD_CONTEXT *pIoThreadContexts = pAuxiliaryThreadContext->pIoThreadContexts;
    int szQueEvents[MAX_IO_THREAD] = { 0 };  //需要唤醒的IO线程索引
    int nWakeIOCount = 0;  //需要唤醒的IO线程个数
    uint64_t ulWakeup = 1;   //任意值,无实际意义,用于线程间唤醒
    CALLBACK_STRUCT_T  tCallback;
    bzero(&tCallback, sizeof(tCallback));
    snprintf(tCallback.strProtocol, sizeof(tCallback.strProtocol) - 1, "auxiliary");
    set_callback_function(SERVER_CALLBACK_FUNC, &tCallback);

    while(1)
    {
        if(tCallback.pfncnv_handle_business)
        {
            tCallback.pfncnv_handle_business(NULL, queuerespond, NULL);
        }
        else  //避免死循环,也可assert退出
        {
            LOG_APP_ERROR("callback func is null!");
            sleep(1);
        }

        int nNumOfPostMsg = get_unblock_queue_count(queuerespond);
        if(nNumOfPostMsg <= 0)  //避免死循环
        {
            sleep(1);
        }
        LOG_SYS_DEBUG("nNumOfPostMsg = %d", nNumOfPostMsg);
        while(nNumOfPostMsg--)     //handle线程单独用的队列,无需加锁
        {
            HANDLE_TO_IO_DATA *pHandleIOData = (HANDLE_TO_IO_DATA *)poll_unblock_queue_head(queuerespond);
            nRet = push_block_queue_tail(pIoThreadContexts[pHandleIOData->io_thread_index - 1].handle_io_msgque, pHandleIOData, 1);  //队列满了把数据丢掉,以免内存泄露
            if(nRet == false)
            {
                free(pHandleIOData->pDataSend);
                free(pHandleIOData);
                continue;
            }

            int i = 0;
            for(; i < nWakeIOCount; i++)  //确保每个线程只唤醒一次
            {
                if(szQueEvents[i] == pHandleIOData->io_thread_index - 1)
                {
                    break;
                }
            }
            if(i >= nWakeIOCount)
            {
                szQueEvents[nWakeIOCount++] = pHandleIOData->io_thread_index - 1;
            }
        }

        for(int i = 0; i < nWakeIOCount; i++)
        {
            nRet = write(pIoThreadContexts[szQueEvents[i]].handle_io_eventfd , &ulWakeup, sizeof(ulWakeup));  //handle唤醒io
            if(nRet != sizeof(ulWakeup))
            {
                LOG_SYS_ERROR("handle wake io failed.");
            }
        }

        bzero(szQueEvents, sizeof(szQueEvents));
        nWakeIOCount = 0;
    }
}

int  auxiliary_thread_init(AUXILIARY_THREAD_ITEM *pConfigAuxiliaryItem, IO_THREAD_CONTEXT *pIoThreadContexts, AUXILIARY_THREAD_CONTEXT *pAuxiliaryThreadContext)
{
    pAuxiliaryThreadContext->pIoThreadContexts = pIoThreadContexts;
    initiate_unblock_queue(&(pAuxiliaryThreadContext->queuerespond), DEFAULT_QUEUE_CAPCITY);   //业务返回的消息队列
    pConfigAuxiliaryItem->pAuxiliaryThreadContext = pAuxiliaryThreadContext;

    return CNV_ERR_OK;
}

int auxiliary_thread_start(IO_THREAD_CONTEXT *pIoThreadContexts, AUXILIARY_THREAD_CONTEXT *pAuxiliaryContexts)
{
    int nRet = CNV_ERR_OK;

    for(int i = 0; i < g_params.tConfigAuxiliary.lNumberOfThread; i++)
    {
        AUXILIARY_THREAD_CONTEXT *pAuxiliaryThreadContext = &(pAuxiliaryContexts[i]);
        nRet = auxiliary_thread_init(&(g_params.tConfigAuxiliary.szConfigAuxiliaryItem[i]), pIoThreadContexts, pAuxiliaryThreadContext);
        if(nRet != CNV_ERR_OK)
        {
            return nRet;
        }
        pAuxiliaryThreadContext->threadindex = i + 1;
        snprintf(pAuxiliaryThreadContext->threadname, sizeof(pAuxiliaryThreadContext->threadname) - 1, "%s", g_params.tConfigAuxiliary.szConfigAuxiliaryItem[i].strThreadName);       //线程名
        nRet = hmi_plat_CreateThread((pfnCNV_PLAT_THREAD_RECALL)auxiliary_thread_run, &(g_params.tConfigAuxiliary.szConfigAuxiliaryItem[i]), 0, &g_params.tConfigAuxiliary.szConfigAuxiliaryItem[i].ulThreadId, &g_params.tConfigAuxiliary.szConfigAuxiliaryItem[i].ThreadHandle);
        LOG_SYS_INFO("io thread start result:%d", nRet);
    }

    return nRet;
}

void auxiliary_thread_uninit(AUXILIARY_THREAD_CONTEXT *pAuxiliaryContexts)
{
    for(int i = 0; i < g_params.tConfigAuxiliary.lNumberOfThread; i++)
    {
        AUXILIARY_THREAD_CONTEXT *pAuxiliaryContext = &pAuxiliaryContexts[i];
        free_handleio_unblockqueue(&(pAuxiliaryContext->queuerespond));  //写给IO的队列
    }

}