#include "netframe_auxiliary.h"
#include "cnv_thread.h"
#include "log/cnv_liblog4cplus.h"
#include <unistd.h>

extern IO_THREAD_CONTEXT g_szIoThreadContexts[MAX_IO_THREAD];

void  free_auxiliary_lockfreequeue(LOCKFREE_QUEUE  *poll_msgque)
{
    AUXILIARY_QUEQUE_DATA *ptAuxiQueData = NULL;
    int nCount = lockfree_queue_len(poll_msgque);
    while(nCount--)
    {
        ptAuxiQueData = (AUXILIARY_QUEQUE_DATA *)lockfree_queue_dequeue(poll_msgque, 1);
        free(ptAuxiQueData->pData);
        free(ptAuxiQueData);
    }
}

void auxiliary_thread_run(void *pThreadParameter)
{
    int nRet = CNV_ERR_OK;
    AUXILIARY_THREAD_CONTEXT *pAuxiliaryThdContext = ((AUXILIARY_THREAD_ITEM *)pThreadParameter)->pAuxiliaryThreadContext;
    CNV_UNBLOCKING_QUEUE *queuerespond = &(pAuxiliaryThdContext->queuerespond);
    CALLBACK_STRUCT_T  tCallback;
    bzero(&tCallback, sizeof(tCallback));
    snprintf(tCallback.strProtocol, sizeof(tCallback.strProtocol) - 1, "auxiliary");
    set_callback_function(SERVER_CALLBACK_FUNC, &tCallback);

    while(1)
    {
        AUXILIARY_QUEQUE_DATA *ptAuxiQueData = (AUXILIARY_QUEQUE_DATA *)lockfree_queue_dequeue(&pAuxiliaryThdContext->poll_msgque, 1);
        if(ptAuxiQueData == NULL)
        {
            //LOG_SYS_DEBUG("auxiliary poll empty queue.");
            continue;
        }

        if(tCallback.pfncnv_auxiliary_callback != NULL)
        {
            tCallback.pfncnv_auxiliary_callback(ptAuxiQueData, queuerespond);
        }

        uint64_t ulWakeup = 1;  //任意值,无实际意义
        K_BOOL bIsWakeIO = K_FALSE;
        int nNumOfPostMsg = get_unblock_queue_count(queuerespond);
        LOG_SYS_DEBUG("nNumOfPostMsg = %d", nNumOfPostMsg);
        while(nNumOfPostMsg--)     // handle线程单独用的队列,无需加锁
        {
            void *pPostData = poll_unblock_queue_head(queuerespond);
            nRet = push_block_queue_tail(g_szIoThreadContexts[0].handle_io_msgque, pPostData, 1);  //队列满了把数据丢掉,以免内存泄露
            if(nRet == false)
            {
                LOG_SYS_ERROR("handle_io queue is full!");
                HANDLE_TO_IO_DATA *pHandleIOData = (HANDLE_TO_IO_DATA *)pPostData;
                free(pHandleIOData->pDataSend);
                free(pHandleIOData);
                continue;
            }
            bIsWakeIO = true;
        }

        if(bIsWakeIO)
        {
            nRet = write(g_szIoThreadContexts[0].handle_io_eventfd, &ulWakeup, sizeof(ulWakeup));  //handle唤醒io
            if(nRet != sizeof(ulWakeup))
            {
                LOG_SYS_ERROR("handle wake io failed.");
            }
            bIsWakeIO = K_FALSE;
        }

        free(ptAuxiQueData->pData);
        free(ptAuxiQueData);
    }
}

void auxiliary_thread_uninit(AUXILIARY_THREAD_CONTEXT *pAuxiliaryContexts)
{
    for(int i = 0; i < g_params.tConfigAuxiliary.lNumberOfThread; i++)
    {
        AUXILIARY_THREAD_CONTEXT *pAuxiliaryContext = &pAuxiliaryContexts[i];
        free_handleio_unblockqueue(&(pAuxiliaryContext->queuerespond));  //写给IO的队列
        free_auxiliary_lockfreequeue(&(pAuxiliaryContext->poll_msgque));
        lockfree_queue_uninit(&(pAuxiliaryContext->poll_msgque));
    }
}

int  auxiliary_thread_init(AUXILIARY_THREAD_ITEM *pConfigAuxiliaryItem, AUXILIARY_THREAD_CONTEXT *pAuxiliaryThreadContext)
{
    lockfree_queue_init(&(pAuxiliaryThreadContext->poll_msgque), 1000);
    initiate_unblock_queue(&(pAuxiliaryThreadContext->queuerespond), DEFAULT_QUEUE_CAPCITY);   //业务返回的消息队列
    pConfigAuxiliaryItem->pAuxiliaryThreadContext = pAuxiliaryThreadContext;

    return CNV_ERR_OK;
}

int auxiliary_thread_start(AUXILIARY_THREAD_CONTEXT *pAuxiliaryContexts)
{
    int nRet = CNV_ERR_OK;

    for(int i = 0; i < g_params.tConfigAuxiliary.lNumberOfThread; i++)
    {
        AUXILIARY_THREAD_CONTEXT *pAuxiliaryThreadContext = &(pAuxiliaryContexts[i]);
        nRet = auxiliary_thread_init(&(g_params.tConfigAuxiliary.szConfigAuxiliaryItem[i]), pAuxiliaryThreadContext);
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