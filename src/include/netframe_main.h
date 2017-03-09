#ifndef __CNV_AGENT_MAIN_H__
#define __CNV_AGENT_MAIN_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "cnv_unblock_queue.h"

    /*=======================================================
    功能:
        获取服务器配置队列指针
    =========================================================*/
    extern int get_svrconf_queue(CNV_UNBLOCKING_QUEUE **queServer);

    /*=======================================================
    功能:
        服务初始化框架
    =========================================================*/
    extern int initial_netframe(char *strConfPath, CNV_UNBLOCKING_QUEUE *queServer, int nMaxPacketSize);

#ifdef __cplusplus
};
#endif

#endif  //__CNV_AGENT_MAIN_H__
