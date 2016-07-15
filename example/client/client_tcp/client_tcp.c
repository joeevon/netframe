#include "agent_dll.h"
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

typedef struct __THREAD_PARAM
{
    char strServerIp[32];
    unsigned int unPort;
    int nIsDebug;
} THREAD_PARAM;


void *thrd_func(void *argc)
{
    THREAD_PARAM *pThreadParm = (THREAD_PARAM *)argc;
    int ClientSocket = create_agent_socket(pThreadParm->strServerIp, pThreadParm->unPort);
    int nIndex = 1;

    while(ClientSocket)
    {
        int sendlen = send_agent(ClientSocket, nIndex++);
        int recvlen = recv_agent(ClientSocket);
        if(sendlen == recvlen && recvlen == 0)
        {
        }
        else
        {
            printf("fail,sendlen %d,recvlen %d\n", sendlen, recvlen);
            pause();
        }

        if(nIndex >= 1000)
        {
            nIndex = 0;
        }
    }

    close_agent_scoket(ClientSocket);
    return 0;
}

int main(int argc, char *argv[])
{
    if(argc != 4)
    {
        printf("error agrv! param: ip port thread_num.\n");
        return -1;
    }

    THREAD_PARAM tThreadParm;
    memset(&tThreadParm, 0, sizeof(THREAD_PARAM));
    snprintf(tThreadParm.strServerIp, sizeof(tThreadParm.strServerIp) - 1, "%s", argv[1]);
    tThreadParm.unPort = (unsigned int)atoi(argv[2]);

    int threadnum = atoi(argv[3]);
    if(threadnum > 50)
    {
        printf("too many thread!");
        return -1;
    }

    pthread_t thread[threadnum];
    for(int i = 0; i < sizeof(thread) / sizeof(pthread_t); ++i)
    {
        pthread_create(&thread[i], 0, thrd_func, &tThreadParm);
    }

    for(int i = 0; i< sizeof(thread) / sizeof(pthread_t); ++i)
    {
        pthread_join(thread[i],0);
    }

    return 0;
}

