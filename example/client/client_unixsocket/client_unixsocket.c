#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <string.h>
#include <sys/un.h>
#include <signal.h>

typedef struct __THREAD_PARAM
{
    char strUnixdomain[128];
    int nIsDebug;
} THREAD_PARAM;

//请求数据头
typedef struct __NAVI_DATA_REQUEST_HEADER
{
    int     BusinessCode;     //业务编码
    int     DataType;           //是否物流
    int     NumOfItem;        //位置点个数
    int     DataOffset;        //协议头大小
    int     DataLen;            //数据长度
} NAVI_DATA_REQUEST_HEADER;

//位置坐标结构
typedef struct __NAVI_KP_POSITION
{
    int         X;
    int         Y;          //坐标（KLD）
    int         Speed;      //即时速度（M/H）
    short         High;       //高程
    short         Direction;  //方向（正北）
    int         L1CellID;   //图幅ID（L1）
    int         Time;       //UTC时间戳
    int         RoadUid;    //位置所在道路UID，无UID用-1
    short         Src;        //位置点来源（CNV_KP_POS_SRC）
    short         CarType;    //位置点车种（自定义）
    int         Remark;     //附加属性
    int         Duid;       //设备编号（>0）
    int         Kuid;       //用户编号，无用户时用-1
} NAVI_KP_POSITION;

//位置数据和城市ID
typedef struct __NAVI_KP_POSITION_CITYID
{
    NAVI_KP_POSITION tPositionData;     //位置数据
    int   lCityID;      //城市ID
} NAVI_KP_POSITION_CITYID;

void close_socket(int Socket)
{
    close(Socket);
}

int recv_position(int Socket)
{
    int ClientSocket = (int)Socket;
    char buff[1024] = { 0 };
    int  lHeaderSize = sizeof(NAVI_DATA_REQUEST_HEADER);
    int  lPositionSize = sizeof(NAVI_KP_POSITION_CITYID);
    int total = lPositionSize + lHeaderSize;

    int recvlen = recv(ClientSocket, buff, total, MSG_WAITALL);
    if(recvlen != lPositionSize + lHeaderSize)
    {
        printf("recv_position error! errno:%d\n", errno);
        return errno;
    }

    return recvlen;
}

int send_position(int Socket)
{
    int ClientSocket = (int)Socket;
    int num = 1;
    int  lHeaderSize = sizeof(NAVI_DATA_REQUEST_HEADER);
    int  lPositionSize = sizeof(NAVI_KP_POSITION_CITYID);
    int  datalen = lHeaderSize + lPositionSize * num;
    char *pDataWrite = (char *)malloc(datalen);
    memset(pDataWrite, 0, datalen);

    NAVI_DATA_REQUEST_HEADER  *pReqHeader = (NAVI_DATA_REQUEST_HEADER *)pDataWrite;
    pReqHeader->BusinessCode = 103;
    pReqHeader->DataLen = lPositionSize * num;
    pReqHeader->DataOffset = lHeaderSize;
    pReqHeader->DataType = 0;
    pReqHeader->NumOfItem = num;

    NAVI_KP_POSITION_CITYID  *pPosition = (NAVI_KP_POSITION_CITYID *)(pDataWrite + lHeaderSize);
    for(int i = 0; i < num; i++)
    {
        pPosition->tPositionData.CarType = (i + 1) * 3;
        pPosition->tPositionData.Direction = (i + 1) * 3;
        pPosition->tPositionData.Duid = (i + 1) * 3;
        pPosition->tPositionData.High = (i + 1) * 3;
        pPosition->tPositionData.Kuid = (i + 1) * 3;
        pPosition->tPositionData.L1CellID = (i + 1) * 3;
        pPosition->tPositionData.Remark = (i + 1) * 3;
        pPosition->tPositionData.RoadUid = (i + 1) * 3;
        pPosition->tPositionData.Speed = (i + 1) * 3;
        pPosition->tPositionData.Src = (i + 1) * 3;
        pPosition->tPositionData.Time = (i + 1) * 3;
        pPosition->tPositionData.X = (i + 1) * 3;
        pPosition->tPositionData.Y = (i + 1) * 3;
        pPosition->lCityID = (i + 1) * 3;
        pPosition++;
    }

    int sendlen = send(ClientSocket, pDataWrite, datalen, 0);
    free(pDataWrite);
    if(sendlen != datalen)
    {
        printf("send_position error! errno:%d\n", errno);
        return errno;
    }

    return sendlen;
}

int create_socket(const char *strUnixdomain)
{
    int ClientSocket = socket(AF_UNIX, SOCK_STREAM, 0);
    if(ClientSocket < 0)
    {
        printf("create_socket socket error, errno:%d!", errno);
        return errno;
    }

    int value = 0;
    struct sockaddr_un srv_addr = { 0 };
    srv_addr.sun_family = AF_UNIX;
    snprintf(srv_addr.sun_path, sizeof(srv_addr.sun_path) - 1, strUnixdomain);

    if(0 != (value = connect(ClientSocket, (struct sockaddr *)&srv_addr, sizeof(struct sockaddr_un))))
    {
        printf("create_socket connect error, errno:%d!", errno);
        return errno;
    }

    return ClientSocket;
}

void *thrd_func(void *argc)
{
    THREAD_PARAM *pThreadParm = (THREAD_PARAM *)argc;
    int ClientSocket = create_socket(pThreadParm->strUnixdomain);

    char strInput[8] = { 0 };
    while(ClientSocket)
    {
        if(pThreadParm->nIsDebug)
        {
            printf("press enter to send position data.\n");
            gets(strInput);
        }

        int sendlen = send_position(ClientSocket);

        int recvlen = recv_position(ClientSocket);
        if(sendlen != 68 || recvlen != 68)
        {
            printf("fail,sendlen %d,recvlen %d\n", sendlen, recvlen);
            pause();
        }

    }
    close_socket(ClientSocket);
    return 0;
}

static void sig_donothing(int signo)
{
}

int main(int argc, char *argv[])
{
    if(argc != 4)
    {
        printf("error agrv! param: thread_num isdebug unix_domain_path.");
        return -1;
    }

    if(signal(SIGCHLD, sig_donothing) == SIG_ERR)
    {
        exit(0);
    }

    if(signal(SIGPIPE, sig_donothing) == SIG_ERR)
    {
        exit(0);
    }

    THREAD_PARAM tThreadParm;
    memset(&tThreadParm, 0, sizeof(THREAD_PARAM));
    tThreadParm.nIsDebug = atoi(argv[2]);
    snprintf(tThreadParm.strUnixdomain, sizeof(tThreadParm.strUnixdomain) - 1, "%s", argv[3]);

    int threadnum = atoi(argv[1]);
    if(threadnum > 50)
    {
        printf("too many thread!");
        return -1;
    }

    pthread_t thread[threadnum];

    for(int i = 0; i < threadnum; ++i)
    {
        pthread_create(&thread[i], 0, thrd_func, &tThreadParm);
    }

    for(int i = 0; i< threadnum; ++i)
    {
        pthread_join(thread[i], 0);
    }

    return 0;
}
