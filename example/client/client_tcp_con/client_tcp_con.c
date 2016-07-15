#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <pthread.h>
#include <time.h>

//请求数据头
typedef struct __NAVI_DATA_REQUEST_HEADER
{
    int     BusinessCode;     //业务编码
    int     DataType;           //是否物流
    int     NumOfItem;        //位置点个数
    int     DataOffset;        //协议头大小
    int     DataLen;            //数据长度
} NAVI_DATA_REQUEST_HEADER;

//应答数据头
typedef struct __NAVI_DATA_RESPONSE_HEADER
{
    int     BusinessCode;       //业务编码
    int     DataType;           //是否物流
    int     RetCode;            //返回码
    int     NumOfItem;          //应答数据个数
    int     DataOffset;         //协议头大小
    int     DataLen;            //数据长度
} NAVI_DATA_RESPONSE_HEADER;

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

//位置数据
typedef struct __NAVI_KP_POSITION_CITYID
{
    NAVI_KP_POSITION tPositionData;     //位置数据
    int   lCityID;      //城市ID
} NAVI_KP_POSITION_CITYID;

static int  lReqHeaderSize = sizeof(NAVI_DATA_REQUEST_HEADER);
static int  lPositionSize = sizeof(NAVI_KP_POSITION_CITYID);

void  printfposition(NAVI_KP_POSITION_CITYID *pPosiData)
{
    printf("CarType = %d\n", pPosiData->tPositionData.CarType);
    printf("Direction = %d\n", pPosiData->tPositionData.Direction);
    printf("Duid = %d\n", pPosiData->tPositionData.Duid);
    printf("High = %d\n", pPosiData->tPositionData.High);
    printf("Kuid = %d\n", pPosiData->tPositionData.Kuid);
    printf("L1CellID = %d\n", pPosiData->tPositionData.L1CellID);
    printf("Remark = %d\n", pPosiData->tPositionData.Remark);
    printf("RoadUid = %d\n", pPosiData->tPositionData.RoadUid);
    printf("Speed = %d\n", pPosiData->tPositionData.Speed);
    printf("Src = %d\n", pPosiData->tPositionData.Src);
    printf("Time = %d\n", pPosiData->tPositionData.Time);
    printf("X = %d\n", pPosiData->tPositionData.X);
    printf("Y = %d\n", pPosiData->tPositionData.Y);
    printf("lCityID = %d\n", pPosiData->lCityID);
}

int Sleep_m(unsigned  ulMicroseconds)
{
    fd_set fdsck;
    FD_ZERO(&fdsck);

    struct timeval timeOut;
    timeOut.tv_sec = (long)(ulMicroseconds / 1000);
    timeOut.tv_usec = (long)((ulMicroseconds % 1000) * 1000);
    select(0, &fdsck, &fdsck, &fdsck, &timeOut);

    return  0;
}

void printerror()
{
    if(errno == EINTR || errno == EAGAIN)
    {
        printf("write is busy, error type:EINTR|EAGAIN, errno:%d", errno);
    }
    else if(errno == ECONNRESET)
    {
        printf("Connection reset by peer, error type:ECONNRESET, errno:%d", errno);
    }
    else if(errno == ENOTCONN)
    {
        printf("The socket is not connected, and no target has been given, error type:ENOTCONN, errno:%d", errno);
    }
    else
    {
        if(errno == EBADF)
        {
            printf("An invalid descriptor was specified, error type:EBADF, errno:%d", errno);
        }
        else if(errno == EDESTADDRREQ)
        {
            printf("The socket is not connection-mode, and no peer address is set, error type:EDESTADDRREQ, errno:%d", errno);
        }
        else if(errno == EFAULT)
        {
            printf("An invalid user space address was specified for a parameter, error type:EFAULT , errno:%d", errno);
        }
        else if(errno == EINVAL)
        {
            printf("Invalid argument passed, error type:EINVAL, errno:%d", errno);
        }
        else if(errno == EISCONN)
        {
            printf("The connection-mode socket was connected already but a recipient was specified, error type:EISCONN , errno:%d", errno);
        }
        else if(errno == EMSGSIZE)
        {
            printf("The socket type requires that message be sent atomically, and the size of the message to be sent made this impossible, error type:EMSGSIZE , errno:%d", errno);
        }
        else if(errno == ENOBUFS)
        {
            printf("The output queue for a network interface was full, error type:ENOBUFS , errno:%d", errno);
        }
        else if(errno == ENOMEM)
        {
            printf("No memory available, error type:ENOMEM , errno:%d", errno);
        }
        else if(errno == ENOTSOCK)
        {
            printf("The argument s is not a socket, error type:ENOTSOCK , errno:%d", errno);
        }
        else if(errno == EOPNOTSUPP)
        {
            printf("Some bit in the flags argument is inappropriate for the socket type, error type:EOPNOTSUPP , errno:%d", errno);
        }
        else if(errno == EPIPE)
        {
            printf("The local end has been shut down on a connection oriented socket, error type:EPIPE , errno:%d", errno);
        }
    }
}

int recv_message(int *SocketClient)
{
    int ret = 0;
    int iSocketClient = *SocketClient;
    int num = 1;
    int RecvLen = lReqHeaderSize + lPositionSize*num;
    int RecvLenTmp = RecvLen;
    char *pDataRecv = (char *)malloc(RecvLenTmp);
    char *pRecvTmp = pDataRecv;
    memset(pRecvTmp, 0, RecvLenTmp);

    while(RecvLenTmp > 0)
    {
        ret = read(iSocketClient, pRecvTmp, RecvLenTmp);
        if(ret < 0)
        {
            printerror();
            free(pDataRecv);
            return ret;
        }
        else if(ret == RecvLenTmp || ret == 0)
        {
            break;
        }
        else
        {
            pRecvTmp += ret;
            RecvLenTmp -= ret;
        }
    }

    //printf("receive data, length=%d\n", RecvLen);
    //NAVI_KP_POSITION_CITYID  *position = (NAVI_KP_POSITION_CITYID *)(pDataRecv+sizeof(NAVI_DATA_REQUEST_HEADER));
    //printfposition(position);
    //Sleep_m(100);
    free(pDataRecv);
    return 0;
}

int send_message(int *SocketClient)
{
    int lRet = 0;
    int iSocketClient = *SocketClient;
    int num = 1;
    int i = 0;
    int  datalen = lReqHeaderSize + lPositionSize*num;
    int datalentmp = datalen;
    char *pDataWrite = (char *)malloc(datalentmp);
    char *pWriteTmp = pDataWrite;
    memset(pWriteTmp, 0, datalentmp);

    NAVI_DATA_REQUEST_HEADER  *pReqHeader = (NAVI_DATA_REQUEST_HEADER *)pWriteTmp;
    pReqHeader->BusinessCode = 103;
    pReqHeader->DataLen = lPositionSize *num;
    pReqHeader->DataOffset = lReqHeaderSize;
    pReqHeader->DataType = 0;
    pReqHeader->NumOfItem = num;

    NAVI_KP_POSITION_CITYID  *pPosition = (NAVI_KP_POSITION_CITYID *)(pWriteTmp + lReqHeaderSize);

    for(i = 0; i < num; i++)
    {
        pPosition->tPositionData.CarType = (i+1)*3;
        pPosition->tPositionData.Direction = (i+1)*3;
        pPosition->tPositionData.Duid = (i+1)*3;
        pPosition->tPositionData.High = (i+1)*3;
        pPosition->tPositionData.Kuid = (i+1)*3;
        pPosition->tPositionData.L1CellID = (i+1)*3;
        pPosition->tPositionData.Remark = (i+1)*3;
        pPosition->tPositionData.RoadUid = (i+1)*3;
        pPosition->tPositionData.Speed = (i+1)*3;
        pPosition->tPositionData.Src = (i+1)*3;
        pPosition->tPositionData.Time = (i+1)*3;
        pPosition->tPositionData.X = (i+1)*3;
        pPosition->tPositionData.Y = (i+1)*3;
        pPosition->lCityID = (i+1)*3;
        pPosition++;
    }

    while(datalentmp > 0)
    {
        lRet = write(iSocketClient, pWriteTmp, datalentmp);
        if(lRet < 0)
        {
            printerror();
            free(pDataWrite);
            return lRet;
        }
        else if(lRet == datalentmp)
        {
            break;
        }
        else
        {
            pWriteTmp += lRet;
            datalentmp -= lRet;
        }
    }

    //printf("write %d byte successfully .\n", datalen);
    //Sleep_m(50);
    free(pDataWrite);
    return 0;
}

void *thrd_func(void *arg)
{
    int lRet = 0;
    char strServIP[32] = "169.254.10.12";
    int port = 35525;
    struct sockaddr_in tSocketServerAddr = {0};

    while(1)
    {
        usleep(1000000);
        int lcount = 10;
        int iSocketClient = socket(AF_INET, SOCK_STREAM, 0);
        if(-1 == iSocketClient)
        {
            printf("socket error %s %d\n", strerror(errno), errno);
            continue;
        }

        tSocketServerAddr.sin_family = AF_INET;
        tSocketServerAddr.sin_port   = htons(port);
        inet_aton(strServIP, &tSocketServerAddr.sin_addr);
        memset(tSocketServerAddr.sin_zero, 0, 8);

        lRet = connect(iSocketClient, (const struct sockaddr *)&tSocketServerAddr, sizeof(struct sockaddr));
        if(lRet < 0)
        {
            perror("connect error \n");
            close(iSocketClient);
            continue;
        }

        while(lcount--)
        {
            lRet = send_message(&iSocketClient);
            if(lRet < 0)
            {
                perror("send_message error!");
                close(iSocketClient);
                continue;
            }

            lRet = recv_message(&iSocketClient);
            if(lRet < 0)
            {
                perror("recv_message error!");
                close(iSocketClient);
                continue;
            }
        }

        close(iSocketClient);
        //printf("close socket ok");
    }

    return NULL;
}

int main(int argc, char **argv)
{
    int i;
    pthread_t threadnum[20];

    for(i = 0; i < 20; ++i)
    {
        pthread_create(&threadnum[i], NULL, thrd_func, NULL);
    }

    for(i = 0; i < 20; ++i)
    {
        pthread_join(threadnum[i], 0);
    }

    return 0;
}

