// agent_dll.cpp : 定义 DLL 应用程序的导出函数。
//

//#include "stdafx.h"
#include "agent_dll.h"
//#include <WS2tcpip.h>
//#include <WinSock2.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <unistd.h>
//#pragma comment(lib, "WS2_32")

typedef int SOCKET;

typedef struct __DATA_REQUEST_HEADER
{
    int     BusinessCode;     //业务编码
    int     DataType;           //是否物流
    int     NumOfItem;        //位置点个数
    int     DataOffset;        //协议头大小
    int     DataLen;            //数据长度
} DATA_REQUEST_HEADER;

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


int create_agent_socket(const char *host, unsigned short port)
{
    SOCKET ClientSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    int value = 0;

    struct sockaddr_in in = {0};
    in.sin_family = AF_INET;
    in.sin_port   = htons(port);
    inet_aton(host, &in.sin_addr);
    memset(in.sin_zero, 0, 8);

    if(0 == (value = connect(ClientSocket, (const struct sockaddr *)&in, sizeof(struct sockaddr))))
    {
        return  ClientSocket;
    }
    return errno;
}

void close_agent_scoket(int Socket)
{
    close(Socket);
}

int send_agent(int Socket, int nIndex)
{
    int nHeaderSize = sizeof(DATA_REQUEST_HEADER);
    int nPositionSize = sizeof(NAVI_KP_POSITION);
    int datalen = nHeaderSize + nPositionSize;
    char *pDataWrite = (char *)malloc(datalen);
    memset(pDataWrite, 0, datalen);

    DATA_REQUEST_HEADER  *pReqHeader = (DATA_REQUEST_HEADER *)pDataWrite;
    pReqHeader->BusinessCode = 103;
    pReqHeader->DataLen = nPositionSize;
    pReqHeader->DataOffset = nHeaderSize;
    pReqHeader->DataType = 0;
    pReqHeader->NumOfItem = 1;

    NAVI_KP_POSITION  *pPosition = (NAVI_KP_POSITION *)(pDataWrite + nHeaderSize);
    pPosition->CarType = nIndex;
    pPosition->Direction = nIndex;
    pPosition->Duid = nIndex;
    pPosition->High = nIndex;
    pPosition->Kuid = nIndex;
    pPosition->L1CellID = nIndex;
    pPosition->Remark = nIndex;
    pPosition->RoadUid = nIndex;
    pPosition->Speed = nIndex;
    pPosition->Src = nIndex;
    pPosition->Time = nIndex;
    pPosition->X = nIndex;
    pPosition->Y = nIndex;

    int sendlen = send(Socket, pDataWrite, datalen, 0);
    free(pDataWrite);
    if(sendlen == datalen)
    {
        return 0;
    }
    return errno;
}

int recv_agent(int Socket)
{
    SOCKET ClientSocket = (SOCKET)Socket;
    char buff[1024] = { 0 };
    int  lHeaderSize = sizeof(DATA_REQUEST_HEADER);
    int  lPositionSize = sizeof(NAVI_KP_POSITION);
    int total = lPositionSize + lHeaderSize;
    int recvlen = recv(ClientSocket, buff, total, MSG_WAITALL);
    if(recvlen == lPositionSize + lHeaderSize)
    {
        return 0;
    }

    return errno;
}

int sum(int *s2, int *s1)
{
    return *s1 + *s2;
}
