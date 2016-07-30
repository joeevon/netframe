/****************************
    FileName:netframe_net.c
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_net C文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-07
*****************************/
#include "netframe_net.h"
#include "cnv_comm.h"
#include "netframe_main.h"
#include "common_type.h"
#include "log/cnv_liblog4cplus.h"
#include "cnv_adler32.h"
#include "cnv_crc32.h"
#include "cnv_base_define.h"
#include "cnv_net_define.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/epoll.h>

void netframe_print_errinfo(int nErrno, char *pAddrIP, unsigned int ulPort)
{
    if(nErrno == EINPROGRESS)
        LOG_SYS_ERROR("Connection cannot be completed immediately, error type:EINPROGRESS, errno:%d.", nErrno);
    else if(nErrno == EINTR)
        LOG_SYS_ERROR("The system call was interrupted by a signal that was caught, error type:EINTR, errno:%d.", nErrno);
    else if(nErrno == EISCONN)
        LOG_SYS_ERROR("The socket is already connected, error type:EISCONN, errno:%d.", nErrno);
    else if(nErrno == EAGAIN)
        LOG_SYS_ERROR("No more free local ports or insufficient entries in the routing cache, error type:EAGAIN, errno:%d.", nErrno);
    else if(nErrno == EADDRINUSE)
        LOG_SYS_ERROR("Local address is already in use, error type:EADDRINUSE, errno:%d.", nErrno);
    else if(nErrno == EADDRNOTAVAIL)
        LOG_SYS_ERROR("Non-existent interface was requested or the requested address was not local, error type:EADDRNOTAVAIL, errno:%d.", nErrno);
    else if(nErrno == ECONNREFUSED)
        LOG_SYS_ERROR("No-one listening on the remote address, error type:ECONNREFUSED, errno:%d.", nErrno);
    else if(nErrno == ENETUNREACH)
        LOG_SYS_ERROR("Network is unreachable, error type:ENETUNREACH, errno:%d.", nErrno);
    else if(nErrno == EACCES)
        LOG_SYS_ERROR("The  user tried to connect to a broadcast address without having the socket broadcast flag enabled or the con-nection request failed because of a local firewall rule, error type:EACCES, errno:%d.", nErrno);
    else if(nErrno == EPERM)
        LOG_SYS_ERROR("The  user tried to connect to a broadcast address without having the socket broadcast flag enabled or the con-nection request failed because of a local firewall rule, error type:EPERM, errno:%d.", nErrno);
    else if(nErrno == EAFNOSUPPORT)
        LOG_SYS_ERROR("The passed address didn’t have the correct address family in its sa_family field, error type:EAFNOSUPPORT, errno:%d.", nErrno);
    else if(nErrno == EALREADY)
        LOG_SYS_ERROR("The socket is non-blocking and a previous connection attempt has not yet been completed, error type:EALREADY, errno:%d.", nErrno);
    else if(nErrno == EBADF)
        LOG_SYS_ERROR("The file descriptor is not a valid index in the descriptor table, error type:EBADF, errno:%d.", nErrno);
    else if(nErrno == EFAULT)
        LOG_SYS_ERROR("The socket structure address is outside the user’s address space, error type:EFAULT, errno:%d.", nErrno);
    else if(nErrno == ENOTSOCK)
        LOG_SYS_ERROR("The file descriptor is not associated with a socket, error type:ENOTSOCK, errno:%d.", nErrno);
    else if(nErrno == ENOMEM)
        LOG_SYS_ERROR("unable to allocate memory for internal tables, error type:ENOMEM, errno:%d.", nErrno);
    else if(nErrno == ECONNREFUSED)
        LOG_SYS_ERROR("No one listening on the remote address, error type:ECONNREFUSED, errno:%d.", nErrno);
    else
        LOG_SYS_ERROR("unrecognised errno:%d!", nErrno);

    if(pAddrIP)
    {
        LOG_SYS_ERROR("ip:%s, port:%u", pAddrIP, ulPort);
    }
}

int netframe_init_tcpserver(int *pSocket, struct sockaddr_in *pSockAddr, int lMaxCout)
{
    int nRet = CNV_ERR_OK;
    int fd;
    int opt = 1;
    struct sockaddr_in *SockAddr = pSockAddr;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if(INVALID_SOCKET == fd)
    {
        return AGENT_NET_CREATE_SOCKET_FAILED;
    }

    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt, sizeof(int)) < 0)
    {
        netframe_close_socket(fd);
        return AGENT_NET_ADDR_IN_USE;
    }

    nRet = netframe_setblockopt(fd, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("netframe_setblockopt failed!");
        netframe_close_socket(fd);
        return nRet;
    }

    nRet = bind(fd, (struct sockaddr *)SockAddr, sizeof(struct sockaddr_in));
    if(nRet < 0)
    {
        LOG_SYS_ERROR("bind failed!");
        netframe_close_socket(fd);
        return AGENT_NET_BIND_FAILED;
    }

    if(listen(fd, lMaxCout) < 0)
    {
        LOG_SYS_ERROR("listen failed!");
        netframe_close_socket(fd);
        return AGENT_NET_LISTEN_FAILED;
    }

    *pSocket = fd;

    return  CNV_ERR_OK;
}

int netframe_init_udpserver(int *pSocket, struct sockaddr_in *pSockAddr)
{
    int  nRet = CNV_ERR_OK;
    int fd;
    struct sockaddr_in  *SockAddr = pSockAddr;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if(INVALID_SOCKET == fd)
    {
        return AGENT_NET_CREATE_SOCKET_FAILED;
    }

    nRet = bind(fd, (struct sockaddr *)SockAddr, sizeof(struct sockaddr_in));
    if(nRet < 0)
    {
        netframe_close_socket(fd);
        return AGENT_NET_BIND_FAILED;
    }

    *pSocket = fd;

    return  CNV_ERR_OK;
}

int netframe_init_unixsocket(int *pSocket, struct sockaddr_un *pSockAddr)
{
    int  nRet = CNV_ERR_OK;
    int ServFd = 0;

    ServFd = socket(AF_UNIX, SOCK_STREAM, 0);
    if(ServFd < 0)
    {
        LOG_SYS_ERROR("netframe_init_unixsocket socket failed!");
        return AGENT_NET_CREATE_SOCKET_FAILED;
    }

    unlink(pSockAddr->sun_path);

    nRet = bind(ServFd, (struct sockaddr *)pSockAddr, sizeof(struct sockaddr_un));
    if(nRet < 0)
    {
        LOG_SYS_ERROR("netframe_init_unixsocket bind failed!");
        netframe_close_socket(ServFd);
        unlink(pSockAddr->sun_path);
        return AGENT_NET_BIND_FAILED;
    }

    nRet = listen(ServFd, 1);
    if(nRet < 0)
    {
        LOG_SYS_ERROR("netframe_init_unixsocket listen failed!");
        netframe_close_socket(ServFd);
        unlink(pSockAddr->sun_path);
        return AGENT_NET_LISTEN_FAILED;
    }

    //设置通信文件权限
    chmod(pSockAddr->sun_path, 777);

    *pSocket = ServFd;

    return CNV_ERR_OK;
}

int netframe_nonblock_connect(int *pnSockfd, int nTimeout)
{
    int Sockfd = *pnSockfd;
    int nErrno = netframe_get_sokceterror(Sockfd);
    netframe_print_errinfo(nErrno, NULL, 0);
    if(nErrno == ECONNREFUSED)
    {
        return AGENT_NET_CONNECT_REFUSED;
    }
    else if(netframe_is_selfconnected(Sockfd))
    {
        LOG_SYS_ERROR("connect self!");
        return AGENT_NET_CONNECT_SELF;
    }

    struct timeval timeout = { 0 };
    timeout.tv_sec = nTimeout / 1000;
    timeout.tv_usec = nTimeout % 1000;
    fd_set  wt_set;
    FD_ZERO(&wt_set);
    FD_SET(Sockfd, &wt_set);

    int nRet = select(Sockfd + 1, K_NULL, &wt_set, K_NULL, &timeout);
    if(nRet < 0) //error
    {
        LOG_SYS_ERROR("nonblock connect error!");
        netframe_print_errinfo(errno, 0, 0);
        return AGENT_NET_NONBLOCK_CONNECT_SELECT;
    }
    else if(nRet == 0)   //timeout
    {
        LOG_SYS_ERROR("nonblock connect timeout!");
        return AGENT_NET_NONBLOCK_CONNECT_TIMEOUT;
    }

    if(FD_ISSET(Sockfd, &wt_set))
    {
        LOG_SYS_DEBUG("netframe_nonblock_connect successd, Sockfd:%d", Sockfd);
        return CNV_ERR_OK;
    }
    else
    {
        LOG_SYS_ERROR("nonblock connect failed!");
        return AGENT_NET_CONNECT_FAILED;
    }

    return CNV_ERR_OK;
}

int netframe_connect_(int *pSocket, char *pAddrIP, unsigned int ulPort, int nEveryTimeout)
{
    int nRet = CNV_ERR_OK;

    int Sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if(Sockfd == INVALID_SOCKET)
    {
        return AGENT_NET_CREATE_SOCKET_FAILED;
    }

    struct sockaddr_in tServAddr = { 0 };
    tServAddr.sin_family = AF_INET;
    inet_aton(pAddrIP, &(tServAddr.sin_addr));
    tServAddr.sin_port = htons((unsigned short)ulPort);

    nRet = connect(Sockfd, (struct sockaddr *)&tServAddr, sizeof(struct sockaddr));
    if(nRet != 0)
    {
        int nErrno = errno;
        netframe_print_errinfo(nErrno, pAddrIP, ulPort);
        switch(nErrno)
        {
            case EINPROGRESS:
            case EINTR:
            case EISCONN:
                nRet = netframe_nonblock_connect(&Sockfd, nEveryTimeout);
                if(nRet != CNV_ERR_OK)
                {
                    netframe_close_socket(Sockfd);
                    Sockfd = -1;
                }
                break;

            case EAGAIN:
            case EADDRINUSE:
            case EADDRNOTAVAIL:
            case ECONNREFUSED:
            case ENETUNREACH:
                netframe_close_socket(Sockfd);
                Sockfd = -1;
                break;

            case EACCES:
            case EPERM:
            case EAFNOSUPPORT:
            case EALREADY:
            case EBADF:
            case EFAULT:
            case ENOTSOCK:
                netframe_close_socket(Sockfd);
                Sockfd = -1;
                break;

            default:
                LOG_SYS_ERROR("unrecognised errno!");
                netframe_close_socket(Sockfd);
                Sockfd = -1;
                break;
        }
    }

    *pSocket = Sockfd;
    return nRet;
}

int netframe_connect(int *pSocket, char  *pAddrIP, unsigned int  ulPort, int nMaxTimeout)
{
    int nRet = CNV_ERR_OK;
    int nTimeout = ((nMaxTimeout > 0 && nMaxTimeout <= 200) ? nMaxTimeout : 200); //默认超时,毫秒

    do
    {
        LOG_SYS_DEBUG("Timeout:%d, RemainTime:%d", nTimeout, nMaxTimeout);
        nRet = netframe_connect_(pSocket, pAddrIP, ulPort, nTimeout);
        if(nMaxTimeout == 0)   //最大超时已为0
        {
            return nRet;
        }
        if(nMaxTimeout - nTimeout > nTimeout * 2)
        {
            nMaxTimeout -= nTimeout;
            nTimeout *= 2;      //超时时长为上一次的2倍
        }
        else
        {
            nTimeout = nMaxTimeout - nTimeout;    //剩下的时间
            nMaxTimeout = 0;
        }
    }
    while(nRet);

    return nRet;
}

int netframe_close_socket(int Socket)
{
    if(Socket != K_NULL)
    {
        close(Socket);
    }
    return CNV_ERR_OK;
}

int netframe_setblockopt(int Socket, K_BOOL bIsBlocking)
{
    int fold = fcntl(Socket, F_GETFL, 0);

    if(fold < 0)
    {
        return AGENT_NET_SETBLOKOPT_FAILED;
    }

    if(!bIsBlocking)
        fold |= O_NONBLOCK;
    else
        fold &= ~O_NONBLOCK;

    return (fcntl(Socket, F_SETFL, fold));

    return CNV_ERR_OK;
}

int netframe_create_epoll(int *pEpfd, int  lSize)
{
    int Epollfd = epoll_create(5);
    if(Epollfd == -1)
    {
        return  AGENT_NET_CREATE_EPOLL;
    }

    *pEpfd = Epollfd;

    return  CNV_ERR_OK;
}

int netframe_add_event(int Epollfd, int Socket, unsigned int ulEventType, char *pConnId)
{
    int  nRet;
    struct epoll_event tEpollEvent = { 0 };
    tEpollEvent.events = ulEventType;

    if(pConnId)
    {
        tEpollEvent.data.ptr = (void *)pConnId;
    }
    else
    {
        tEpollEvent.data.fd = Socket;
    }

    nRet = epoll_ctl(Epollfd, EPOLL_CTL_ADD, Socket, &tEpollEvent);
    if(nRet == -1)
    {
        LOG_SYS_ERROR("netframe_add_event error");
        return  AGENT_NET_ADD_EPOLL;
    }

    LOG_SYS_DEBUG("netframe_add_event succeed, Sockfd:%d", Socket);
    return  CNV_ERR_OK;
}

int netframe_add_readevent(int Epollfd, int Socket, void *pConnId)
{
    int  nRet = CNV_ERR_OK;
    unsigned int ulEventType = EPOLLIN;

    nRet = netframe_add_event(Epollfd, Socket, ulEventType, pConnId);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("netframe_add_readevent error");
        return  AGENT_NET_ADD_READEPOLL;
    }

    return  CNV_ERR_OK;
}

int netframe_add_writeevent(int Epollfd, int Socket, void *pConnId)
{
    int  nRet = 0;
    unsigned int ulEventType = EPOLLOUT;

    nRet = netframe_add_event(Epollfd, Socket, ulEventType, pConnId);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("netframe_add_writeevent error");
        return  AGENT_NET_ADD_WRITEEPOLL;
    }

    return  CNV_ERR_OK;
}

int netframe_read(int Socket, char *pDataBuffer, int *pDataLen)
{
    int  nRet = CNV_ERR_OK;

    nRet = read(Socket, pDataBuffer, *pDataLen);
    if(nRet < 0)
    {
        if(errno == EAGAIN)
        {
            return AGENT_NET_READ_BUSY;
        }
        else if(errno == EINTR)
        {
            LOG_SYS_INFO("read is busy, error type:EINTR, errno:%d", errno);
            return AGENT_NET_READ_BUSY;
        }
        else
        {
            LOG_SYS_ERROR("%s", strerror(errno));
            return AGENT_NET_READ_ABNORMAL;
        }
    }
    else if(nRet == 0)
    {
        LOG_SYS_DEBUG("client closed.");
        return  AGENT_NET_CLIENT_CLOSED;
    }
    else
    {
        LOG_SYS_DEBUG("netframe_read read length:%d", nRet);
        *pDataLen = nRet;
    }

    return  CNV_ERR_OK;
}

int netframe_recvmsg(int Socket, struct msghdr *pmsg, int *pDataLen)
{
    int nRet = CNV_ERR_OK;

    nRet = recvmsg(Socket, pmsg, 0);
    if(nRet < 0)
    {
        if(errno == EAGAIN)
        {
            return AGENT_NET_READ_BUSY;
        }
        else if(errno == EINTR)
        {
            LOG_SYS_INFO("%s", strerror(errno));
            return AGENT_NET_READ_BUSY;
        }
        else
        {
            LOG_SYS_ERROR("%s", strerror(errno));
            return AGENT_NET_READ_ABNORMAL;
        }
    }
    else if(nRet == 0)
    {
        LOG_SYS_DEBUG("client closed.");
        return  AGENT_NET_CLIENT_CLOSED;
    }
    else
    {
        LOG_SYS_DEBUG("netframe_recvmsg read length:%d", nRet);
        *pDataLen = nRet;
    }

    return CNV_ERR_OK;
}

int netframe_write(int Socket, char *pDataBuffer, int nDataLen, int *pnLenAlreadyWrite)
{
    int nSendLen = send(Socket, pDataBuffer, nDataLen, 0);
    if(nSendLen < 0)
    {
        int nErrno = errno;
        if(nErrno == EAGAIN)
        {
            LOG_SYS_DEBUG("EAGAIN happens.");
        }
        else if(nErrno == EINTR)
        {
            LOG_SYS_ERROR("error type:EINTR, write is busy.");
        }
        else
        {
            LOG_SYS_ERROR("%s", strerror(nErrno));
            return AGENT_NET_CONNECTION_ABNORMAL;
        }
    }

    if(nSendLen != nDataLen && (nSendLen > 0 && pnLenAlreadyWrite))    //数据发送不完整
    {
        *pnLenAlreadyWrite = nSendLen;
        LOG_SYS_ERROR("write %d bytes, remain %d bytes.", nSendLen, (nDataLen - nSendLen));
        return AGENT_NET_WRITE_INCOMPLETED;
    }

    LOG_SYS_DEBUG("write %d succesfully.", nSendLen);
    return CNV_ERR_OK;
}

int netframe_sendmsg(int Socket, struct msghdr *pmsg, int nDataLen, int *pnLenAlreadyWrite)
{
    if(nDataLen <= 0)
    {
        LOG_SYS_ERROR("illegal length:%d", nDataLen);
        return AGENT_NET_ILLEGAL_LENGTH;
    }

    int nSendLen = sendmsg(Socket, pmsg, 0);
    if(nSendLen < 0)
    {
        int nErrno = errno;
        LOG_SYS_ERROR("nErrno:%d", nErrno);
        if(nErrno == EAGAIN)
        {
            LOG_SYS_DEBUG("EAGAIN happens");
        }
        else if(nErrno == EINTR)
        {
            LOG_SYS_DEBUG("write is busy, error type:EINTR, nErrno:%d", nErrno);
        }
        else if(nErrno == EWOULDBLOCK)
        {
            LOG_SYS_ERROR("EWOULDBLOCK happens");
        }
        else if(nErrno == EMSGSIZE)
        {
            LOG_SYS_ERROR("The socket type requires that message be sent atomically, and the size of the message to be sent made this impossible, error type:EMSGSIZE , nErrno:%d", nErrno);
        }
        else
        {
            LOG_SYS_ERROR("%s", strerror(nErrno));
            return AGENT_NET_CONNECTION_ABNORMAL;
        }
    }

    if(nSendLen != nDataLen && (nSendLen > 0 && pnLenAlreadyWrite))   //数据发送不完整
    {
        *pnLenAlreadyWrite = nSendLen;
        LOG_SYS_ERROR("write %d bytes, remain %d bytes.", nSendLen, (nDataLen - nSendLen));

        return AGENT_NET_WRITE_INCOMPLETED;
    }

    return CNV_ERR_OK;
}

int netframe_modify_event(int Epollfd, struct epoll_event *pepollevent)
{
    int  nRet = CNV_ERR_OK;
    struct epoll_event epollevent = {0};

    memcpy(&epollevent, pepollevent, sizeof(struct epoll_event));
    int Socket = epollevent.data.fd;

    nRet = epoll_ctl(Epollfd, EPOLL_CTL_MOD, Socket, &epollevent);
    if(nRet != CNV_ERR_OK)
    {
        return  nRet;
    }

    return  CNV_ERR_OK;
}

int netframe_modify_readevent(int Epollfd, int  Socket, char *pConnId)
{
    struct epoll_event epollevent = {0};
    epollevent.data.fd = Socket;
    epollevent.events = EPOLLOUT;
    if(pConnId)
    {
        epollevent.data.ptr = (void *)pConnId;
    }

    int nRet = epoll_ctl(Epollfd, EPOLL_CTL_MOD, Socket, &epollevent);
    if(nRet != 0)
    {
        LOG_SYS_ERROR("netframe_modify_readevent error!");
        netframe_print_errinfo(errno, NULL, 0);
    }

    return nRet;
}

int netframe_modify_writeevent(int Epollfd, int Socket, char *pConnId)
{
    struct epoll_event epollevent = {0};
    epollevent.data.fd = Socket;
    epollevent.events = EPOLLIN;
    if(pConnId)
    {
        epollevent.data.ptr = (void *)pConnId;
    }

    int nRet = epoll_ctl(Epollfd, EPOLL_CTL_MOD, Socket, &epollevent);
    if(nRet != 0)
    {
        LOG_SYS_ERROR("netframe_modify_readevent error!");
        netframe_print_errinfo(errno, NULL, 0);
    }

    return  CNV_ERR_OK;
}

int netframe_delete_event(int Epollfd, int  Socket)
{
    int  nRet = CNV_ERR_OK;
    struct epoll_event epollevent = {0};

    epollevent.data.fd = Socket;
    nRet = epoll_ctl(Epollfd, EPOLL_CTL_DEL, Socket, &epollevent);
    if(nRet != CNV_ERR_OK)
    {
        return  nRet;
    }

    return  CNV_ERR_OK;
}

int netframe_delete_event_ex(int Epollfd, struct epoll_event *pepollevent)
{
    int  nRet = CNV_ERR_OK;
    struct epoll_event epollevent = {0};

    memcpy(&epollevent, pepollevent, sizeof(struct epoll_event));
    int Socket = epollevent.data.fd;

    nRet = epoll_ctl(Epollfd, EPOLL_CTL_DEL, Socket, &epollevent);
    if(nRet != CNV_ERR_OK)
    {
        return  nRet;
    }

    return  CNV_ERR_OK;
}

int netframe_get_sokceterror(int Sockfd)
{
    int optval = 0;
    socklen_t optlen = sizeof(int);

    if(getsockopt(Sockfd, SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0)
    {
        return errno;
    }
    else
    {
        return optval;
    }
}

struct sockaddr_in netframe_get_localaddr(int Sockfd)
{
    struct sockaddr_in tLocalAddr;
    socklen_t nAddrLen = sizeof(struct sockaddr_in);
    bzero(&tLocalAddr, nAddrLen);

    if(getsockname(Sockfd, (struct sockaddr *)&tLocalAddr, &nAddrLen) < 0)
    {
        LOG_SYS_ERROR("getsockname error!");
    }
    return tLocalAddr;
}

struct sockaddr_in netframe_get_peeraddr(int Sockfd)
{
    struct sockaddr_in tPeerAddr;
    socklen_t nAddrLen = sizeof(struct sockaddr_in);
    bzero(&tPeerAddr, nAddrLen);

    if(getpeername(Sockfd, (struct sockaddr *)&tPeerAddr, &nAddrLen) < 0)
    {
        LOG_SYS_ERROR("getpeername error!");
    }
    return tPeerAddr;
}

K_BOOL netframe_is_selfconnected(int Sockfd)
{
    struct sockaddr_in tLocalAddr = netframe_get_localaddr(Sockfd);
    struct sockaddr_in peeraddr = netframe_get_peeraddr(Sockfd);
    return tLocalAddr.sin_port == peeraddr.sin_port && tLocalAddr.sin_addr.s_addr == peeraddr.sin_addr.s_addr;
}

int svc_protobuffer_packex(short in_iFlags,
                           char *in_pTypeName,
                           int in_lNameLen,
                           char *in_pData,
                           int in_lDataLen,
                           char **out_pProtoBuf,
                           int *out_plProtoBufLen)
{
    int lLen = sizeof(int) * 3;
    char *pBuffer = K_NULL,
          *ptr = K_NULL;
    NAVI_SVC_PROTOBUFFER protoBuf = {0};
    unsigned int ulChecksum = 1;

    if(((in_pTypeName == K_NULL || in_lNameLen <= 0)
            && (in_pData == K_NULL || in_lDataLen <= 0))
            || (out_pProtoBuf == K_NULL || out_plProtoBufLen == K_NULL))
    {
        LOG_SYS_ERROR("the parameters is invalid.errno: %d.", CNV_ERR_PARAM);
        //参数错误
        return CNV_ERR_PARAM;
    }
    protoBuf.flags = in_iFlags;
    if(in_pTypeName != K_NULL && in_lNameLen > 0)
    {
        lLen += in_lNameLen;
        protoBuf.lNameLen = (short)in_lNameLen;
    }
    if(in_pData != K_NULL && in_lDataLen > 0)
    {
        lLen += in_lDataLen;
    }
    protoBuf.lLen = lLen - sizeof(int);
    pBuffer = (char *)cnv_comm_Malloc(lLen);
    if(pBuffer == K_NULL)
    {
        LOG_SYS_ERROR("failed to malloc a buffer.errno: %d.", CNV_ERR_MALLOC);
        return CNV_ERR_MALLOC;
    }
    memset(pBuffer, 0, lLen);
    ptr = pBuffer;
    memcpy(ptr, &protoBuf, sizeof(int) * 2);
    ptr += sizeof(int) * 2;

    if(in_pTypeName != K_NULL && in_lNameLen > 0)
    {
        memcpy(ptr, in_pTypeName, in_lNameLen);
        ptr += in_lNameLen;
    }
    if(in_pData != K_NULL && in_lDataLen > 0)
    {
        memcpy(ptr, in_pData, in_lDataLen);
        ptr += in_lDataLen;
    }
    if(protoBuf.flags & 0x01)
    {

        ulChecksum = 0;
        cnv_net_crc32_checksum(pBuffer,
                               lLen - sizeof(int),
                               &ulChecksum);
    }
    else
    {

        ulChecksum = cnv_adler32_checksum(ulChecksum, (const K_UINT8 *)pBuffer, lLen - sizeof(int));
    }

    memcpy(ptr, &ulChecksum, sizeof(unsigned int));

    *out_pProtoBuf = pBuffer;
    *out_plProtoBufLen = lLen;

    return K_SUCCEED;
}

K_BOOL svc_protobuffer_check(char *in_pProtoBufData,
                             int in_lProtoBufDataLen)
{
    unsigned int ulChecksum = 1,
                 ulOldChecksum = 0;

    NAVI_SVC_PROTOBUFFER *pProtoBuf = (NAVI_SVC_PROTOBUFFER *)in_pProtoBufData;

    if((in_pProtoBufData == K_NULL)
            || (in_lProtoBufDataLen <= 0)
            || (pProtoBuf->lLen <= 0))
    {
        LOG_SYS_ERROR("the parameters is invalid.errno: %d.", CNV_ERR_PARAM);
        //参数错误
        return K_FALSE;
    }

    if(pProtoBuf->flags & 0x01)
    {

        ulChecksum = 0;
        cnv_net_crc32_checksum(in_pProtoBufData,
                               pProtoBuf->lLen,
                               &ulChecksum);
    }
    else
    {

        ulChecksum = cnv_adler32_checksum(ulChecksum,
                                          (const K_UINT8 *)in_pProtoBufData,
                                          pProtoBuf->lLen);

    }
    ulOldChecksum = *((unsigned int *)((char *)(in_pProtoBufData + pProtoBuf->lLen)));

    if(ulChecksum != ulOldChecksum)
    {

        LOG_SYS_ERROR("checksum failed. flags: %d, length: %d,  buffer checksum: %u, checksum: %u.",
                      pProtoBuf->flags,
                      pProtoBuf->lLen,
                      ulChecksum,
                      ulOldChecksum);
    }

    return (ulChecksum == ulOldChecksum) ? true : K_FALSE;
}

int svc_protobuffer_unpackex(char *in_pProtoBufData,
                             int in_plProtoBufDataLen,
                             short *out_piFlags,
                             char **out_pTypeName,
                             int *out_plNameLen,
                             char **out_pData,
                             int *out_plDataLen)
{
    char *ptr = in_pProtoBufData;
    NAVI_SVC_PROTOBUFFER *pProtoBuf = (NAVI_SVC_PROTOBUFFER *)in_pProtoBufData;

    if(out_piFlags != K_NULL)
        *out_piFlags = 0;

    if(in_pProtoBufData == K_NULL
            || in_plProtoBufDataLen <= 0
            || pProtoBuf->lLen <= 0)
    {
        LOG_SYS_ERROR("the parameters is invalid.errno: %d.", CNV_ERR_PARAM);
        //参数错误
        return CNV_ERR_PARAM;
    }
    //校验
    //if(!svc_protobuffer_check(in_pProtoBufData, in_plProtoBufDataLen))
    //{
    //  LOG_SYS_ERROR("the protocol buffer's checksum error.errno: %d.", ERRORCODE_WEBSVC_NET_CHECKSUM_FAILED);
    //  return ERRORCODE_WEBSVC_NET_CHECKSUM_FAILED;
    //}
    if(out_piFlags != K_NULL)
        *out_piFlags = pProtoBuf->flags;

    ptr += sizeof(int) * 2;
    if(pProtoBuf->lNameLen > 0)
    {
        if(out_pTypeName != K_NULL
                && out_plNameLen != K_NULL)
        {
            *out_pTypeName = ptr;
            *out_plNameLen = pProtoBuf->lNameLen;
        }
        ptr +=  pProtoBuf->lNameLen;
    }
    if(out_pData != K_NULL && out_plDataLen != K_NULL)
    {
        *out_pData = ptr;
        *out_plDataLen = pProtoBuf->lLen - pProtoBuf->lNameLen - sizeof(int) * 2;
    }
    return K_SUCCEED;
}
