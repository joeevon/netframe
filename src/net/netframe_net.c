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
    if(pAddrIP)
    {
        LOG_SYS_ERROR("ip:%s, port:%u, %s", pAddrIP, ulPort, strerror(nErrno));
    }
    else
    {
        LOG_SYS_ERROR("%s", strerror(nErrno));
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
        close(fd);
        return AGENT_NET_ADDR_IN_USE;
    }

    nRet = netframe_setblockopt(fd, K_FALSE);
    if(nRet != CNV_ERR_OK)
    {
        LOG_SYS_ERROR("netframe_setblockopt failed!");
        close(fd);
        return nRet;
    }

    nRet = bind(fd, (struct sockaddr *)SockAddr, sizeof(struct sockaddr_in));
    if(nRet < 0)
    {
        LOG_SYS_ERROR("bind failed!");
        close(fd);
        return AGENT_NET_BIND_FAILED;
    }

    if(listen(fd, lMaxCout) < 0)
    {
        LOG_SYS_ERROR("listen failed!");
        close(fd);
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

    int optval = 1;
    setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &optval, sizeof(optval));

    nRet = bind(fd, (struct sockaddr *)SockAddr, sizeof(struct sockaddr_in));
    if(nRet < 0)
    {
        LOG_SYS_ERROR("%s", strerror(errno));
        close(fd);
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
        close(ServFd);
        unlink(pSockAddr->sun_path);
        return AGENT_NET_BIND_FAILED;
    }

    nRet = listen(ServFd, 1);
    if(nRet < 0)
    {
        LOG_SYS_ERROR("netframe_init_unixsocket listen failed!");
        close(ServFd);
        unlink(pSockAddr->sun_path);
        return AGENT_NET_LISTEN_FAILED;
    }

    //设置通信文件权限
    chmod(pSockAddr->sun_path, 777);

    *pSocket = ServFd;

    return CNV_ERR_OK;
}

int netframe_tcp_connect(int *pSocket, char *pAddrIP, unsigned int ulPort, int nTimeOut)
{
    int Sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if(Sockfd == -1)
    {
        LOG_SYS_ERROR("%s.", strerror(errno));
        return AGENT_NET_CREATE_SOCKET_FAILED;
    }

    struct sockaddr_in tServAddr = { 0 };
    tServAddr.sin_family = AF_INET;
    inet_aton(pAddrIP, &(tServAddr.sin_addr));
    tServAddr.sin_port = htons((unsigned short)ulPort);

    int nRet = connect(Sockfd, (struct sockaddr *)&tServAddr, sizeof(struct sockaddr));
    if(nRet == 0)
    {
        LOG_SYS_INFO("connect to %s:%d success.", pAddrIP, ulPort);
        *pSocket = Sockfd;
        return CNV_ERR_OK;
    }
    else
    {
        int nErrno = errno;
        LOG_SYS_ERROR("connect to %s:%d failed, %s.", pAddrIP, ulPort , strerror(nErrno));
        if(nErrno != EINPROGRESS && nErrno != EINTR && nErrno != EISCONN)
        {
            close(Sockfd);
            return -1;
        }
    }

    fd_set writefds;
    FD_ZERO(&writefds);
    FD_SET(Sockfd, &writefds);

    struct timeval timeout;
    timeout.tv_sec = nTimeOut / 1000;
    timeout.tv_usec = (nTimeOut % 1000) * 1000;
    //timeout.tv_sec = 1;
    //timeout.tv_usec = 0;

    nRet = select(Sockfd + 1, NULL, &writefds, NULL, &timeout);
    if(nRet <= 0)
    {
        LOG_SYS_ERROR("%s.", strerror(errno));
        close(Sockfd);
        return -1;
    }
    LOG_SYS_ERROR("select return %d.", nRet);

    //if (FD_ISSET(Sockfd, &writefds) == 0)
    //{
    //  LOG_SYS_ERROR("no events on Sockfd found");
    //  netframe_close_socket(Sockfd);
    //  return -1;
    //}

    int nErrno = 0;
    socklen_t nLength = sizeof(nErrno);
    if(getsockopt(Sockfd, SOL_SOCKET, SO_ERROR, &nErrno, &nLength) < 0)
    {
        LOG_SYS_ERROR("%s.", strerror(errno));
        close(Sockfd);
        return -1;
    }

    if(nErrno != 0)
    {
        LOG_SYS_ERROR("connect to %s:%d failed, %s.", pAddrIP, ulPort, strerror(nErrno));
        close(Sockfd);
        return -1;
    }

    *pSocket = Sockfd;
    LOG_SYS_ERROR("connect to %s:%d, %s.", pAddrIP, ulPort, strerror(nErrno));
    return 0;
}

int netframe_unixsocket_connect(int *pSocket, char *strUnixdomain)
{
    int  nSocket = socket(AF_UNIX, SOCK_STREAM, 0);
    if(nSocket < 0)
    {
        LOG_SYS_ERROR("create unixsocket failed,%s.", strerror(errno));
        return -1;
    }

    struct sockaddr_un srv_addr = { 0 };
    srv_addr.sun_family = AF_UNIX;
    memcpy(srv_addr.sun_path, strUnixdomain, sizeof(srv_addr.sun_path) - 1);

    int nRet = connect(nSocket, (struct sockaddr *)&srv_addr, sizeof(struct sockaddr_un));
    if(nRet != 0)
    {
        LOG_SYS_ERROR("connect unixsockket failed,%s,%s.", strUnixdomain, strerror(errno));
        return -1;
    }

    LOG_SYS_INFO("connect to %s succcess.", strUnixdomain);
    *pSocket = nSocket;
    return 0;
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

    if(pConnId) //epoll_event.data是union类型,只能设置一个值
    {
        tEpollEvent.data.ptr = (void *)pConnId;
    }
    else
    {
        tEpollEvent.data.fd = Socket;
    }

    nRet = epoll_ctl(Epollfd, EPOLL_CTL_ADD, Socket, &tEpollEvent);
    if(nRet != 0)
    {
        LOG_SYS_ERROR("%s", strerror(errno));
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
            LOG_SYS_ERROR("%s.", strerror(nErrno));
            nSendLen = 0;
        }
        else if(nErrno == EINTR)
        {
            LOG_SYS_ERROR("%s.", strerror(nErrno));
            nSendLen = 0;
        }
        else
        {
            LOG_SYS_ERROR("%s.", strerror(nErrno));
            return AGENT_NET_CONNECTION_ABNORMAL;
        }
    }

    if(nSendLen != nDataLen && pnLenAlreadyWrite)    //数据发送不完整
    {
        *pnLenAlreadyWrite = nSendLen;
        return AGENT_NET_WRITE_INCOMPLETED;
    }

    LOG_SYS_DEBUG("write %d succesfully.", nSendLen);
    return CNV_ERR_OK;
}

int netframe_sendmsg(int Socket, struct msghdr *pmsg, int nDataLen, int *pnLenAlreadyWrite)
{
    int nSendLen = sendmsg(Socket, pmsg, 0);
    if(nSendLen < 0)
    {
        int nErrno = errno;
        if(nErrno == EAGAIN)
        {
            LOG_SYS_ERROR("%s.", strerror(nErrno));
            nSendLen = 0;
        }
        else if(nErrno == EINTR)
        {
            LOG_SYS_ERROR("%s.", strerror(nErrno));
            nSendLen = 0;
        }
        else
        {
            LOG_SYS_ERROR("%s.", strerror(nErrno));
            return AGENT_NET_CONNECTION_ABNORMAL;
        }
    }

    if(nSendLen != nDataLen && pnLenAlreadyWrite)   //数据发送不完整
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
