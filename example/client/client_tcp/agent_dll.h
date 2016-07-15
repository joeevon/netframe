
//agent_dll.h


#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

    int create_agent_socket(const char *host, unsigned short port);
    void close_agent_scoket(int s);
    int send_agent(int s, int nIndex);
    int recv_agent(int s);

    int sum(int *s2,int *s1);

#ifdef __cplusplus
};
#endif // __cplusplus
