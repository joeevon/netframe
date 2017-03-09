#ifndef __AGENT_H__
#define __AGENT_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "common_type.h"

#define  MAX_NUMBER_OF_DISTRICT  512     // 最大城市路由个数     
#define  MAX_CELL_FOR_DISTRICT  8     // 每个城市最多服务器数    
#define  MAX_BALANCE_ITEM    256    // 路况LINK最大数    
#define  MATCH_TIME_OUT  600    //匹配服务,过滤掉10分钟以外的位置数据
#define  RAREFY_TIME_OUT  300    //匹配服务,过滤掉5分钟以外的位置数据
#define  ALLRAREFY_TIME_OUT  300     //匹配服务,过滤掉5分钟以外的位置数据

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
        int     BusinessCode;    //业务编码
        int     DataType;           //是否物流
        int     RetCode;           //返回码
        int     NumOfItem;       //应答数据个数
        int     DataOffset;        //协议头大小
        int     DataLen;           //数据长度
    } NAVI_DATA_RESPONSE_HEADER;

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

    //配置文件路径
    typedef  struct  __PARAM_CONFIG_PATH
    {
        char      strBalancePath[MAX_PATH];        //匹配服务
        char      strAllrarefyPath[MAX_PATH];      //统计服务
        char      strPosition[MAX_PATH];               //位置服务
        char      strRarefy[MAX_PATH];             //抽希服务
        char      strLog[MAX_PATH];                   //日志
    } PARAM_CONFIG_PATH;

    // CITY_BALANCE  CELL
    typedef  struct  __NAVI_DISTRICT_CELL
    {
        char          strServIp[DEFAULT_ARRAY_SIZE];               //服务IP
        unsigned int         ulPort;                 //端口
    } NAVI_DISTRICT_CELL;

    // CITY_BALANCE  DISTRICT
    typedef  struct  __NAVI_CITY_BALANCE_DISTRICT
    {
        int  lNumberOfCell;
        int  lDistrictID;
        NAVI_DISTRICT_CELL  szDistrictCell[MAX_CELL_FOR_DISTRICT];
    } NAVI_CITY_BALANCE_DISTRICT;

    // CITY_BALANCE
    typedef  struct  __NAVI_CONFIG_CITY_BALANCE
    {
        int  lNumberOfDistrict;
        NAVI_CITY_BALANCE_DISTRICT  szDistrict[MAX_NUMBER_OF_DISTRICT];
    } NAVI_CONFIG_CITY_BALANCE;

    // BALANCE  ITEM
    typedef  struct  __NAVI_BALANCE_ITEM
    {
        char   strServIp[DEFAULT_ARRAY_SIZE];      //服务IP
        unsigned int    ulPort;                 //端口
    } NAVI_BALANCE_ITEM;

    // BALANCE  PARAM
    typedef  struct  __NAVI_BALANCE_PARAM
    {
        int  lNumberOfBalanceItem;
        NAVI_BALANCE_ITEM  szConfigBalanceItem[MAX_BALANCE_ITEM];
    } NAVI_BALANCE_PARAM;

    typedef  struct  __POSITION_PARAMS
    {
        PARAM_CONFIG_PATH  tParamConfigPath;
        NAVI_CONFIG_CITY_BALANCE  tConfigCityBalance;
        NAVI_BALANCE_PARAM  tConfigRarefy;
        NAVI_BALANCE_PARAM  tConfigAllrarefy;
        NAVI_BALANCE_PARAM  tConfigPosition;
    } POSITION_PARAMS;

#define REQUEST_HEADER_SIZE sizeof(NAVI_DATA_REQUEST_HEADER)
#define KP_POSITION_CITYID_SIZE sizeof(NAVI_KP_POSITION_CITYID)
#define POSITION_PACKAGE_SIZE REQUEST_HEADER_SIZE+KP_POSITION_CITYID_SIZE

    /*=======================================================
    功能:
        初始化位置服务
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int cnv_init_agent(char *strConfPath, CNV_UNBLOCKING_QUEUE  *queueConfig);

    /*=======================================================
    功能:
        初始配置文件路径
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int agent_init_path(char *strConfPath);

    /*=======================================================
    功能:
        加载配置
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int agent_init_config();

    /*=======================================================
    功能:
        把配置加入队列,供框架使用
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int init_svrconf_queue(CNV_UNBLOCKING_QUEUE  *queueConfig);

    /*=======================================================
    功能:
        初始化位置路由配置
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int agent_init_server();

    /*=======================================================
    功能:
        解析接收的客户端数据
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int cnv_parse_client(char **pDataBuff, unsigned int *nDataSize, char **pPacket, unsigned int  *nPacketSize, void **auxiliary);

    /*=======================================================
    功能:
        位置上报服务
    参数:
        [in]
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern void cnv_handle_clientdata(const IO_TO_HANDLE_DATA *req, CNV_UNBLOCKING_QUEUE *queuerespond);

    /*=======================================================
    功能:
        选择服务器
    参数:
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int cnv_agent_select_server(int  Duid, HANDLE_TO_IO_DATA  *pHandletoIOData);

    /*=======================================================
    功能:
        解析服务端返回的数据
    参数:
        [in]
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern int cnv_parse_server(char **pDataBuff, unsigned int *nDataSize, char **pPacket, unsigned int  *nPacketSize, void **auxiliary);

    /*=======================================================
    功能:
        服务端返回业务处理
    参数:
        [in]
    返回值:
      0    成功
      其它 失败
    =========================================================*/
    extern void cnv_handle_serverrespond(const IO_TO_HANDLE_DATA *req, CNV_UNBLOCKING_QUEUE *queuerespond);

    /*=======================================================
    功能:
        反初始化位置服务
    参数:
        无
    返回值:
        0    成功
        其它 失败
    =========================================================*/
    extern void cnv_uninit_service();

#ifdef __cplusplus
};
#endif

#endif  //__AGENT_H__
