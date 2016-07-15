/****************************
    FileName:cnv_position.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        cnv_position h文件
    Note:
    Author:WangZhiyong
    Create Date: 2015-05-19
*****************************/

#ifndef __SERVER_H__
#define __SERVER_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "common_type.h"

    //请求数据头
    typedef struct __DATA_REQUEST_HEADER
    {
        int     BusinessCode;     //业务编码
        int     DataType;           //是否物流
        int     NumOfItem;        //位置点个数
        int     DataOffset;        //协议头大小
        int     DataLen;            //数据长度
    } DATA_REQUEST_HEADER;

    //应答数据头
    typedef struct __DATA_RESPONSE_HEADER
    {
        int     BusinessCode;    //业务编码
        int     DataType;           //是否物流
        int     RetCode;           //返回码
        int     NumOfItem;       //应答数据个数
        int     DataOffset;        //协议头大小
        int     DataLen;           //数据长度
    } DATA_RESPONSE_HEADER;

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

    //配置文件路径
    typedef  struct  __PARAM_CONFIG_PATH
    {
        char      strLog[MAX_PATH];                   //日志
    } PARAM_CONFIG_PATH;

    typedef  struct  __POSITION_PARAMS
    {
        PARAM_CONFIG_PATH  tParamConfigPath;
    } POSITION_PARAMS;

#define REQUEST_HEADER_SIZE sizeof(DATA_REQUEST_HEADER)
#define KP_POSITION_SIZE sizeof(NAVI_KP_POSITION)
#define POSITION_PACKAGE_SIZE REQUEST_HEADER_SIZE+KP_POSITION_SIZE

#ifdef __cplusplus
};
#endif

#endif  //__SERVER_H__
