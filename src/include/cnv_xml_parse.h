/****************************
    FileName:netframe_io.h
    (C) Copyright 2014 by Careland
    凯立德秘密信息
    Description:
    主要功能简述
        netframe_io h文件
    Note:
    Author:WangZhiyong,Lijian
    Create Date: 2015-05-19
*****************************/

#ifndef __CNV_XML_PARSE_H__
#define __CNV_XML_PARSE_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "cnv_core_typedef.h"

    extern int cnv_comm_xml_loadFile(char *strFilePath, char *strEncoding, void **ppOutDoc);

#ifdef __cplusplus
};
#endif

#endif  //__CNV_XML_PARSE_H__
