/*===========================================================================
* Copyright (C), 2003-2012, Careland Software Co,.Ltd
* File name:
* Author:
* Version: 1.0.0
* Date:: 2011/04/27
* Description:
*       校验
* Others:
*
* History:
===========================================================================*/
#ifndef __CNV_NET_CRC32_H_2014_01_13__
#define __CNV_NET_CRC32_H_2014_01_13__

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

//#include "cnv_net_datatype.h"
//#include "cnv_web_svc_datatype.h"

#include "netframe/cnv_core_typedef.h"

    /* CRC context. */
    typedef struct __tagNAVI_SVC_CRC32_CONTEXT
    {

        unsigned int crc;
        K_HANDLE context;
        int reserved;

    } NAVI_SVC_CRC32_CONTEXT, *PNAVI_SVC_CRC32_CONTEXT;

    /*========================================================
    *   名称: alg_crc_Generate
    *   功能: 生成CRC32校验码
    *   参数:
    *       pBuffer   [输入] => 需要计算CRC值的缓冲
    *       lBufLen   [输入] => 缓冲大小(Byte)
    *       out_pulCrc[输出] => CRC32校验码
    *   返回:
    *       K_SUCCEED       => 成功
    *       other           => 失败(参考错误代号)
    *========================================================*/
    int cnv_net_crc32_checksum(void *pBuffer, int lBufLen, unsigned int *out_pulCrc);

    /*========================================================
    *   功能: 开始计算CRC
    *   参数:
    *       out_pCRCHandle  [输出] => 输出一个用于计算CRC的句柄
    *   返回:
    *       K_SUCCEED       => 成功
    *       other           => 失败(参考错误代号)
    *========================================================*/
    int cnv_net_crc32_start(K_HANDLE in_pCRCHandle);

    /*========================================================
    *   功能: 更新计算CRC的数据
    *   参数:
    *       in_CRCHandle  [输入] => 用于计算CRC的句柄
    *       pBuffer  [输入] => 需要计算MD5值的缓冲
    *       lBufLen  [输入] => 缓冲大小(Byte)
    *   返回:
    *       ALG_ERR_NONE   => 成功
    *       other          => 失败(参考错误代号)
    *========================================================*/
    int cnv_net_crc32_update(K_HANDLE in_CRCHandle, void *pBuffer, int lBufLen);

    /*========================================================
    *   功能: 计算CRC完成
    *   参数:
    *       in_MD5Handle  [输入] => 用于计算MD5的句柄
    *       pszMd5   [输出] => MD5校验码(16Byte)
    *       lMd5Bytes[输入] => Md5缓冲大小(Byte)
    *   返回:
    *       ALG_ERR_NONE   => 成功
    *       other          => 失败(参考错误代号)
    *========================================================*/
    int cnv_net_crc32_Final(K_HANDLE in_CRCHandle, unsigned int *out_pulCRC);


#ifdef __cplusplus
};
#endif // __cplusplus
#endif // __CNV_NET_CRC32_H_2014_01_13__
