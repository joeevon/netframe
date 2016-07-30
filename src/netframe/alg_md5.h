/*===========================================================================
* Copyright (C), 2003-2012, Careland Software Co,.Ltd
* File name: alg_common.h
* Author: wenap    Version: 1.0.7    Date: 2012/02/21
* Description: 一些简单算法功能模块
*
* Others:
*
* History:
* 1.Date: 2011/09/01
*   Version: 1.0.1
*   Author: wenap
*   Modification:
*
* 2.Date: 2011/09/06
*   Version: 1.0.2
*   Author: wenap
*   Modification: 增加编码类型EncodeType: ENCODE_TP1
*
* 3.Date: 2011/10/10
*   Version: 1.0.3
*   Author: wenap
*   Modification: 增加简单几何计算功能
*
* 4.Date: 2011/10/10
*   Version: 1.0.4
*   Author: wenap
*   Modification: 增加编码类型EncodeType: ENCODE_TP2
*
* 5.Date: 2011/10/26
*   Version: 1.0.5
*   Author: wenap
*   Modification: 增加编码类型EncodeType: ENCODE_TP3
*
* 6.Date: 2012/01/06
*   Version: 1.0.6
*   Author: wenap
*   Modification: 增加坐标旋转功能
*
* 7.Date: 2012/02/21
*   Version: 1.0.7
*   Author: wenap
*   Modification: 增加文件内容CRC32计算函数
*
===========================================================================*/
#ifndef __ALG_COMMON_H__2012
#define __ALG_COMMON_H__2012

#include "netframe/cnv_core_typedef.h"

// 常量宏定义
#define ALG_LIB_VERSION          "2.0.0"   // 库版本号

// 错误代码定义
#define ALG_ERR_BASE             102000
#define ALG_ERR_NONE             (0)                // 成功
#define ALG_ERR_PARAMETER        (ALG_ERR_BASE+0)   // 参数错误
#define ALG_ERR_FILE_OPEN        (ALG_ERR_BASE+1)   // 打开文件失败
#define ALG_ERR_FILE_READ        (ALG_ERR_BASE+2)   // 读文件失败
#define ALG_ERR_FILE_WRITE       (ALG_ERR_BASE+3)   // 写文件失败
#define ALG_ERR_FILE_SEEK        (ALG_ERR_BASE+4)   // SEEK文件失败
#define ALG_ERR_BUFF_OUT         (ALG_ERR_BASE+5)   // 缓冲溢出(不足)
#define ALG_ERR_MEM_ALLOC        (ALG_ERR_BASE+6)   // 内存分配失败
#define ALG_ERR_CRC_MISMATCH     (ALG_ERR_BASE+7)   // CRC校验不匹配
#define ALG_ERR_MD5_MISMATCH     (ALG_ERR_BASE+10)  // MD5校验不匹配
#define ALG_ERR_GENERATE_KEY     (ALG_ERR_BASE+11)  // 生成密钥失败
#define ALG_ERR_ENCODE           (ALG_ERR_BASE+12)  // 加密失败
#define ALG_ERR_DECODE           (ALG_ERR_BASE+13)  // 解密失败

// 常量宏定义
#define ALG_MD5_BYTE_LEN         16   // MD5存储字节数(不能修改)
#define ALG_MD5_CALC_BUFF_LEN    1024 // MD5计算缓冲字节数

#define ALG_MALLOC   malloc
#define ALG_FREE     free

// 编码类型枚举
typedef enum __enumEncodeType
{
    ENCODE_TP1   = 1,  // 编码类型1
    ENCODE_TP2   = 2,  // 编码类型2
    ENCODE_TP3   = 3,  // 编码类型3
    ENCODE_TP4   = 4,  // 编码类型4
    ENCODE_TP5   = 5,  // 编码类型5
    ENCODE_TP6   = 6,  // 编码类型6
    ENCODE_TP7   = 7,  // 编码类型7
    ENCODE_TP8   = 8,  // 编码类型8
} EncodeType;

// 常量PI
#ifndef PI
#define PI        3.14159265359
#endif

// 点结构
typedef struct __tagAlgPoint
{
    int lX;
    int lY;
} ALG_POINT, *PALG_POINT;

// 线段结构
typedef struct __tagAlgLineSeg
{
    ALG_POINT s;  // 起点
    ALG_POINT e;  // 终点
} ALG_LINESEG, *PALG_LINESEG;

// MD5句柄,用于生成MD5
typedef void *MD5_HANDLE;

// CRC句柄，用于生成CRC32
typedef void *CRC_HANDLE;

#ifdef __cplusplus
extern "C" {
#endif

    /*========================================================
    *   功能: 获取模块版本信息
    *   参数:
    *       out_pszVer[输出] => 版本信息(格式1.0.1)，内存由调用者分配
    *       in_lBufLen[输入] => 缓冲大小(Byte)，最小为6字节
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_GetVersion(char *out_pszVer, int in_lBufLen);

    /****************************************************************************************
    ***********************************  MD5相关函数接口  ***********************************
    *****************************************************************************************/

    /*========================================================
    *   功能: 开始计算MD5
    *   参数:
    *       out_pMD5Handle  [输出] => 输出一个用于计算MD5的句柄
    *   返回:
    *       ALG_ERR_NONE   => 成功
    *       other          => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_Start(MD5_HANDLE *out_pMD5Handle);

    /*========================================================
    *   功能: 更新计算MD5的数据
    *   参数:
    *       in_MD5Handle  [输入] => 用于计算MD5的句柄
    *       pBuffer  [输入] => 需要计算MD5值的缓冲
    *       lBufLen  [输入] => 缓冲大小(Byte)
    *   返回:
    *       ALG_ERR_NONE   => 成功
    *       other          => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_Update(MD5_HANDLE in_MD5Handle, void *pBuffer, int lBufLen);

    /*========================================================
    *   功能: 计算MD5完成
    *   参数:
    *       in_MD5Handle  [输入] => 用于计算MD5的句柄
    *       pszMd5   [输出] => MD5校验码(16Byte)
    *       lMd5Bytes[输入] => Md5缓冲大小(Byte)
    *   返回:
    *       ALG_ERR_NONE   => 成功
    *       other          => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_Final(MD5_HANDLE in_MD5Handle, K_UINT8 *pszMd5, int lMd5Bytes);


    /*========================================================
    *   功能: 生成MD5校验码
    *   参数:
    *       pBuffer  [输入] => 需要计算MD5值的缓冲
    *       lBufLen  [输入] => 缓冲大小(Byte)
    *       pszMd5   [输出] => MD5校验码(16Byte)
    *       lMd5Bytes[输入] => Md5缓冲大小(Byte)
    *   返回:
    *       ALG_ERR_NONE   => 成功
    *       other          => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_Generate(void *pBuffer, int lBufLen, K_UINT8 *pszMd5, int lMd5Bytes);

    /*========================================================
    *   功能: 生成32位字符串MD5校验码
    *   参数:
    *       pBuffer [输入] => 需要计算MD5值的缓冲
    *       lBufLen [输入] => 缓冲大小(Byte)
    *       pcMd5Str[输出] => 32位MD5校验码，内存由调用者分配，不小于33字节
    *       lStrLen [输入] => MD5缓冲大小(Byte)
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_GenerateString32(void *pBuffer, int lBufLen, char *pcMd5Str, int lStrLen);

    /*========================================================
    *   功能: 生成16位字符串MD5校验码
    *   参数:
    *       pBuffer [输入] => 需要计算MD5值的缓冲
    *       lBufLen [输入] => 缓冲大小(Byte)
    *       pcMd5Str[输出] => 16位MD5校验码，内存由调用者分配，不小于17字节
    *       lStrLen [输入] => MD5缓冲大小(Byte)
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_GenerateString16(void *pBuffer, int lBufLen, char *pcMd5Str, int lStrLen);

    /*========================================================
    *   名称: alg_md5_CheckBuffer
    *   功能: 根据输入的MD5校验码校验缓冲数据
    *   参数:
    *       pBuffer[输入] => 需要校验MD5值的缓冲
    *       lBufLen[输入] => 缓冲大小(Byte)
    *       ucMd5  [输入] => MD5校验码
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_CheckBuffer(void *pBuffer, int lBufLen, K_UINT8 ucMd5[ALG_MD5_BYTE_LEN]);

    /*========================================================
    *   功能: 根据输入的32位字串MD5校验码校验缓冲数据
    *   参数:
    *       pBuffer [输入] => 需要校验MD5值的缓冲
    *       lBufLen [输入] => 缓冲大小(Byte)
    *       pcMd5Str[输入] => 32位MD5校验码
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_CheckBufferByString32(void *pBuffer, int lBufLen, char *pcMd5Str);

    /*========================================================
    *   功能: 根据输入的16位字串MD5校验码校验缓冲数据
    *   参数:
    *       pBuffer [输入] => 需要校验MD5值的缓冲
    *       lBufLen [输入] => 缓冲大小(Byte)
    *       pcMd5Str[输入] => 16位MD5校验码
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_md5_CheckBufferByString16(void *pBuffer, int lBufLen, char *pcMd5Str);

    /****************************************************************************************
    ***********************************  CRC相关函数接口  ***********************************
    *****************************************************************************************/
    /*========================================================
    *   功能: 开始计算CRC
    *   参数:
    *       out_pCRCHandle  [输出] => 输出一个用于计算CRC的句柄
    *   返回:
    *       ALG_ERR_NONE   => 成功
    *       other          => 失败(参考错误代号)
    *========================================================*/
    extern int alg_crc_Start(CRC_HANDLE *out_pCRCHandle);

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
    extern int alg_crc_Update(CRC_HANDLE in_CRCHandle, void *pBuffer, int lBufLen);

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
    extern int alg_crc_Final(CRC_HANDLE in_CRCHandle, unsigned int *out_pulCRC);

    /*========================================================
    *   功能: 生成CRC32校验码
    *   参数:
    *       pBuffer   [输入] => 需要计算CRC值的缓冲
    *       lBufLen   [输入] => 缓冲大小(Byte)
    *       out_pulCrc[输出] => CRC32校验码
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_crc_Generate(void *pBuffer, int lBufLen, unsigned int *out_pulCrc);

    /****************************************************************************************
    *********************************  BASE64相关函数接口  **********************************
    *****************************************************************************************/
    /*========================================================
    *   功能: 对数据进行BASE64编码
    *   参数:
    *       in_pData         [输入] => 需要编码的数据
    *       in_lDataLen      [输入] => 需要编码的数据长度(Byte)
    *       out_pData        [输入] => 编码后的数据，内存由调用者分配(约为编码数据长度的4/3)
    *       in_out_lpLen[输入/输出] => 输入为存储缓冲长度，输出为编码后的实际长度
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_b64_Encode(const void *in_pData, int in_lDataLen, void *out_pData, int *in_out_lpLen);

    /*========================================================
    *   功能: 对数据进行BASE64解码
    *   参数:
    *       in_pData         [输入] => 需要解码的数据
    *       in_lDataLen      [输入] => 需要解码的数据长度(Byte)
    *       out_pData        [输入] => 解码后的数据，内存由调用者分配(约为解码数据长度的3/4)
    *       in_out_lpLen[输入/输出] => 输入为存储缓冲长度，输出为解码后的实际长度
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_b64_Decode(const void *in_pData, int in_lDataLen, void *out_pData, int *in_out_lpLen);


    /****************************************************************************************
    ********************************  BIN编码相关函数接口  **********************************
    *****************************************************************************************/
    /*========================================================
    *   功能: 对数据进行简单明码编码，注意编码后的缓冲约为原来的两倍
    *   参数:
    *       in_pData         [输入] => 需要编码的数据
    *       in_lDataLen      [输入] => 需要编码的数据长度(Byte)
    *       out_pData        [输入] => 编码后的数据，内存由调用者分配(约为编码数据长度的2倍)
    *       in_out_lpLen[输入/输出] => 输入为存储缓冲长度，输出为编码后的实际长度
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_bin_PlainEecode(const void *in_pData, int in_lDataLen, void *out_pData, int *in_out_lpLen);

    /*========================================================
    *   功能: 对数据进行简单明码解码
    *   参数:
    *       in_pData         [输入] => 需要解码的数据
    *       in_lDataLen      [输入] => 需要解码的数据长度(Byte)
    *       out_pData        [输入] => 解码后的数据，内存由调用者分配(约为解码数据长度的3/4)
    *       in_out_lpLen[输入/输出] => 输入为存储缓冲长度，输出为解码后的实际长度
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_bin_PlainDecode(const void *in_pData, int in_lDataLen, void *out_pData, int *in_out_lpLen);

    /*========================================================
    *   功能: 对数据进行BIN编码，注意编码后的缓冲会增大
    *   参数:
    *       in_pData         [输入] => 需要编码的数据
    *       in_lDataLen      [输入] => 需要编码的数据长度(Byte)
    *       out_pData        [输入] => 编码后的数据，内存由调用者分配(最小为编码数据长度+32)
    *       in_out_lpLen[输入/输出] => 输入为存储缓冲长度，输出为编码后的实际长度
    *       in_eType         [输入] => 编码类型
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_bin_Encode(const void *in_pData, int in_lDataLen, void *out_pData, int *in_out_lpLen, EncodeType in_eType);

    /*========================================================
    *   功能: 对数据进行BIN解码
    *   参数:
    *       in_pData         [输入] => 需要解码的数据
    *       in_lDataLen      [输入] => 需要解码的数据长度(Byte)
    *       out_pData        [输入] => 解码后的数据，内存由调用者分配(最小为解码数据的长度)
    *       in_out_lpLen[输入/输出] => 输入为存储缓冲长度，输出为解码后的实际长度
    *       in_eType         [输入] => 解码类型
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_bin_Decode(const void *in_pData, int in_lDataLen, void *out_pData, int *in_out_lpLen, EncodeType in_eType);


    /****************************************************************************************
    ********************************  简单几何相关函数接口  **********************************
    *****************************************************************************************/
    /*========================================================
    *   功能: 计算两点之间的距离
    *   参数:
    *       p1[输入] => 点1
    *       p2[输入] => 点2
    *   返回:
    *       欧氏距离
    *========================================================*/
    extern double alg_geo_CalcDistance(ALG_POINT p1, ALG_POINT p2);

    /*========================================================
    *   功能: 计算两向量之间的角度
    *   参数:
    *       o[输入] => 顶角o点
    *       s[输入] => 起始边s点
    *       e[输入] => 终止边e点
    *   返回:
    *       顶角在o点，起始边为os，终止边为oe的夹角(单位：弧度)
    *       角度小于PI，返回正值
    *       角度大于PI，返回负值
    *========================================================*/
    extern double alg_geo_CalcAngleByPoint(ALG_POINT o, ALG_POINT s, ALG_POINT e);

    /*========================================================
    *   功能: 计算两线段之间的角度，单位：弧度 范围(-PI，PI)
    *   参数:
    *       l1[输入] => 起始边l1
    *       l2[输入] => 终止边l2
    *   返回:
    *       返回线段l1与l2之间的夹角
    *========================================================*/
    extern double alg_geo_CalcAngleByLineSeg(ALG_LINESEG l1, ALG_LINESEG l2);

    /*========================================================
    *   功能: 计算两线段之间的角度，单位：角度 范围(-180，180)
    *   参数:
    *       l1[输入] => 起始边l1
    *       l2[输入] => 终止边l2
    *   返回:
    *       返回线段l1与l2之间的夹角
    *========================================================*/
    extern int alg_geo_CalcAngleByLineSegEx(ALG_LINESEG l1, ALG_LINESEG l2);

    /*========================================================
    *   功能: 根据指定中心点旋转一个点(坐标)
    *   参数:
    *       in_tCen [输入] => 中心点
    *       in_tSrc [输入] => 原点
    *       out_pDst[输出] => 旋转后的点
    *       dAngle  [输出] => 旋转角度(弧度)
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_geo_RotatePoint(ALG_POINT in_tCen, ALG_POINT in_tSrc, PALG_POINT out_pDst, double dAngle);

    /*========================================================
    *   功能: 根据指定中心点旋转一个多边形
    *   参数:
    *       in_tCen             [输入] => 中心点
    *       in_out_pPolygon[输入/输出] => 输入为原多边形，输出为旋转后的多边形
    *       in_lNumOfSide       [输入] => 多边形边数
    *       dAngle              [输出] => 旋转角度(弧度)
    *   返回:
    *       ALG_ERR_NONE => 成功
    *       other        => 失败(参考错误代号)
    *========================================================*/
    extern int alg_geo_RotatePolygon(ALG_POINT in_tCen, PALG_POINT in_out_pPolygon, int in_lNumOfSide, double dAngle);

#ifdef __cplusplus
};
#endif

#endif // #ifndef __ALG_COMMON_H__2012
