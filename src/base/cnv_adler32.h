#ifndef __CNV_NET_ADLER32_H_2014_01_13__
#define __CNV_NET_ADLER32_H_2014_01_13__

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#include "cnv_core_typedef.h"

    /*=======================================================
      功能:
           checksum
      参数:
          [in]
      返回值:
          0    成功
          其它 失败
      =========================================================*/
    extern  unsigned int cnv_adler32_checksum(unsigned int adler, const K_UINT8 *buf, unsigned int len);

#ifdef __cplusplus
};
#endif // __cplusplus
#endif // __CNV_NET_ADLER32_H_2014_01_13__
