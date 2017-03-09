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
