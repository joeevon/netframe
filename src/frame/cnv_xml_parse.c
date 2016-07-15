#include "netframe/cnv_xml_parse.h"
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

int cnv_comm_xml_loadFile(char *strFilePath, char *strEncoding, void **ppOutDoc)
{
    //解析文件
    xmlDocPtr doc = xmlReadFile(strFilePath, strEncoding, XML_PARSE_RECOVER);
    if(NULL == doc)
    {
        return -1;
    }
    *ppOutDoc = doc;
    return 0;
}
