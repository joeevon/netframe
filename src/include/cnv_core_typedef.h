#ifndef __HMI_CORE_TYPEDEF_H_201210101733__    //用这个宏名避免其他项目调用引起重复
#define __HMI_CORE_TYPEDEF_H_201210101733__

#ifdef __cplusplus
extern "C"
{
#endif

    //前缀缩写K K:Careland(凯立德)

    typedef char K_CHAR;                    //ASCII 字符
    typedef unsigned char   K_UCHAR;
    typedef unsigned short K_WCHAR;         //unicode 字符

    typedef signed char K_INT8;             //8位整型
    typedef unsigned char K_UINT8;          //8位无符号整型

    typedef short K_INT16;                  //16位整型
    typedef unsigned short K_UINT16;        //16位无符号整型

    typedef int K_INT32;                   //32位整型
    typedef unsigned int K_UINT32;         //32位无符号整型

    typedef long long K_INT64;              //64位整型
    typedef unsigned long long K_UINT64;    //64位无符号整型

    typedef float K_FLOAT32;                //单精度(32位)浮点

    typedef double K_DOUBLE64;              //双精度(64位)浮点
    typedef void  *K_POINTER;                // 地址

#define K_FAILED (-1)
#define K_SUCCEED 0
#define K_NULL (0)

    //字节类型定义
    typedef K_UINT8 K_BYTE;
    //定义句柄类型
    typedef void *K_HANDLE;
    //函数执行失败
    //定义布尔类型
    typedef enum __enumK_BOOL
    {
        K_FALSE = 0x00000000,
        K_TRUE = 0x00000001,

    } K_BOOL;

    //无效socket
#define   INVALID_SOCKET -1

#ifdef __cplusplus
};
#endif

#endif  //__HMI_CORE_TYPEDEF_H_201210101733__
