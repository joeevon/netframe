#include "cnv_adler32.h"
#include "cnv_base_define.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>



unsigned int cnv_adler32_checksum(unsigned int adler, const K_UINT8 *buf, unsigned int len)
{
    unsigned int sum2;
    unsigned int n;

    /* split Adler-32 into component sums */
    sum2 = (adler >> 16) & 0xffff;
    adler &= 0xffff;

    /* in case user likes doing a byte at a time, keep it fast */
    if(len == 1)
    {
        adler += buf[0];
        if(adler >= BASE)
            adler -= BASE;
        sum2 += adler;
        if(sum2 >= BASE)
            sum2 -= BASE;
        return adler | (sum2 << 16);
    }

    /* initial Adler-32 value (deferred check for len == 1 speed) */
    if(buf == K_NULL)
        return 1L;

    /* in case short lengths are provided, keep it somewhat fast */
    if(len < 16)
    {
        while(len--)
        {
            adler += *buf++;
            sum2 += adler;
        }
        if(adler >= BASE)
            adler -= BASE;
        MOD4(sum2);             /* only added so many BASE's */
        return adler | (sum2 << 16);
    }

    /* do length NMAX blocks -- requires just one modulo operation */
    while(len >= NMAX)
    {
        len -= NMAX;
        n = NMAX / 16;          /* NMAX is divisible by 16 */
        do
        {
            DO16(buf);          /* 16 sums unrolled */
            buf += 16;
        }
        while(--n);
        MOD(adler);
        MOD(sum2);
    }

    /* do remaining bytes (less than NMAX, still just one modulo) */
    if(len)                     /* avoid modulos if none remaining */
    {
        while(len >= 16)
        {
            len -= 16;
            DO16(buf);
            buf += 16;
        }
        while(len--)
        {
            adler += *buf++;
            sum2 += adler;
        }
        MOD(adler);
        MOD(sum2);
    }

    /* return recombined sums */
    return adler | (sum2 << 16);
}
