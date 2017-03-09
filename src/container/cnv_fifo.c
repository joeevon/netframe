#include "cnv_fifo.h"

/**
 * fls64 - find last bit set in a 64-bit value
 * @n: the value to search
 *
 * This is defined the same way as ffs:
 * - return 64..1 to indicate bit 63..0 most significant bit set
 * - return 0 to indicate no bits set
 */
int fls64(__u64 x)
{
    __u32 h = x >> 32;
    if(h)
        return fls(h) + 32;
    return fls(x);
}

unsigned fls_long(unsigned long l)
{
    if(sizeof(l) == 4)
        return fls(l);
    return fls64(l);
}

unsigned long roundup_pow_of_two(unsigned long x)
{
    return 1UL << fls_long(x - 1);
}

/**
 * * cnv_fifo_alloc - allocates a new FIFO and its internal buffer
 * * @size: the size of the internal buffer to be allocated.
 * * @gfp_mask: get_free_pages mask, passed to kmalloc()
 * * @lock: the lock to be used to protect the fifo buffer
 * *
 * * The size will be rounded-up to a power of 2.
 * */
cnv_fifo *cnv_fifo_alloc(unsigned int size)
{
    unsigned char *buffer;
    cnv_fifo *fifo;
    /*
     *       * round up to the next power of 2, since our 'let the indices
     *            * wrap' tachnique works only in this case.
     *                 */
    if(size & (size - 1))
    {
        if(size > 0x80000000)
            return NULL;
        size = roundup_pow_of_two(size);
    }

    buffer = (unsigned char *)malloc(size);
    if(!buffer)
        return NULL;

    fifo = (cnv_fifo *)malloc(sizeof(cnv_fifo));

    if(!fifo)
    {
        free(buffer);
        return NULL;
    }

    fifo->buffer = buffer;
    fifo->size = size;
    fifo->in = fifo->out = 0;

    return fifo;
}

/**
 * * cnv_fifo_free - frees the FIFO
 * * @fifo: the fifo to be freed.
 * */
void cnv_fifo_free(cnv_fifo *fifo)
{
    free(fifo->buffer);
    free(fifo);
}

/**
* cnv_fifo_put - puts some data into the FIFO, no locking version
* @fifo: the fifo to be used.
* @buffer: the data to be added.
* @len: the length of the data to be added.
*
* This function copies at most @len bytes from the @buffer into
* the FIFO depending on the free space, and returns the number of
* bytes copied.
*
* Note that with only one concurrent reader and one concurrent
* writer, you don't need extra locking to use these functions.
*/
unsigned int cnv_fifo_put(cnv_fifo *fifo, const unsigned char *buffer, unsigned int len)
{
    unsigned int l;

    len = min(len, fifo->size - fifo->in + fifo->out);

    /* first put the data starting from fifo->in to buffer end */
    l = min(len, fifo->size - (fifo->in & (fifo->size - 1)));
    memcpy(fifo->buffer + (fifo->in & (fifo->size - 1)), buffer, l);

    /* then put the rest (if any) at the beginning of the buffer */
    memcpy(fifo->buffer, buffer + l, len - l);

    fifo->in += len;

    return len;
}

/**
* cnv_fifo_get - gets some data from the FIFO, no locking version
* @fifo: the fifo to be used.
* @buffer: where the data must be copied.
* @len: the size of the destination buffer.
*
* This function copies at most @len bytes from the FIFO into the
* @buffer and returns the number of copied bytes.
*
* Note that with only one concurrent reader and one concurrent
* writer, you don't need extra locking to use these functions.
*/
unsigned int cnv_fifo_get(cnv_fifo *fifo, unsigned char *buffer, unsigned int len)
{
    unsigned int l;

    len = min(len, fifo->in - fifo->out);

    /* first get the data from fifo->out until the end of the buffer */
    l = min(len, fifo->size - (fifo->out & (fifo->size - 1)));
    memcpy(buffer, fifo->buffer + (fifo->out & (fifo->size - 1)), l);

    /* then get the rest (if any) from the beginning of the buffer */
    memcpy(buffer + l, fifo->buffer, len - l);

    fifo->out += len;

    return len;
}

/**
* cnv_fifo_reset - removes the entire FIFO contents, no locking version
* @fifo: the fifo to be emptied.
*/
void cnv_fifo_reset(cnv_fifo *fifo)
{
    fifo->in = fifo->out = 0;
}

/**
* cnv_fifo_len - returns the number of bytes available in the FIFO, no locking version
* @fifo: the fifo to be used.
*/
unsigned int cnv_fifo_len(cnv_fifo *fifo)
{
    return fifo->in - fifo->out;
}
