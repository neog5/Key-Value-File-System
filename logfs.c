/**
 * Tony Givargis
 * Copyright (C), 2023
 * University of California, Irvine
 *
 * CS 238P - Operating Systems
 * logfs.c
 */

#include <pthread.h>
#include "device.h"
#include "logfs.h"

#define WCACHE_BLOCKS 32
#define RCACHE_BLOCKS 256
#define BLOCK_SIZE 4096 
#define CAPACITY device_size
/* ?? */

/**
 * Needs:
 *   pthread_create()
 *   pthread_join()
 *   pthread_mutex_init()
 *   pthread_mutex_destroy()
 *   pthread_mutex_lock()
 *   pthread_mutex_unlock()
 *   pthread_cond_init()
 *   pthread_cond_destroy()
 *   pthread_cond_wait()
 *   pthread_cond_signal()
 */

/* research the above Needed API and design accordingly */

struct w_buffer {
    void *buffer;       /* aligned */
    void *buffer_;      /* malloc/free */
    size_t head;        /* where the next write goes */
    size_t tail;        /* where the read goes */
    size_t buffer_size; /* fixed */
    size_t size;        /* distance from tail to head, could have been calculated using head-tail */
};

struct r_buffer {
    void *buffer;       /* aligned */
    void *buffer_;      /* malloc/free */
    size_t head;        /* where the next write goes */
    size_t tail;        /* where the read goes */
    size_t buffer_size; /* fixed */
};

struct worker {

};

struct logfs {
    struct w_buffer *w_buffer;
    size_t capacity;
    pthread_t worked;
    pthread_mutex_t lock;
    pthread_cond_t data_avail;
    pthread_cond_t space_avail;
    int done; /* as long as done is 0, keep writing | only make it 1 when closing */
    /* head moves infitely but buffer has a limited size, so we use head%buffer_size */
    /* a background thread copies items */
};


/* assert(0==(tail%block_size)) */

int worker(struct logfs *logfs) {
    pthread_mutex_lock(&(logfs->lock));
    while (!logfs->done) {
        if (logfs->w_buffer->size < BLOCK_SIZE) {
            pthread_cond_wait(&(logfs->data_avail), &(logfs->lock));
            continue;
        }
        //good
        size_t a_src = logfs->w_buffer->buffer + (logfs->w_buffer->tail%logfs->w_buffer->buffer_size);
        size_t a_dest = logfs->w_buffer->tail;

        device_write(a_src, a_dest, BLOCK_SIZE,0);
        logfs->w_buffer->tail += BLOCK_SIZE;
        logfs->w_buffer->size -= BLOCK_SIZE;
        pthread_cond_signal(&(logfs->space_avail));
    }
    
    pthread_mutex_unlock(&(logfs->lock));
    return 0;
}

int logfs_append(struct logfs *logfs, const void *buf, uint64_t len) {
    if((len+(logfs->w_buffer->head)) > logfs->capacity) {
        //ERROR no space
        TRACE("Not enough memory");
    }

    assert(len <= logfs->w_buffer->buffer_size);
    pthread_mutex_lock(&(logfs->lock));

    for(;;) {
        if((logfs->w_buffer->buffer_size - logfs->w_buffer->size) < len) {
            pthread_cond_wait(&(logfs->space_avail), &(logfs->lock));
            continue;  
        }
        break;
    }

    /* data split at the end of the buffer */
    if() {
        memcpy(logfs->w_buffer->buffer+(logfs->w_buffer->head%logfs->w_buffer->buffer_size),buf,len);
    } else {
        memcpy();
        memcpy();
    }

    logfs->w_buffer->head += len;
    logfs->w_buffer->size += len;

}
