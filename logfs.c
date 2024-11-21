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
    pthread_t thread;
    pthread_mutex_t lock;
    pthread_cond_t data_avail;
    pthread_cond_t space_avail;
    int done; /* as long as done is 0, keep writing | only make it 1 when closing */
    /* head moves infitely but buffer has a limited size, so we use head%buffer_size */
    /* a background thread copies items */
};

struct logfs {
    struct w_buffer *w_buffer;
    struct worker *worker;
    struct device *device;
    size_t device_capacity;
    size_t block_size;
};


/* assert(0==(tail%block_size)) */

void *worker(void *arg) {
    struct logfs *logfs = arg;
    pthread_mutex_lock(&(logfs->worker->lock));

    while (!logfs->worker->done) {
        if (logfs->w_buffer->size < logfs->block_size) {
            pthread_cond_wait(&(logfs->worker->data_avail), &(logfs->worker->lock));
            continue;
        }
        /*good*/

        /*device_write((size_t)logfs->w_buffer->buffer + (logfs->w_buffer->tail%logfs->w_buffer->buffer_size), logfs->w_buffer->tail, logfs->block_size,0);*/
        device_write(logfs->device, (void*)((size_t)logfs->w_buffer->buffer + (size_t)(logfs->w_buffer->tail%logfs->w_buffer->buffer_size)), logfs->w_buffer->tail, logfs->block_size);
        logfs->w_buffer->tail += logfs->block_size;
        logfs->w_buffer->size -= logfs->block_size;
        pthread_cond_signal(&(logfs->worker->space_avail));
    }
    
    pthread_mutex_unlock(&(logfs->worker->lock));
    return NULL;
}

int logfs_append(struct logfs *logfs, const void *buf, uint64_t len) {
    if((len+(logfs->w_buffer->head)) > logfs->device_capacity) {
        /*ERROR no space*/
        TRACE("Not enough memory");
    }

    UNUSED(buf);

    assert(len <= logfs->w_buffer->buffer_size);
    pthread_mutex_lock(&(logfs->worker->lock));

    for(;;) {
        if((logfs->w_buffer->buffer_size - logfs->w_buffer->size) < len) {
            pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock));
            continue;  
        }
        break;
    }

    /* data split at the end of the buffer 
    if() {
        memcpy(logfs->w_buffer->buffer+(logfs->w_buffer->head%logfs->w_buffer->buffer_size),buf,len);
    } else {
        memcpy();
        memcpy();
    }
    */

    logfs->w_buffer->head += len;
    logfs->w_buffer->size += len;
    return 0;
}

struct logfs *logfs_open(const char *pathname) {
    struct logfs  *logfs;

    assert(safe_strlen(pathname));

    if (!(logfs = malloc(sizeof(struct logfs)))) {
        TRACE("out of memory");
        return NULL;
    }
    memset(logfs, 0, sizeof(struct logfs));

    if (set_device(logfs, pathname) || set_w_buffer(logfs) ||/* setup_cache(logfs) ||*/ setup_worker(logfs)) {
        logfs_close(logfs);
        TRACE(0);
        return NULL;
    }

    return logfs;
}

/*my func*/
int set_device(struct logfs *logfs, const char *pathname) {
    if (!(logfs->device = device_open(pathname))) {
        return 1;
    }

    logfs->device_capacity = device_size(logfs->device);
    logfs->block_size = device_block(logfs->device);

    return 0;
}

/*my func*/
int set_w_buffer(struct logfs *logfs) {
    if (!(logfs->w_buffer = malloc(sizeof(struct w_buffer)))) {
        return 1;
    }
    memset(logfs->w_buffer, 0, sizeof(struct w_buffer));

    logfs->w_buffer->head = 0;
    logfs->w_buffer->tail = 0;
    logfs->w_buffer->buffer_size = logfs->block_size * WCACHE_BLOCKS;

    if (!(logfs->w_buffer->buffer_ = malloc(logfs->w_buffer->buffer_size + logfs->block_size))) {
        return 1;
    }
    memset(logfs->w_buffer->buffer_, 0, logfs->w_buffer->buffer_size);

    logfs->w_buffer->buffer = memory_align(logfs->w_buffer->buffer_,logfs->block_size);

    return 0;
}

/*my func*/
int setup_worker(struct logfs *logfs) {
    if (!(logfs->worker = malloc(sizeof(struct worker)))) {
        return 1;
    }
    memset(logfs->worker, 0, sizeof(struct worker));

    if (pthread_mutex_init(&logfs->worker->lock, NULL) ||
        pthread_cond_init(&logfs->worker->data_avail, NULL) ||
        pthread_cond_init(&logfs->worker->space_avail, NULL) ||
        pthread_create(&logfs->worker->thread, NULL, worker, logfs)) {
        return 1;
    }

    return 0;
}
