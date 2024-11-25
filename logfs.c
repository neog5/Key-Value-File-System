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

struct cache {
    void *data;         /* aligned */
    void *data_;
    int isValid;        /* malloc/free */
    size_t tag;      /* where the next write goes */
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
    struct cache cache[RCACHE_BLOCKS];
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
    size_t remain_block;
    size_t buf_head = (size_t)logfs->w_buffer->buffer+(logfs->w_buffer->head%logfs->w_buffer->buffer_size);

    if ((len + logfs->w_buffer->head) > logfs->device_capacity) {
        TRACE("Not enough memory");
        return -1;
    }

    if (logfs->w_buffer->head >= logfs->device_capacity) {
        TRACE("Head pointer out of bounds");
        return -1;
    }

    assert(len <= logfs->w_buffer->buffer_size);
    if (0 != pthread_mutex_lock(&(logfs->worker->lock))) {
            TRACE("error in flush data");
            exit(1);
    }

    for(;;) {
        if((logfs->w_buffer->buffer_size - logfs->w_buffer->size) < len) {
            pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock));
            continue;  
        }
        break;
    }

    /* data split at the end of the buffer */
    if((logfs->w_buffer->head + len)%(logfs->w_buffer->buffer_size) > (logfs->w_buffer->head)%(logfs->w_buffer->buffer_size)) {
        memcpy((void *)(buf_head),buf,len);
    } else {
        remain_block = logfs->w_buffer->buffer_size-(logfs->w_buffer->head%logfs->w_buffer->buffer_size);
        memcpy((void *)(buf_head),buf,remain_block);
        memcpy(logfs->w_buffer->buffer,(void *)((size_t)buf+remain_block),len-remain_block);
    }

    logfs->w_buffer->head += len;
    logfs->w_buffer->size += len;
    pthread_cond_signal(&(logfs->worker->data_avail));
    if (0 != pthread_mutex_unlock(&(logfs->worker->lock))) {
            TRACE("error in flush data");
            exit(1);
    }
    return 0;
}

void flush3(struct logfs *logfs) {
    size_t original_head;
    size_t original_tail;
    size_t original_size;

    pthread_mutex_lock(&(logfs->worker->lock)); /* Lock the worker to manipulate buffer safely */

    /* Save the original buffer state */
    original_head = logfs->w_buffer->head;
    original_tail = logfs->w_buffer->tail;
    original_size = logfs->w_buffer->size;

    /* Align head to a block boundary to ensure a valid write */
    if (logfs->w_buffer->size % logfs->block_size != 0) {
        size_t pad_size = logfs->block_size - (logfs->w_buffer->size % logfs->block_size);
        logfs->w_buffer->head = (logfs->w_buffer->head + pad_size) % logfs->w_buffer->buffer_size;
        logfs->w_buffer->size += pad_size;
    }

    /* Signal the worker to process the remaining data */
    while (logfs->w_buffer->size > 0) {
        pthread_cond_signal(&(logfs->worker->data_avail));
        pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock)); /* Wait for space */
    }

    /* Restore the original buffer state */
    logfs->w_buffer->head = original_head;
    logfs->w_buffer->tail = original_tail;
    logfs->w_buffer->size = original_size;

    pthread_mutex_unlock(&(logfs->worker->lock)); /* Release the lock */
}



int logfs_read(struct logfs *logfs, void *buf, uint64_t off, size_t len) {
    int disk_block_number = off / logfs->block_size;
    int cache_block_number = disk_block_number % RCACHE_BLOCKS;
    size_t tag_cur = cache_block_number / RCACHE_BLOCKS;
    uint64_t data_start, remaining_bytes, off_new, cur_block_len, temp_buf;

        /*printf("going");*/
    if((size_t)off + len > logfs->w_buffer->head) {

        /*TRACE("Data out of bound")*/
        return -1;
    }
        
    if((size_t)off + len > logfs->w_buffer->tail) {
        flush3(logfs);
    }

    remaining_bytes = len;
    off_new = off;
    temp_buf = (uint64_t)buf;

    while (0 < remaining_bytes) {
        disk_block_number = off_new / logfs->block_size;
        cache_block_number = disk_block_number % RCACHE_BLOCKS;
        tag_cur = disk_block_number / RCACHE_BLOCKS;

        if(!(logfs->cache[cache_block_number].isValid && logfs->cache[cache_block_number].tag == tag_cur)) {
            /* do device read */
            if(device_read(logfs->device, logfs->cache[cache_block_number].data, cache_block_number * logfs->block_size, logfs->block_size)) {
                /*TRACE*/
                TRACE("Device read fail");
                return -1;
            }
            logfs->cache[cache_block_number].isValid = 1;
            logfs->cache[cache_block_number].tag = tag_cur;
        }
        data_start = (size_t)logfs->cache[cache_block_number].data + ((size_t)off_new % logfs->block_size);
        cur_block_len = MIN(logfs->block_size - ((size_t)off_new % logfs->block_size),remaining_bytes);
        memcpy((void*)temp_buf,(void*)data_start,cur_block_len);

        off_new += cur_block_len;
        remaining_bytes -= cur_block_len;
        temp_buf += cur_block_len;

    }
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

    if (set_device(logfs, pathname) || set_w_buffer(logfs) || set_cache(logfs) || set_worker(logfs)) {
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
int set_worker(struct logfs *logfs) {
    if (!(logfs->worker = malloc(sizeof(struct worker)))) {
        return 1;
    }
    memset(logfs->worker, 0, sizeof(struct worker));

    if (0 != pthread_mutex_init(&logfs->worker->lock, NULL)) {
        perror("Mutex initialization failed");
        free(logfs);
        return -1;
    }
    if (0 != pthread_cond_init(&logfs->worker->space_avail, NULL)) {
        perror("Worker condition initialization failed");
        pthread_mutex_destroy(&logfs->worker->lock);
        free(logfs);
        return -1;
    }
    if (0 != pthread_cond_init(&logfs->worker->data_avail, NULL)) {
        perror("User condition initialization failed");
        pthread_cond_destroy(&logfs->worker->space_avail);
        pthread_mutex_destroy(&logfs->worker->lock);
        free(logfs);
        return -1;
    }
    if (0 != pthread_cond_init(&logfs->worker->space_avail, NULL)) {
        perror("Worker condition initialization failed");
        pthread_mutex_destroy(&logfs->worker->lock);
        free(logfs);
        return -1;
    }
    if (0 != pthread_cond_init(&logfs->worker->data_avail, NULL)) {
        perror("User condition initialization failed");
        pthread_cond_destroy(&logfs->worker->space_avail);
        pthread_mutex_destroy(&logfs->worker->lock);
        free(logfs);
        return -1;
    }
    if (0 != pthread_create(&logfs->worker->thread, NULL, worker, logfs)) {
        perror("Thread creation failed");
        pthread_cond_destroy(&logfs->worker->space_avail);
        pthread_cond_destroy(&logfs->worker->data_avail);
        pthread_mutex_destroy(&logfs->worker->lock);
        free(logfs);
        return -1;
    }

    logfs->worker->done = 0;

    return 0;

}

int set_cache(struct logfs *logfs) {
   /* if (!(logfs->cache = malloc(sizeof(struct cache) * RCACHE_BLOCKS))) {
        return 1;
    }*/
    /* memset(logfs->cache, 0, sizeof(struct cache) * RCACHE_BLOCKS);*/
    
    int i;
    for(i = 0; i < RCACHE_BLOCKS; i++) {
        if(!(logfs->cache[i].data_ = malloc(logfs->block_size * 2))) {
            TRACE("block memory allocation error");
            return 1;
        }
        logfs->cache[i].data = memory_align(logfs->cache[i].data_, logfs->block_size);
        logfs->cache[i].isValid = 0;
        logfs->cache[i].tag = -1;
    }
    return 0;
    
}

void logfs_close(struct logfs *logfs) {
    int i;

    assert(logfs);
    /*
    if (RESTORE_FROM_FILE) {
        set_metadata(logfs, logfs->utilized);
    }
    */

    if (logfs) {
        if (logfs->worker) {
            if (pthread_mutex_lock(&logfs->worker->lock)) {
                TRACE("pthread_mutex_lock()");
            }
            logfs->worker->done = 1;
            if (pthread_cond_signal(&logfs->worker->data_avail)) {
                TRACE("pthread_cond_signal()");
            }
            if (pthread_cond_signal(&logfs->worker->space_avail)) {
                TRACE("pthread_cond_signal()");
            }
            if (pthread_mutex_unlock(&logfs->worker->lock)) {
                TRACE("pthread_mutex_unlock()");
            }/*
            if (pthread_join(logfs->worker->thread, NULL)) {
                TRACE("pthread_join()");
            }
            */
            pthread_mutex_destroy(&logfs->worker->lock);
            pthread_cond_destroy(&logfs->worker->data_avail);
            pthread_cond_destroy(&logfs->worker->space_avail);
        }
        if (logfs->w_buffer) {
            FREE(logfs->w_buffer->buffer_);
            /*FREE(logfs->w_buffer);*/
        }
        for (i = 0; i < RCACHE_BLOCKS; i++) {
            if (logfs->cache[i].data_) {
                FREE(logfs->cache[i].data_);
            }
        }
        /*
        if (logfs->worker) {
            FREE(logfs->worker);
        }
        if (logfs->w_buffer) {
            FREE(logfs->w_buffer);
        }
        */
        if (logfs->device) {
            device_close(logfs->device);
        }
        memset(logfs, 0, sizeof(struct logfs));
        FREE(logfs);
    }
}




/*interval analysis method instead of flush?*/
/*flush needed for extra credit*/
/**/