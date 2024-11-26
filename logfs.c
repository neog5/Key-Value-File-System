

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
    void *data_;        /* malloc/free */
    int isValid;        
    size_t tag;         /* where the next write goes */
};

struct worker {
    pthread_t thread;
    pthread_mutex_t lock;           /* used to the lock the resources during its execution */
    pthread_cond_t data_avail;
    pthread_cond_t space_avail;
    int done;                       /* as long as done is 0, keep writing | only make it 1 when closing */
};

struct logfs {
    struct w_buffer *w_buffer;
    struct cache cache[RCACHE_BLOCKS];
    struct worker *worker;
    struct device *device;
    size_t device_capacity;
    size_t block_size;
};

/**
 * worker
 * -----------------------------------------------------
 * Background thread to handle buffered writes to the device.
 */
void *worker_thread(void *arg) {
    struct logfs *logfs = arg;

    if (pthread_mutex_lock(&(logfs->worker->lock)) != 0) {
        TRACE("Worker: Failed to lock mutex");
        return NULL;
    }

    while (!logfs->worker->done) {
        /** Wait for data availability if the buffer is below block size */
        if (logfs->w_buffer->size < logfs->block_size) {
            if (pthread_cond_wait(&(logfs->worker->data_avail), &(logfs->worker->lock)) != 0) {
                TRACE("Worker: Failed to wait for condition");
                break;
            }
            continue;
        }

        /** Write data to device */
        if (device_write(logfs->device, 
                         (void *)((size_t)logfs->w_buffer->buffer + (logfs->w_buffer->tail % logfs->w_buffer->buffer_size)), 
                         logfs->w_buffer->tail, logfs->block_size)) {
            TRACE("Worker: Device write failed");
            break;
        }

        /** Update tail and size */
        logfs->w_buffer->tail += logfs->block_size;
        logfs->w_buffer->size -= logfs->block_size;

        /** Notify space availability */
        if (pthread_cond_signal(&(logfs->worker->space_avail)) != 0) {
            TRACE("Worker: Failed to signal space availability");
            break;
        }
    }

    if (pthread_mutex_unlock(&(logfs->worker->lock)) != 0) {
        TRACE("Worker: Failed to unlock mutex");
    }

    return NULL;
}


/**
 * logfs_open
 * -----------------------------------------------------
 * Opens and initializes a logfs instance.
 */
struct logfs *logfs_open(const char *pathname) {
    struct logfs *logfs;

    /** Ensure the pathname is valid */
    assert(pathname && strlen(pathname) > 0);

    /** Allocate memory for the logfs structure */
    if (!(logfs = (struct logfs *)malloc(sizeof(struct logfs)))) {
        TRACE("logfs_open: Memory allocation failed for logfs structure");
        return NULL;
    }
    memset(logfs, 0, sizeof(struct logfs));

    /** Initialize components */
    if (set_device(logfs, pathname) || set_w_buffer(logfs) || set_cache(logfs) || set_worker(logfs)) {
        logfs_close(logfs);
        TRACE("logfs_open: Initialization failed");
        return NULL;
    }

    return logfs;
}

/**
 * set_device
 * -----------------------------------------------------
 * Sets up the device for logfs operations.
 */
int set_device(struct logfs *logfs, const char *pathname) {
    /** Open the device */
    if (!(logfs->device = device_open(pathname))) {
        TRACE("set_device: Device open failed");
        return 1;
    }

    /** Retrieve device properties */
    logfs->device_capacity = device_size(logfs->device);
    logfs->block_size = device_block(logfs->device);

    return 0;
}

/**
 * set_w_buffer
 * -----------------------------------------------------
 * Initializes the write buffer for logfs.
 */
int set_w_buffer(struct logfs *logfs) {
    /** Allocate and initialize the write buffer structure */
    if (!(logfs->w_buffer = malloc(sizeof(struct w_buffer)))) {
        TRACE("set_w_buffer: Memory allocation failed for write buffer");
        return 1;
    }
    memset(logfs->w_buffer, 0, sizeof(struct w_buffer));

    /** Set initial buffer parameters */
    logfs->w_buffer->head = 0;
    logfs->w_buffer->tail = 0;
    logfs->w_buffer->size = 0;
    logfs->w_buffer->buffer_size = logfs->block_size * WCACHE_BLOCKS;

    /** Allocate memory for the buffer and align it */
    if (!(logfs->w_buffer->buffer_ = malloc(logfs->w_buffer->buffer_size + logfs->block_size))) {
        TRACE("set_w_buffer: Memory allocation failed for buffer storage");
        return 1;
    }
    memset(logfs->w_buffer->buffer_, 0, logfs->w_buffer->buffer_size + logfs->block_size);
    logfs->w_buffer->buffer = memory_align(logfs->w_buffer->buffer_, logfs->block_size);

    return 0;
}

/**
 * set_cache
 * -----------------------------------------------------
 * Initializes the cache for logfs.
 */
int set_cache(struct logfs *logfs) {
    int i;

    /** Allocate and align cache block memory */
    for (i = 0; i < RCACHE_BLOCKS; i++) {
        if (!(logfs->cache[i].data_ = malloc(logfs->block_size * 2))) {
            TRACE("set_cache: Memory allocation failed for cache block");
            return 1;
        }
        memset(logfs->cache[i].data_, 0, logfs->block_size * 2);

        logfs->cache[i].data = memory_align(logfs->cache[i].data_, logfs->block_size);
        logfs->cache[i].isValid = 0;
        logfs->cache[i].tag = -1;
    }

    return 0;
}


/**
 * set_worker
 * -----------------------------------------------------
 * Sets up the background worker thread for logfs.
 */
int set_worker(struct logfs *logfs) {
    /** Allocate memory for the worker structure */
    if (!(logfs->worker = malloc(sizeof(struct worker)))) {
        TRACE("set_worker: Memory allocation failed for worker");
        return -1;
    }
    memset(logfs->worker, 0, sizeof(struct worker));

    /** Initialize mutex and condition variables */
    if (pthread_mutex_init(&logfs->worker->lock, NULL) != 0) {
        TRACE("set_worker: Mutex initialization failed");
        FREE(logfs->worker);
        return -1;
    }
    if (pthread_cond_init(&logfs->worker->space_avail, NULL) != 0 ||
        pthread_cond_init(&logfs->worker->data_avail, NULL) != 0) {
        TRACE("set_worker: Condition variable initialization failed");
        pthread_mutex_destroy(&logfs->worker->lock);
        FREE(logfs->worker);
        return -1;
    }

    /** Create the worker thread */
    if (pthread_create(&logfs->worker->thread, NULL, worker_thread, logfs) != 0) {
        TRACE("set_worker: Worker thread creation failed");
        pthread_cond_destroy(&logfs->worker->space_avail);
        pthread_cond_destroy(&logfs->worker->data_avail);
        pthread_mutex_destroy(&logfs->worker->lock);
        FREE(logfs->worker);
        return -1;
    }

    logfs->worker->done = 0;
    return 0;
}


/**
 * logfs_append
 * -----------------------------------------------------
 * Appends data to the logfs write buffer.
 */
int logfs_append(struct logfs *logfs, const void *buf, uint64_t len) {
    size_t buf_head, remain_block;

    if (len == 0) return 0;

    /** Check device capacity */
    if ((len + logfs->w_buffer->head) > logfs->device_capacity) {
        TRACE("logfs_append: Insufficient device capacity");
        return -1;
    }

    assert(len <= logfs->w_buffer->buffer_size);

    /** Lock worker mutex */
    if (pthread_mutex_lock(&(logfs->worker->lock)) != 0) {
        TRACE("logfs_append: Failed to lock mutex");
        return -1;
    }

    /** Wait for sufficient buffer space */
    while ((logfs->w_buffer->buffer_size - logfs->w_buffer->size) < len) {
        if (pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock)) != 0) {
            TRACE("logfs_append: Failed to wait for space condition");
            pthread_mutex_unlock(&(logfs->worker->lock));
            return -1;
        }
    }

    /** Copy data to buffer, handling wrap-around */
    buf_head = (size_t)logfs->w_buffer->buffer + (logfs->w_buffer->head % logfs->w_buffer->buffer_size);
    if ((logfs->w_buffer->head + len) % logfs->w_buffer->buffer_size > 
        logfs->w_buffer->head % logfs->w_buffer->buffer_size) {
        memcpy((void *)buf_head, buf, len);
    } else {
        remain_block = logfs->w_buffer->buffer_size - (logfs->w_buffer->head % logfs->w_buffer->buffer_size);
        memcpy((void *)buf_head, buf, remain_block);
        memcpy(logfs->w_buffer->buffer, (void *)((size_t)buf + remain_block), len - remain_block);
    }

    /** Update buffer state */
    logfs->w_buffer->head += len;
    logfs->w_buffer->size += len;

    /** Notify worker thread */
    if (pthread_cond_signal(&(logfs->worker->data_avail)) != 0) {
        TRACE("logfs_append: Failed to signal data availability");
        pthread_mutex_unlock(&(logfs->worker->lock));
        return -1;
    }

    /** Unlock worker mutex */
    if (pthread_mutex_unlock(&(logfs->worker->lock)) != 0) {
        TRACE("logfs_append: Failed to unlock mutex");
        return -1;
    }

    return 0;
}

/**
 * flush
 * -----------------------------------------------------
 * Flushes all data from the write buffer to the device.
 */
void flush(struct logfs *logfs) {
    size_t original_head, original_tail, original_size;

    if (pthread_mutex_lock(&(logfs->worker->lock)) != 0) {
        TRACE("flush: Failed to lock mutex");
        return;
    }

    /** Wait for sufficient space to process blocks */
    while (logfs->w_buffer->size > logfs->block_size) {
        if (pthread_cond_signal(&(logfs->worker->data_avail)) != 0) {
            TRACE("flush: Failed to signal data availability");
            break;
        }
        if (pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock)) != 0) {
            TRACE("flush: Failed to wait for space condition");
            break;
        }
    }

    /** Save original state */
    original_head = logfs->w_buffer->head;
    original_tail = logfs->w_buffer->tail;
    original_size = logfs->w_buffer->size;

    /** Align head to block boundary */
    if (logfs->w_buffer->size % logfs->block_size != 0) {
        size_t pad_size = logfs->block_size - (logfs->w_buffer->size % logfs->block_size);
        logfs->w_buffer->head += pad_size;
        logfs->w_buffer->size += pad_size;
        assert(logfs->w_buffer->size % logfs->block_size == 0);
    }

    /** Process remaining data */
    while (logfs->w_buffer->size > 0) {
        if (pthread_cond_signal(&(logfs->worker->data_avail)) != 0) {
            TRACE("flush: Failed to signal data availability");
            break;
        }
        if (pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock)) != 0) {
            TRACE("flush: Failed to wait for space condition");
            break;
        }
    }

    /** Restore original state */
    logfs->w_buffer->head = original_head;
    logfs->w_buffer->tail = original_tail;
    logfs->w_buffer->size = original_size;

    if (pthread_mutex_unlock(&(logfs->worker->lock)) != 0) {
        TRACE("flush: Failed to unlock mutex");
    }

}

/**
 * logfs_read
 * -----------------------------------------------------
 * Reads data from the logfs device, using cache for optimization.
 */
int logfs_read(struct logfs *logfs, void *buf, uint64_t off, size_t len) {
    size_t disk_block_number, cache_block_number;
    size_t tag_cur, data_start, cur_block_len, remaining_bytes, off_new;
    uint64_t temp_buf;

    if (len == 0) return 0;

    /** Check for out-of-bound reads */
    if ((size_t)(off + len) > logfs->w_buffer->head) {
        TRACE("logfs_read: Requested read exceeds buffer head");
        return -1;
    }

    /** Ensure all pending writes are flushed for reads crossing tail */
    if ((size_t)(off + len) > logfs->w_buffer->tail) {
        flush(logfs);
    }

    remaining_bytes = len;
    off_new = off;
    temp_buf = (uint64_t)buf;

    while (remaining_bytes > 0) {
        /** Determine block and cache information */
        disk_block_number = off_new / logfs->block_size;
        cache_block_number = disk_block_number % RCACHE_BLOCKS;
        tag_cur = disk_block_number / RCACHE_BLOCKS;

        /** Check cache validity */
        if (!(logfs->cache[cache_block_number].isValid && logfs->cache[cache_block_number].tag == tag_cur)) {
            if (device_read(logfs->device, logfs->cache[cache_block_number].data, 
                            disk_block_number * logfs->block_size, logfs->block_size)) {
                TRACE("logfs_read: Device read failed");
                return -1;
            }
            logfs->cache[cache_block_number].isValid = 1;
            logfs->cache[cache_block_number].tag = tag_cur;
        }

        /** Copy data from cache to the buffer */
        data_start = (size_t)logfs->cache[cache_block_number].data + (off_new % logfs->block_size);
        cur_block_len = MIN(logfs->block_size - (off_new % logfs->block_size), remaining_bytes);
        memcpy((void *)temp_buf, (void *)data_start, cur_block_len);

        /** Update counters */
        off_new += cur_block_len;
        remaining_bytes -= cur_block_len;
        temp_buf += cur_block_len;
    }

    /** Invalidate cache if read went beyond the tail */
    if (off + len > logfs->w_buffer->tail) {
        cache_block_number = ((off + len) / logfs->block_size) % RCACHE_BLOCKS;
        logfs->cache[cache_block_number].isValid = 0;
        logfs->cache[cache_block_number].tag = (size_t)-1;
    }

    return 0;
}

/**
 * logfs_close
 * -----------------------------------------------------
 * Closes and cleans up a logfs instance.
 */
void logfs_close(struct logfs *logfs) {
    int i;

    /** Validate the logfs pointer */
    if (!logfs) return;

    /** Flush the write buffer */
    flush(logfs);

    /** Clean up the worker thread */
    if (logfs->worker) {
        pthread_mutex_lock(&logfs->worker->lock);

        /** Signal the worker thread to finish */
        logfs->worker->done = 1;
        pthread_cond_signal(&logfs->worker->data_avail);

        pthread_mutex_unlock(&logfs->worker->lock);

        /** Wait for the worker thread to terminate */
        pthread_join(logfs->worker->thread, NULL);

        /** Destroy mutex and condition variables */
        pthread_mutex_destroy(&logfs->worker->lock);
        pthread_cond_destroy(&logfs->worker->space_avail);
        pthread_cond_destroy(&logfs->worker->data_avail);

        /** Free the worker structure */
        FREE(logfs->worker);
    }

    /** Free the write buffer */
    if (logfs->w_buffer) {
        FREE(logfs->w_buffer->buffer_);
        FREE(logfs->w_buffer);
    }

    /** Free the cache */
    for (i = 0; i < RCACHE_BLOCKS; i++) {
        if (logfs->cache[i].data_) {
            FREE(logfs->cache[i].data_);
        }
    }

    /** Close the device */
    if (logfs->device) {
        device_close(logfs->device);
    }

    /** Free the logfs structure */
    FREE(logfs);
}
