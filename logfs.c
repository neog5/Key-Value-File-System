

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

    if (pthread_mutex_lock(&(logfs->worker->lock)) != 0) {
        TRACE("Failed to lock worker mutex in worker thread");
        return NULL;
    }

    while (!logfs->worker->done) {
        if (logfs->w_buffer->size < logfs->block_size) {
            if (pthread_cond_wait(&(logfs->worker->data_avail), &(logfs->worker->lock)) != 0) {
                TRACE("Failed to wait for data_avail condition");
                break;
            }
            continue;
        }

        if (device_write(logfs->device, 
                         (void *)((size_t)logfs->w_buffer->buffer + (logfs->w_buffer->tail % logfs->w_buffer->buffer_size)), 
                         logfs->w_buffer->tail, logfs->block_size)) {
            TRACE("Device write failed");
            break;
        }

        logfs->w_buffer->tail += logfs->block_size;
        logfs->w_buffer->size -= logfs->block_size;

        if (pthread_cond_signal(&(logfs->worker->space_avail)) != 0) {
            TRACE("Failed to signal space_avail condition");
            break;
        }
    }

    if (pthread_mutex_unlock(&(logfs->worker->lock)) != 0) {
        TRACE("Failed to unlock worker mutex in worker thread");
    }
    return NULL;
}
/**
 * logfs_append
 * -----------------------------------------------------
 * Functionality:
 * Appends data to the write buffer (`w_buffer`) of the
 * `logfs` structure while ensuring thread-safe access.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *   - const void *buf: Pointer to the data to append.
 *   - uint64_t len: Length of the data to append.
 *
 * Output:
 *   - Returns 0 on success.
 *   - Returns -1 if there's insufficient memory or other errors.
 *
 * Errors:
 *   - TRACE logs are added for insufficient memory, mutex lock/unlock
 *     failures, and thread condition signaling issues.
 */
int logfs_append(struct logfs *logfs, const void *buf, uint64_t len) {
    size_t buf_head;
    size_t remain_block;
/*
    TRACE("Write:");
    TRACE("head:");
    fprintf(stderr, "%lu\n", logfs->w_buffer->head);
    TRACE("len:");
    fprintf(stderr, "%lu\n\n", len);
    fprintf(stderr, "%s\n\n", (char *)buf);
*/


    if(len == 0) return 0;

    /* Check if there's enough device capacity */
    if ((len + logfs->w_buffer->head) > logfs->device_capacity) {
        TRACE("Not enough memory to append data");
        return -1;
    }

    /* Ensure data fits in the buffer */
    assert(len <= logfs->w_buffer->buffer_size);

    if (pthread_mutex_lock(&(logfs->worker->lock)) != 0) {
        TRACE("Failed to lock worker mutex in logfs_append");
        return -1;
    }

    /* Wait if there isn't enough space in the buffer */
    while ((logfs->w_buffer->buffer_size - logfs->w_buffer->size) <= len) {
        if (pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock)) != 0) {
            TRACE("Failed to wait for space_avail condition in logfs_append");
            pthread_mutex_unlock(&(logfs->worker->lock));
            return -1;
        }
    }

    /* Append data, handling potential wrap-around at the end of the buffer */
    buf_head = (size_t)logfs->w_buffer->buffer + (logfs->w_buffer->head % logfs->w_buffer->buffer_size);
    if ((logfs->w_buffer->head + len) % logfs->w_buffer->buffer_size > 
        logfs->w_buffer->head % logfs->w_buffer->buffer_size) {
        memcpy((void *)buf_head, buf, len);
    } else {
        remain_block = logfs->w_buffer->buffer_size - (logfs->w_buffer->head % logfs->w_buffer->buffer_size);
        memcpy((void *)buf_head, buf, remain_block);
        memcpy(logfs->w_buffer->buffer, (void *)((size_t)buf + remain_block), len - remain_block);
    }

    /* Update buffer state */
    logfs->w_buffer->head += len;
    logfs->w_buffer->size += len;

    /* Notify the worker thread that data is available */
    if (pthread_cond_signal(&(logfs->worker->data_avail)) != 0) {
        TRACE("Failed to signal data_avail condition in logfs_append");
        pthread_mutex_unlock(&(logfs->worker->lock));
        return -1;
    }

    if (pthread_mutex_unlock(&(logfs->worker->lock)) != 0) {
        TRACE("Failed to unlock worker mutex in logfs_append");
        return -1;
    }

    return 0;
}
/**
 * flush
 * -----------------------------------------------------
 * Functionality:
 * Flushes all data in the write buffer (`w_buffer`) to
 * the device, ensuring block alignment. Signals the
 * worker thread to process remaining data.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *
 * Output:
 *   - None
 *
 * Errors:
 *   - TRACE logs for mutex lock/unlock failures and condition signaling issues.
 */
void flush(struct logfs *logfs) {
    size_t original_head, original_tail, original_size;

    /* Lock the worker mutex for safe manipulation of the buffer */
    if (pthread_mutex_lock(&(logfs->worker->lock)) != 0) {
        TRACE("Failed to lock worker mutex in flush3");
        return;
    }

    /* Wait until enough space is available for a complete block */
    while (logfs->w_buffer->size > logfs->block_size) {
        if (pthread_cond_signal(&(logfs->worker->data_avail)) != 0) {
            TRACE("Failed to signal data_avail condition in flush3");
            break;
        }
        if (pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock)) != 0) {
            TRACE("Failed to wait for space_avail condition in flush3");
            break;
        }
    }
    /* Save the original buffer state */
    original_head = logfs->w_buffer->head;
    original_tail = logfs->w_buffer->tail;
    original_size = logfs->w_buffer->size;


    /* Align the head to the block boundary */
    if (logfs->w_buffer->size % logfs->block_size != 0) {
        size_t pad_size = logfs->block_size - (logfs->w_buffer->size % logfs->block_size);
        logfs->w_buffer->head += pad_size;
        logfs->w_buffer->size += pad_size;
        assert(logfs->w_buffer->size % logfs->block_size == 0);
    }

    /* Signal the worker thread and process remaining data */
    while (logfs->w_buffer->size > 0) {
        if (pthread_cond_signal(&(logfs->worker->data_avail)) != 0) {
            TRACE("Failed to signal data_avail condition in flush3");
            break;
        }
        if (pthread_cond_wait(&(logfs->worker->space_avail), &(logfs->worker->lock)) != 0) {
            TRACE("Failed to wait for space_avail condition in flush3");
            break;
        }
    }

    /* Restore the original buffer state */
    logfs->w_buffer->head = original_head;
    logfs->w_buffer->tail = original_tail;
    logfs->w_buffer->size = original_size;

    /* Unlock the worker mutex */
    if (pthread_mutex_unlock(&(logfs->worker->lock)) != 0) {
        TRACE("Failed to unlock worker mutex in flush3");
    }
}
/**
 * logfs_read
 * -----------------------------------------------------
 * Functionality:
 * Reads data from the `logfs` device. Manages cache for 
 * optimized reading and ensures all required data is 
 * available.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *   - void *buf: Buffer to store the read data.
 *   - uint64_t off: Offset from where data is to be read.
 *   - size_t len: Number of bytes to read.
 *
 * Output:
 *   - 0 on success.
 *   - -1 on failure with TRACE logging.
 *
 * Errors:
 *   - TRACE logs for out-of-bound reads and device read failures.
 */
int logfs_read(struct logfs *logfs, void *buf, uint64_t off, size_t len) {
    size_t disk_block_number, cache_block_number;
    size_t tag_cur, data_start, cur_block_len, remaining_bytes, off_new;
    uint64_t temp_buf;

    /*
    TRACE("logfs_read called");
    TRACE("Parameters:");
    TRACE("off: ");
    fprintf(stderr, "%lu\n", off);  
    TRACE("len: ");
    fprintf(stderr, "%zu\n", len);  
    fprintf(stderr, "\n");  
    */


    if(len == 0) return 0;

    /* Check if the read operation exceeds the buffer head */
    if ((size_t)(off + len) > logfs->w_buffer->head) {
        TRACE("Read out of bounds: Requested data exceeds head");
        return -1;
    }

    /* Ensure all pending writes are flushed if the read crosses the tail */
    if ((size_t)(off + len) > logfs->w_buffer->tail) {
        flush(logfs);
    }

    remaining_bytes = len;
    off_new = off;
    temp_buf = (uint64_t)buf;

    while (remaining_bytes > 0) {
        /*TRACE("inside while");*/
        disk_block_number = off_new / logfs->block_size;
        cache_block_number = disk_block_number % RCACHE_BLOCKS;
        tag_cur = disk_block_number / RCACHE_BLOCKS;

        /* If data is not in cache or tag mismatch, fetch from device */
        if (!(logfs->cache[cache_block_number].isValid && logfs->cache[cache_block_number].tag == tag_cur)) {
            /*TRACE("CACHE MISS");*/
            if (device_read(logfs->device, logfs->cache[cache_block_number].data, disk_block_number * logfs->block_size, logfs->block_size)) {
                TRACE("Device read failed during logfs_read");
                return -1;
            }
            logfs->cache[cache_block_number].isValid = 1;
            logfs->cache[cache_block_number].tag = tag_cur;
        }

        /* Calculate data position and copy to buffer */
        data_start = (size_t)logfs->cache[cache_block_number].data + (off_new % logfs->block_size);
        cur_block_len = MIN(logfs->block_size - (off_new % logfs->block_size), remaining_bytes);
        memcpy((void *)temp_buf, (void *)data_start, cur_block_len);

        /* Update pointers and counters */
        off_new += cur_block_len;
        remaining_bytes -= cur_block_len;
        temp_buf += cur_block_len;
    }

    /* Invalidate cache if the read operation went beyond the tail */
    if (off + len > logfs->w_buffer->tail) {
        cache_block_number = ((off + len) / logfs->block_size) % RCACHE_BLOCKS;
        logfs->cache[cache_block_number].isValid = 0;
        logfs->cache[cache_block_number].tag = (size_t)-1;
    }

    /*fprintf(stderr, "string: %s\n\n", (char *)buf);*/

    return 0;
}

/**
 * logfs_open
 * -----------------------------------------------------
 * Functionality:
 * Opens and initializes a logfs instance.
 *
 * Input:
 *   - const char *pathname: Path to the storage device.
 *
 * Output:
 *   - Pointer to the initialized `logfs` structure on success.
 *   - NULL on failure with TRACE logging.
 *
 * Errors:
 *   - Memory allocation failures.
 *   - Device initialization issues.
 *   - Errors in setting up write buffer, cache, or worker thread.
 */
struct logfs *logfs_open(const char *pathname) {
    struct logfs *logfs;

    /* Ensure the pathname is valid */
    assert(pathname && strlen(pathname) > 0);

    /* Allocate memory for the logfs structure */
    if (!(logfs = (struct logfs *)malloc(sizeof(struct logfs)))) {
        TRACE("Memory allocation failed for logfs structure");
        return NULL;
    }
    memset(logfs, 0, sizeof(struct logfs));

    /* Initialize components */
    if (set_device(logfs, pathname) || set_w_buffer(logfs) || set_cache(logfs) || set_worker(logfs)) {
        logfs_close(logfs);  
        TRACE("logfs initialization failed");
        return NULL;
    }

    return logfs;
}
/**
 * set_device
 * -----------------------------------------------------
 * Functionality:
 * Sets up the device for `logfs` operations.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *   - const char *pathname: Path to the storage device.
 *
 * Output:
 *   - 0 on success.
 *   - 1 on failure with TRACE logging.
 *
 * Errors:
 *   - Device open failure.
 */
int set_device(struct logfs *logfs, const char *pathname) {
    if (!(logfs->device = device_open(pathname))) {
        TRACE("Device open failed");
        return 1;
    }

    logfs->device_capacity = device_size(logfs->device);
    logfs->block_size = device_block(logfs->device);

    return 0;
}
/**
 * set_w_buffer
 * -----------------------------------------------------
 * Functionality:
 * Initializes the write buffer for `logfs`.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *
 * Output:
 *   - 0 on success.
 *   - 1 on failure with TRACE logging.
 *
 * Errors:
 *   - Memory allocation failures.
 */
int set_w_buffer(struct logfs *logfs) {
    /* Allocate and initialize the write buffer structure */
    if (!(logfs->w_buffer = malloc(sizeof(struct w_buffer)))) {
        TRACE("Memory allocation failed for write buffer");
        return 1;
    }
    memset(logfs->w_buffer, 0, sizeof(struct w_buffer));

    logfs->w_buffer->head = 0;
    logfs->w_buffer->tail = 0;
    logfs->w_buffer->size = 0;
    logfs->w_buffer->buffer_size = logfs->block_size * WCACHE_BLOCKS;

    /* Allocate memory for the buffer and align it */
    if (!(logfs->w_buffer->buffer_ = malloc(logfs->w_buffer->buffer_size + logfs->block_size))) {
        TRACE("Memory allocation failed for write buffer storage");
        return 1;
    }
    memset(logfs->w_buffer->buffer_, 0, logfs->w_buffer->buffer_size + logfs->block_size);
    logfs->w_buffer->buffer = memory_align(logfs->w_buffer->buffer_, logfs->block_size);

    return 0;
}
/**
 * set_cache
 * -----------------------------------------------------
 * Functionality:
 * Initializes the cache for `logfs`.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *
 * Output:
 *   - 0 on success.
 *   - 1 on failure with TRACE logging.
 *
 * Errors:
 *   - Memory allocation failures.
 */
int set_cache(struct logfs *logfs) {
    int i;
    for (i = 0; i < RCACHE_BLOCKS; i++) {
        /* Allocate and align cache block memory */
        if (!(logfs->cache[i].data_ = malloc(logfs->block_size * 2))) {
            TRACE("Memory allocation failed for cache block");
            return 1;
        }
        logfs->cache[i].data = memory_align(logfs->cache[i].data_, logfs->block_size);
        logfs->cache[i].isValid = 0;
        logfs->cache[i].tag = -1;
    }
    return 0;
}
/**
 * set_worker
 * -----------------------------------------------------
 * Functionality:
 * Sets up the background worker thread for `logfs`.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *
 * Output:
 *   - 0 on success.
 *   - -1 on failure with TRACE logging.
 *
 * Errors:
 *   - Mutex or condition variable initialization failures.
 *   - Thread creation failure.
 */
int set_worker(struct logfs *logfs) {
    /* Allocate memory for the worker structure */
    if (!(logfs->worker = malloc(sizeof(struct worker)))) {
        TRACE("Memory allocation failed for worker");
        return -1;
    }
    memset(logfs->worker, 0, sizeof(struct worker));

    /* Initialize mutex and condition variables */
    if (pthread_mutex_init(&logfs->worker->lock, NULL) != 0) {
        TRACE("Mutex initialization failed");
        free(logfs->worker);
        return -1;
    }
    if (pthread_cond_init(&logfs->worker->space_avail, NULL) != 0 ||
        pthread_cond_init(&logfs->worker->data_avail, NULL) != 0) {
        TRACE("Condition variable initialization failed");
        pthread_mutex_destroy(&logfs->worker->lock);
        free(logfs->worker);
        return -1;
    }

    /* Create the worker thread */
    if (pthread_create(&logfs->worker->thread, NULL, worker, logfs) != 0) {
        TRACE("Worker thread creation failed");
        pthread_cond_destroy(&logfs->worker->space_avail);
        pthread_cond_destroy(&logfs->worker->data_avail);
        pthread_mutex_destroy(&logfs->worker->lock);
        free(logfs->worker);
        return -1;
    }

    logfs->worker->done = 0;
    return 0;
}

/**
 * logfs_close
 * -----------------------------------------------------
 * Functionality:
 * Closes and cleans up a `logfs` instance.
 *
 * Input:
 *   - struct logfs *logfs: Pointer to the `logfs` structure.
 *
 * Output:
 *   - None.
 *
 * Errors:
 *   - Handles internal failures gracefully.
 *   - Ensures all resources are released even on partial failures.
 */
void logfs_close(struct logfs *logfs) {
    int i;
    if (!logfs) return;

    /* Flush the write buffer */
    flush(logfs);

    /* Clean up the worker thread */
    if (logfs->worker) {
        pthread_mutex_lock(&logfs->worker->lock);

        /* Signal the worker thread to finish */
        logfs->worker->done = 1;
        pthread_cond_signal(&logfs->worker->data_avail);

        pthread_mutex_unlock(&logfs->worker->lock);

        /* Wait for the worker thread to terminate */
        pthread_join(logfs->worker->thread, NULL);

        /* Destroy mutex and condition variables */
        pthread_mutex_destroy(&logfs->worker->lock);
        pthread_cond_destroy(&logfs->worker->space_avail);
        pthread_cond_destroy(&logfs->worker->data_avail);

        /* Free the worker structure */
        FREE(logfs->worker);
    }

    /* Free the write buffer */
    if (logfs->w_buffer) {
        FREE(logfs->w_buffer->buffer_);
        FREE(logfs->w_buffer);
    }

    /* Free the cache */
    for (i = 0; i < RCACHE_BLOCKS; i++) {
        if (logfs->cache[i].data_) {
            FREE(logfs->cache[i].data_);
        }
    }

    /* Close the device */
    if (logfs->device) {
        device_close(logfs->device);
    }

    /* Free the logfs structure */
    FREE(logfs);
}





