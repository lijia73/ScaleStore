#ifndef DDCKV_MM_TEST_H_
#define DDCKV_MM_TEST_H_

#include <stdint.h>
#include <pthread.h>

#include "client_fmm.h"

typedef struct TagMMRunClientArgs
{
    int thread_id;
    int main_core_id;
    int poll_core_id;
    char *workload_name;
    char *config_file;
    pthread_barrier_t *alloc_start_barrier;
    pthread_barrier_t *alloc_finish_barrier;
    pthread_barrier_t *free_start_barrier;
    pthread_barrier_t *free_finish_barrier;
    volatile bool *should_stop;
    // bool * timer_is_ready;
    pthread_barrier_t *timer_barrier;

    uint32_t ret_num_alloc_ops;
    uint32_t ret_num_free_ops;
    uint32_t ret_fail_alloc_num;
    uint32_t ret_fail_free_num;

    uint32_t client_id;
    uint32_t num_threads;
    char *op_type;
    ClientFMM *client;
} MMRunClientArgs;

void *run_client(void *_args);

#endif