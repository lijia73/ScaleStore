#include <stdio.h>
#include <stdlib.h>

#include <stdio.h>
#include <stdlib.h>

#include <atomic>

#include "client.h"
#include "micro_test.h"

static void start_client_threads(char * op_type, int num_clients, GlobalConfig * config, 
        char * config_fname) {
    MicroRunClientArgs * client_args_list = (MicroRunClientArgs *)malloc(sizeof(MicroRunClientArgs) * num_clients);
    pthread_barrier_t alloc_start_barrier;
    pthread_barrier_t alloc_finish_barrier;
    pthread_barrier_t free_start_barrier;
    pthread_barrier_t free_finish_barrier;
    pthread_barrier_t global_timer_barrier;
    pthread_barrier_init(&alloc_start_barrier, NULL, num_clients);
    pthread_barrier_init(&alloc_finish_barrier, NULL, num_clients);
    pthread_barrier_init(&free_start_barrier, NULL, num_clients);
    pthread_barrier_init(&free_finish_barrier, NULL, num_clients);
    pthread_barrier_init(&global_timer_barrier, NULL, num_clients);
    volatile bool should_stop = false;

    pthread_t tid_list[num_clients];
    for (int i = 0; i < num_clients; i ++) {
        client_args_list[i].client_id    = config->server_id - config->memory_num;
        client_args_list[i].thread_id    = i;
        client_args_list[i].num_threads  = num_clients;
        client_args_list[i].main_core_id = config->main_core_id + i * 2;
        client_args_list[i].poll_core_id = config->poll_core_id + i * 2;
        client_args_list[i].config_file   = config_fname;
        client_args_list[i].alloc_start_barrier= &alloc_start_barrier;
        client_args_list[i].alloc_finish_barrier= &alloc_finish_barrier;
        client_args_list[i].free_start_barrier= &free_start_barrier;
        client_args_list[i].free_finish_barrier= &free_finish_barrier;
        client_args_list[i].timer_barrier = &global_timer_barrier;
        client_args_list[i].should_stop   = &should_stop;
        client_args_list[i].ret_num_alloc_ops = 0;
        client_args_list[i].ret_num_free_ops = 0;
        client_args_list[i].ret_fail_alloc_num = 0;
        client_args_list[i].ret_fail_free_num = 0;
        client_args_list[i].op_type = op_type;
        pthread_t tid;
        pthread_create(&tid, NULL, run_client, &client_args_list[i]);
        tid_list[i] = tid;
    }

    uint32_t total_alloc_tpt = 0;
    uint32_t total_alloc_failed = 0;
    uint32_t total_free_tpt = 0;
    uint32_t total_free_failed = 0;
    for (int i = 0; i < num_clients; i ++) {
        pthread_join(tid_list[i], NULL);
        total_alloc_tpt += client_args_list[i].ret_num_alloc_ops;
        total_free_tpt += client_args_list[i].ret_num_free_ops;
        total_alloc_failed += client_args_list[i].ret_fail_alloc_num;
        total_free_failed += client_args_list[i].ret_fail_free_num;
    }
    printf("alloc total: %d ops\n", total_alloc_tpt);
    printf("alloc failed: %d ops\n", total_alloc_failed);
    printf("alloc tpt: %d ops/s\n", (total_alloc_tpt - total_alloc_failed) * 1000 / 500);
    printf("free total: %d ops\n", total_delete_tpt);
    printf("free failed: %d ops\n", total_delete_failed);
    printf("free tpt: %d ops/s\n", (total_delete_tpt - total_delete_failed) * 1000 / 500);
    free(client_args_list);
}

int main(int argc, char ** argv) {
    if (argc != 3) {
        printf("Usage: %s path-to-config-file num-clients\n", argv[0]);
        return 1;
    }

    int num_clients = atoi(argv[2]);

    GlobalConfig config;
    int ret = load_config(argv[1], &config);
    assert(ret == 0);

    start_client_threads("INSERT", num_clients, &config, argv[1]);   
}