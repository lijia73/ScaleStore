#include <stdio.h>

#include "client.h"
#include "ycsb_test.h"

int main(int argc, char ** argv) {
    if (argc != 3) {
        printf("Usage: %s path-to-config-file workload-name\n", argv[0]);
        return 1;
    }

    int ret = 0;
    WorkloadFileName * workload_fnames = get_workload_fname(argv[2]);

    GlobalConfig config;
    ret = load_config(argv[1], &config);
    assert(ret == 0);

    // bind this process to main core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    ret = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = sched_getaffinity(0, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("main process running on core: %d\n", i);
        }
    }

    Client client(&config);

    // start polling thread
    pthread_t polling_tid = client.start_polling_thread();
    
    // 1. load workload load
    ret = load_workload_1coro(client, workload_fnames);
    assert(ret == 0);

    std::vector<uint8_t> crash_server_list;
    crash_server_list.push_back(2);
    client.crash_server(crash_server_list);

    std::vector<uint8_t> server_id_list;
    // init all server table
    for (int i = 0; i < 1; i ++) {
        server_id_list.push_back(i + config.memory_num);
    }
    client.get_addr_translate_table(server_id_list);
    // 2. load test workload
    
    ret = load_test_cnt_ops_on_crash(client, workload_fnames);
    assert(ret == 0);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}