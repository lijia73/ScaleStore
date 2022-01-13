#include <stdio.h>
#include <sched.h>

#include "ycsb_test.h"

int main(int argc, char ** argv) {
    if (argc != 3) {
        printf("Usage: %s path-to-config-file workload-filename\n", argv[0]);
        return 1;
    }

    WorkloadFileName * workload_fnames = get_workload_fname(argv[2]);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(argv[1], &config);
    assert(ret == 0);

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
    pthread_t polling_tid = client.start_polling_thread();

    ret = load_workload(client, workload_fnames);
    assert(ret == 0);
    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);

    char out_buf[256];
    sprintf(out_buf, "results/%s.txt", argv[2]);
    ret = load_test_lat(client, workload_fnames, out_buf);
    assert(ret == 0);
}