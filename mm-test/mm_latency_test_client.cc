#include <stdio.h>
#include <sched.h>

#include "mm_latency_test.h"

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        printf("Usage: %s path-to-config-file\n", argv[0]);
        return 1;
    }

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
    assert(ret == 0);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i++)
    {
        if (CPU_ISSET(i, &cpuset))
        {
            printf("main process running on core: %d\n", i);
        }
    }

    ClientFMM client(&config);

    ret = test_alloc_baseline_lat(client);
    assert(ret == 0);

    // ret = test_free_baseline_lat(client);
    // assert(ret == 0);
}