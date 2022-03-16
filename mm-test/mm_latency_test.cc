#include <stdio.h>
#include <sys/time.h>

#include "client.h"

#include "mm_latency_test.h"

#define WORKLOAD_ALL (-1)
// #define WORKLOAD_NUM WORKLOAD_ALL
#define WORKLOAD_NUM 100000

static int test_lat(ClientFMM &client, char *op_type, const char *out_fname)
{
    int ret = 0;
    ret = client.load_seq_mm_requests(WORKLOAD_NUM, op_type);
    assert(ret == 0);

    printf("lat test %s\n", op_type);
    uint64_t *lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    struct timeval st, et;
    for (int i = 0; i < client.num_local_operations_; i++)
    {
        MMReqCtx *ctx = &client.mm_req_ctx_list_[i];
        ctx->coro_id = 0;

        switch (ctx->req_type)
        {
        case MM_REQ_BASELINE:
            gettimeofday(&st, NULL);
            ret = client.alloc_baseline(ctx);
            gettimeofday(&et, NULL);
            if (ret == MM_OPS_FAIL_RETURN)
            {
                num_failed++;
            }
            break;
        default:
            assert(0);
            break;
        }

        lat_list[i] = (et.tv_sec - st.tv_sec) * 1000000 + (et.tv_usec - st.tv_usec);
    }
    printf("Failed: %d\n", num_failed);

    FILE *lat_fp = fopen(out_fname, "w");
    assert(lat_fp != NULL);
    for (int i = 0; i < client.num_local_operations_; i++)
    {
        fprintf(lat_fp, "%ld\n", lat_list[i]);
    }
    fclose(lat_fp);
    return 0;
}

int test_alloc_baseline_lat(ClientFMM &client)
{
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/alloc_baseline_lat-%drp.txt", num_rep);
    return test_lat(client, "BASELINE", out_fname);
}