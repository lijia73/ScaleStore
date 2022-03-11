#include "client_fmm.h"

#include <assert.h>
#include <sys/mman.h>
#include <stdio.h>
#include <sys/time.h>

// #include <boost/fiber/all.hpp>

#include <vector>
#include <fstream>

#include "kv_debug.h"

#define READ_BUCKET_ST_WRID 100
#define WRITE_KV_ST_WRID 200
#define READ_KV_ST_WRID 300
#define CAS_ST_WRID 400
#define INVALID_ST_WRID 500
#define READ_CACHE_ST_WRID 600
#define WRITE_HB_ST_WRID 700
#define LOG_COMMIT_ST_WRID 800
#define UPDATE_PREV_ST_WRID 900
#define READ_ALL_BUCKET_ST_WRID 150

ClientFMM::ClientFMM(const struct GlobalConfig *conf)
{
    num_idx_rep_ = conf->num_idx_rep;
    num_replication_ = conf->num_replication;
    remote_global_meta_addr_ = conf->server_base_addr;
    remote_meta_addr_ = conf->server_base_addr + CLIENT_META_LEN * (conf->server_id - conf->memory_num + 1);
    remote_gc_addr_ = conf->server_base_addr + META_AREA_LEN + CLIENT_GC_LEN * (conf->server_id - conf->memory_num + 1);
    remote_root_addr_ = conf->server_base_addr + META_AREA_LEN + GC_AREA_LEN;
    my_server_id_ = conf->server_id;
    num_memory_ = conf->memory_num;
    workload_run_time_ = conf->workload_run_time;

    printf("num_idx_rep: %d\n", num_idx_rep_);

    num_coroutines_ = conf->num_coroutines;
    num_total_operations_ = 0;
    num_local_operations_ = 0;
    kv_info_list_ = NULL;
    kv_req_ctx_list_ = NULL;

    // bind core information
    main_core_id_ = conf->main_core_id;
    poll_core_id_ = conf->poll_core_id;
    bg_core_id_ = conf->bg_core_id;
    gc_core_id_ = conf->gc_core_id;

    miss_rate_threash_ = conf->miss_rate_threash;

    server_st_addr_ = conf->server_base_addr;
    server_data_len_ = conf->server_data_len;
    micro_workload_num_ = conf->micro_workload_num;

    // create cm
    nm_ = new UDPNetworkManager(conf);

    int ret = connect_ib_qps();
    // assert(ret == 0);

    // create mm
    mm_ = new ClientMM(conf, nm_);

    // alloc mr
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    local_buf_ = mmap(NULL, CORO_LOCAL_BUF_LEN * num_coroutines_, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    // assert(local_buf_ != MAP_FAILED);
    local_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, local_buf_, CORO_LOCAL_BUF_LEN * num_coroutines_,
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // print_log(DEBUG, "register mr addr(0x%lx) rkey(%x)", local_buf_mr_->addr, local_buf_mr_->rkey);

    race_root_ = (RaceHashRoot *)malloc(sizeof(RaceHashRoot));
    // assert(race_root_ != NULL);
    race_root_mr_ = ibv_reg_mr(ib_info.ib_pd, race_root_, sizeof(RaceHashRoot),
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // assert(race_root_mr_ != NULL);
    // print_log(DEBUG, "register mr addr(0x%lx) rkey(%x)", race_root_mr_->addr, race_root_mr_->rkey);

    input_buf_ = malloc(CLINET_INPUT_BUF_LEN);
    // assert(input_buf_ != NULL);
    input_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, input_buf_, CLINET_INPUT_BUF_LEN,
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // assert(input_buf_mr_ != NULL);

    coro_local_addr_list_ = (uint64_t *)malloc(sizeof(uint64_t) * num_coroutines_);
    for (int i = 0; i < num_coroutines_; i++)
    {
        coro_local_addr_list_[i] = (uint64_t)local_buf_ + CORO_LOCAL_BUF_LEN * i;
    }

    // record meta info
    if (conf->is_recovery == false)
    {
        mm_->get_log_head(&pr_log_server_id_, &pr_log_head_);
        pr_log_tail_ = pr_log_head_;
        ret = write_client_meta_info();
        // assert(ret == 0);

        // get root
        ret = get_race_root();
        // kv_assert(ret == 0);

        if (my_server_id_ - conf->memory_num == 0)
        {
            // init table
            ret = init_hash_table();
            // kv_assert(ret == 0);

            ret = sync_init_finish();
            // kv_assert(ret == 0);

            ret = get_race_root();
            // kv_assert(ret == 0);
        }
        else
        {
            while (!init_is_finished())
                ;
            ret = get_race_root();
            // kv_assert(ret == 0);
        }
    }
    else
    {
        ret = get_race_root();
        // kv_assert(ret == 0);

        ret = client_recovery();
        // assert(ret == 0);
    }
}

ClientFMM::~ClientFMM()
{
    delete nm_;
    delete mm_;
}

int ClientFMM::init_hash_table()
{
    // initialize remote subtable entry after getting root information
    for (int i = 0; i < RACE_HASH_INIT_SUBTABLE_NUM; i++)
    {
        for (int j = 0; j < RACE_HASH_SUBTABLE_NUM / RACE_HASH_INIT_SUBTABLE_NUM; j++)
        {
            uint64_t subtable_idx = j * RACE_HASH_INIT_SUBTABLE_NUM + i;
            ClientMMAllocSubtableCtx subtable_info[num_idx_rep_];
            mm_->mm_alloc_subtable(nm_, subtable_info);
            for (int r = 0; r < num_idx_rep_; r++)
            {
                // print_log(DEBUG, "[%s] subtable(%lx) on server(%d)", __FUNCTION__, subtable_info[r].addr, subtable_info[r].server_id);
                race_root_->subtable_entry[subtable_idx][r].lock = 0;
                race_root_->subtable_entry[subtable_idx][r].local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
                race_root_->subtable_entry[subtable_idx][r].server_id = subtable_info[r].server_id;
                // assert((subtable_info[r].addr & 0xFF) == 0);
                HashIndexConvert64To40Bits(subtable_info[r].addr, race_root_->subtable_entry[subtable_idx][r].pointer);
            }
        }
    }

    // write root information back to all replicas
    int ret = write_race_root();
    // assert(ret == 0);
    return 0;
}

int ClientFMM::sync_init_finish()
{
    uint64_t local_msg = 1;
    int ret = 0;
    for (int i = num_replication_ - 1; i >= 0; i--)
    {
        ret = nm_->nm_rdma_write_inl_to_sid(&local_msg, sizeof(uint64_t),
                                            remote_global_meta_addr_, server_mr_info_map_[i]->rkey, i);
        // assert(ret == 0);
    }
    return 0;
}

bool ClientFMM::init_is_finished()
{
    int ret = 0;
    ret = nm_->nm_rdma_read_from_sid(local_buf_, local_buf_mr_->lkey,
                                     sizeof(uint64_t), remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    // assert(ret == 0);
    uint64_t read_value = *(uint64_t *)local_buf_;
    if (read_value == 1)
    {
        return true;
    }
    return false;
}

int ClientFMM::alloc_baseline(MMReqCtx *ctx)
{
    uint32_t alloc_size = size + sizeof(KVLogHeader);
    mm_->mm_alloc_baseline(alloc_size, nm_, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_)
    {
        ctx->is_finished = true;
        ctx->ret_code = MM_OPS_FAIL_RETURN;
        return ctx->ret_code;
    }

    ctx->is_finished = true;
    return ctx->ret_code
}
