#include "client_fmm.h"

#include <assert.h>
#include <sys/mman.h>
#include <stdio.h>
#include <sys/time.h>

// #include <boost/fiber/all.hpp>

#include <vector>
#include <fstream>

#include "kv_debug.h"

#define WRITE_KV_ST_WRID 200

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

    // printf("num_idx_rep: %d\n", num_idx_rep_);

    num_coroutines_ = conf->num_coroutines;
    num_total_operations_ = 0;
    num_local_operations_ = 0;
    kv_info_list_ = NULL;
    mm_req_ctx_list_ = NULL;

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

    // create mm
    mm_ = new ClientMM(conf, nm_);

    // alloc mr
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    size_t local_buf_sz = (size_t)CORO_LOCAL_BUF_LEN * num_coroutines_;
    printf("allocating %ld\n", local_buf_sz);
    local_buf_ = mmap(NULL, local_buf_sz, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    // assert(local_buf_ != MAP_FAILED);
    local_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, local_buf_, local_buf_sz,
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // print_log(DEBUG, "register mr addr(0x%lx) rkey(%x)", local_buf_mr_->addr, local_buf_mr_->rkey);

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
    mm_->get_log_head(&pr_log_server_id_, &pr_log_head_);
    pr_log_tail_ = pr_log_head_;
    ret = write_client_meta_info();
    // assert(ret == 0);
}

ClientFMM::~ClientFMM()
{
    delete nm_;
    delete mm_;
}

int ClientFMM::connect_ib_qps()
{
    uint32_t num_servers = nm_->get_num_servers();
    int ret = 0;
    for (int i = 0; i < num_servers; i++)
    {
        struct MrInfo *gc_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
        ret = nm_->client_connect_one_rc_qp(i, gc_info);
        // assert(ret == 0);
        server_mr_info_map_[i] = gc_info;
        // print_log(DEBUG, "connect to server(%d) addr(%lx) rkey(%x)", i, server_mr_info_map_[i]->addr, server_mr_info_map_[i]->rkey);
    }
    return 0;
}

int ClientFMM::write_client_meta_info()
{
    ClientLogMetaInfo meta_info;
    int ret;
    meta_info.pr_server_id = pr_log_server_id_;
    meta_info.pr_log_head = pr_log_head_;
    meta_info.pr_log_tail = pr_log_tail_;

    for (int i = 0; i < num_replication_; i++)
    {
        struct MrInfo *cur_mr_info = server_mr_info_map_[i];
        // print_log(DEBUG, "write meta info to server(%d) addr(0x%lx) rkey(%x) len(%d)", i, cur_mr_info->addr, cur_mr_info->rkey, sizeof(ClientLogMetaInfo));
        ret = nm_->nm_rdma_write_inl_to_sid(&meta_info, sizeof(ClientLogMetaInfo), remote_meta_addr_, cur_mr_info->rkey, i);
        // assert(ret == 0);
    }

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

void ClientFMM::update_kv_header(KVLogHeader *header, ClientMMAllocCtx *mm_alloc_ctx)
{
    header->next_addr = mm_alloc_ctx->next_addr_list[0];
    header->prev_addr = mm_alloc_ctx->prev_addr_list[0];
}

int ClientFMM::alloc_baseline(MMReqCtx *ctx)
{
    int ret = 0;
    KVLogHeader *header = (KVLogHeader *)ctx->kv_info->l_addr;
    // 1. allocate remote memory
    uint32_t alloc_size = ctx->size_ + sizeof(KVLogHeader);
    mm_->mm_alloc_baseline(alloc_size, nm_, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_)
    {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = MM_OPS_FAIL_RETURN;
        return ctx->ret_val.ret_code;
    }

    // 2. update kv header
    update_kv_header(header, &ctx->mm_alloc_ctx);

    // 3. generate write header send requests
    uint32_t write_kv_sr_list_num;
    IbvSrList *write_kv_sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &write_kv_sr_list_num);

    // 4. post requests and wait for completion
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_lists_sync(write_kv_sr_list, write_kv_sr_list_num, &wc);
    free_write_kv_sr_lists(write_kv_sr_list);

    ctx->is_finished = true;
    return ctx->ret_val.ret_code;
}

IbvSrList *ClientFMM::gen_write_kv_sr_lists(uint32_t coro_id, KVInfo *a_kv_info, ClientMMAllocCtx *r_mm_info,
                                            __OUT uint32_t *num_sr_lists)
{
    IbvSrList *ret_sr_list = (IbvSrList *)malloc(sizeof(IbvSrList) * num_replication_);
    struct ibv_send_wr *sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * num_replication_);
    struct ibv_sge *sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * num_replication_);
    memset(sr, 0, sizeof(struct ibv_send_wr) * num_replication_);
    memset(sge, 0, sizeof(struct ibv_sge) * num_replication_);

    for (int i = 0; i < num_replication_; i++)
    {
        sge[i].addr = (uint64_t)a_kv_info->l_addr;
        // sge[i].length = r_mm_info->num_subblocks * mm_->subblock_sz_;
        sge[i].length = r_mm_info->size_;
        sge[i].lkey = a_kv_info->lkey;

        sr[i].wr_id = ib_gen_wr_id(coro_id, r_mm_info->server_id_list[i], WRITE_KV_ST_WRID, i + 1);
        sr[i].sg_list = &sge[i];
        sr[i].num_sge = 1;
        sr[i].opcode = IBV_WR_RDMA_WRITE;
        sr[i].wr.rdma.remote_addr = r_mm_info->addr_list[i];
        sr[i].wr.rdma.rkey = r_mm_info->rkey_list[i];
        sr[i].next = NULL;

        ret_sr_list[i].sr_list = &sr[i];
        ret_sr_list[i].num_sr = 1;
        ret_sr_list[i].server_id = r_mm_info->server_id_list[i];

        // print_log(DEBUG, "\t  [%s] write kv to server(%d) addr(%lx) rkey(%x)", __FUNCTION__,
        //     ret_sr_list[i].server_id, sr[i].wr.rdma.remote_addr, sr[i].wr.rdma.rkey);
    }

    *num_sr_lists = num_replication_;
    return ret_sr_list;
}

void ClientFMM::free_write_kv_sr_lists(IbvSrList *sr_list)
{
    free(sr_list[0].sr_list[0].sg_list);
    free(sr_list[0].sr_list);
    free(sr_list);
}

int ClientFMM::load_seq_mm_requests(uint32_t num_ops, char *op_type)
{
    num_total_operations_ = num_ops;
    num_local_operations_ = num_ops;
    if (kv_info_list_ == NULL && mm_req_ctx_list_ == NULL)
    {

        kv_info_list_ = (KVInfo *)malloc(sizeof(KVInfo) * num_local_operations_);
        assert(kv_info_list_ != NULL);
        memset(kv_info_list_, 0, sizeof(KVInfo) * num_local_operations_);

        mm_req_ctx_list_ = new MMReqCtx[num_local_operations_];
        assert(mm_req_ctx_list_ != NULL);
    }

    uint64_t input_buf_ptr = (uint64_t)input_buf_;

    for (int i = 0; i < num_local_operations_; i++)
    {
        uint32_t all_len = mm_->mm_block_sz_;
        kv_info_list_[i].l_addr = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;

        KVLogHeader *kv_log_header = (KVLogHeader *)input_buf_ptr;
        if (strcmp(operation, "ALLOC_BASELINE") == 0)
        {
            kv_log_header->ctl_bits = KV_LOG_VALID;
        }
        else if (strcmp(operation, "FREE_BASELINE") == 0)
        {
            kv_log_header->ctl_bits = KV_LOG_GC;
        }
        input_buf_ptr += all_len;

        init_mm_req_ctx(&mm_req_ctx_list_[i], &kv_info_list_[i], op_type);
    }
}
void ClientFMM::init_mm_req_ctx(MMReqCtx *req_ctx, KVInfo *kv_info, char *operation)
{
    req_ctx->coro_id = 0;
    req_ctx->size_ = mm_->mm_block_sz_ - sizeof(KVLogHeader);

    req_ctx->kv_info = kv_info;

    if (strcmp(operation, "ALLOC_BASELINE") == 0)
    {
        req_ctx->req_type = MM_REQ_ALLOC_BASELINE;
    }
    else if (strcmp(operation, "FREE_BASELINE") == 0)
    {
        req_ctx->req_type = MM_REQ_FREE_BASELINE;
    }
    else if (strcmp(operation, "ALLOC_IMPROVEMENT") == 0)
    {
        req_ctx->req_type = MM_REQ_FREE_BASELINE;
    }
    else if (strcmp(operation, "FREE_IMPROVEMENT") == 0)
    {
        req_ctx->req_type = MM_REQ_FREE_BASELINE;
    }
}

int ClientFMM::get_num_rep()
{
    return num_replication_;
}

int ClientFMM::free_baseline(MMReqCtx *ctx)
{
    int ret = 0;
    KVLogHeader *header = (KVLogHeader *)ctx->kv_info->l_addr;

    header->ctl_bits = KV_LOG_GC;
    // 1. free remote memory
    uint32_t alloc_size = ctx->size_ + sizeof(KVLogHeader);
    ret = mm_->mm_free_baseline(nm_, &ctx->mm_alloc_ctx);
    if (ret != 0)
    {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = MM_OPS_FAIL_RETURN;
        return ctx->ret_val.ret_code;
    }

    // 2. generate write header send requests
    uint32_t write_kv_sr_list_num;
    IbvSrList *write_kv_sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &write_kv_sr_list_num);

    // 3. post requests and wait for completion
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_lists_sync(write_kv_sr_list, write_kv_sr_list_num, &wc);
    free_write_kv_sr_lists(write_kv_sr_list);

    ctx->is_finished = true;
    return ctx->ret_val.ret_code;
}