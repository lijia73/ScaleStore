#include "client_mm.h"

#include <unistd.h>
#include <assert.h>
#include <sys/time.h>

#include "kv_debug.h"
#include "hashtable.h"

#define REC_SPACE_SIZE (64 * 1024 * 1024)

ClientMM::ClientMM(const struct GlobalConfig *conf, UDPNetworkManager *nm)
{
    int ret = 0;
    client_meta_addr_ = conf->server_base_addr + CLIENT_META_LEN * (conf->server_id - conf->memory_num + 1) + sizeof(ClientLogMetaInfo);
    client_gc_addr_ = conf->server_base_addr + META_AREA_LEN + CLIENT_GC_LEN * (conf->server_id - conf->memory_num + 1);

    num_replication_ = conf->num_replication;
    num_idx_rep_ = conf->num_idx_rep;
    num_memory_ = conf->memory_num;

    last_allocated_ = conf->server_id;
    mm_block_sz_ = conf->block_size;
    subblock_sz_ = conf->subblock_size;
    subblock_num_ = mm_block_sz_ / subblock_sz_;

    // n_dyn_req_ = 0;

    mm_blocks_lock_ = 0;
    cur_mm_block_idx_ = 0;

    alloc_new_block_lock_.unlock();
    is_allocing_new_block_ = false;

    if (conf->is_recovery == false)
    {
        // allocate initial blocks
        alloc_new_block_lock_.lock();
        is_allocing_new_block_ = true;
        // int ret;
        // if (conf->server_id - conf->memory_num == 0)
        // {
        //     ret = init_get_new_block_from_server(nm);
        // }
        // else
        // {
        //     ret = get_new_block_from_server(nm);
        // }
        int ret = get_new_block_from_server(nm);
        // int ret = init_get_new_block_from_server(nm);
        alloc_new_block_lock_.unlock();
        // assert(ret == 0);

        // assert(mm_blocks_.size() == 1);
        cur_mm_block_idx_ = 0;

        pr_log_server_id_ = mm_blocks_[cur_mm_block_idx_]->server_id_list[0];
        pr_log_head_ = mm_blocks_[cur_mm_block_idx_]->mr_info_list[0].addr;
    }
    else
    {
        // start recovery
        // printf("!!!!!\n");
        ret = mm_recovery(nm);
        // assert(ret == 0);
    }
    // printf("end\n");
}

ClientMM::~ClientMM()
{
    for (size_t i = 0; i < mm_blocks_.size(); i++)
    {
        free(mm_blocks_[i]);
    }
    mm_blocks_.clear();
    // printf("!!!!!!!!n_dyn_req: %d\n", n_dyn_req_);
}

int32_t ClientMM::dyn_get_new_block_from_server(UDPNetworkManager *nm)
{
    // n_dyn_req_ ++;
    int ret = 0;
    uint32_t my_server_id = nm->get_server_id();
    uint32_t alloc_hint = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers = nm->get_num_servers();

    struct MrInfo mr_info_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
    for (int i = 0; i < num_replication_; i++)
    {
        uint32_t server_id = (pr_server_id + i) % num_servers;
        server_id_list[i] = server_id;
        ret = alloc_from_sid(server_id, nm, TYPE_KVBLOCK, &mr_info_list[i]);
        // assert(ret == 0);
    }

    if (mr_info_list[0].addr == 0)
    {
        return -1;
    }

    ret = dyn_reg_new_space(mr_info_list, server_id_list, nm, TYPE_KVBLOCK);
    // assert(ret == 0);
    return 0;
}

int32_t ClientMM::dyn_get_new_block_from_server_baseline(UDPNetworkManager *nm)
{
    // n_dyn_req_ ++;
    int ret = 0;
    uint32_t my_server_id = nm->get_server_id();
    uint32_t alloc_hint = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers = nm->get_num_servers();

    struct MrInfo mr_info_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
    for (int i = 0; i < num_replication_; i++)
    {
        uint32_t server_id = (pr_server_id + i) % num_servers;
        server_id_list[i] = server_id;
        ret = alloc_from_sid(server_id, nm, TYPE_KVBLOCK, &mr_info_list[i]);
        // assert(ret == 0);
    }

    if (mr_info_list[0].addr == 0)
    {
        return -1;
    }

    ret = dyn_reg_new_space(mr_info_list, server_id_list, nm, TYPE_BASELINE);
    // assert(ret == 0);
    return 0;
}

int ClientMM::get_new_block_from_server(UDPNetworkManager *nm)
{
    int ret = 0;
    uint32_t my_server_id = nm->get_server_id();
    uint32_t alloc_hint = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers = nm->get_num_servers();

    struct MrInfo mr_info_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
    for (int i = 0; i < num_replication_; i++)
    {
        uint32_t server_id = (pr_server_id + i) % num_servers;
        server_id_list[i] = server_id;
        ret = alloc_from_sid(server_id, nm, TYPE_KVBLOCK, &mr_info_list[i]);
        // assert(ret == 0);
    }

    ret = reg_new_space(mr_info_list, server_id_list, nm, TYPE_KVBLOCK);
    // assert(ret == 0);
    return 0;
}

int ClientMM::init_get_new_block_from_server(UDPNetworkManager *nm)
{
    int ret = 0;
    uint32_t my_server_id = nm->get_server_id();

    struct MrInfo mr_info_list[num_memory_][MAX_REP_NUM];
    uint8_t server_id_list[num_memory_][MAX_REP_NUM];

    for (int i = 0; i < num_memory_; i++)
    {
        uint32_t alloc_hint = get_alloc_hint_rr();
        uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
        for (int j = 0; j < num_replication_; j++)
        {
            uint32_t server_id = (pr_server_id + j) % num_memory_;
            server_id_list[i][j] = server_id;
            ret = alloc_from_sid(server_id, nm, TYPE_KVBLOCK, &mr_info_list[i][j]);
            // print_log(DEBUG, "allocated mmblock on %d %lx", server_id, mr_info_list[i][j].addr);
        }
    }

    ret = init_reg_space(mr_info_list, server_id_list, nm, TYPE_KVBLOCK);
    return 0;
}

void ClientMM::update_mm_block_next(ClientMMBlock *mm_block)
{
    int32_t max_idx = -1;
    int32_t max_cnt = -1;
    bool *mm_block_bmap = mm_block->bmap;

    for (int i = 1; i < subblock_num_;)
    {
        if (mm_block_bmap[i] == 1)
        {
            i++;
            continue;
        }

        int j;
        for (j = 0; i + j < subblock_num_; j++)
        {
            if (mm_block_bmap[i + j] == 1)
            {
                break;
            }
        }
        if (j > max_cnt)
        {
            max_cnt = j;
            max_idx = i;
        }
        i += j;
    }

    mm_block->prev_free_subblock_idx = mm_block->next_free_subblock_idx;
    mm_block->next_free_subblock_idx = max_idx;
    mm_block->next_free_subblock_cnt = max_cnt;
}

void ClientMM::mm_free_key(std::string key)
{
    std::queue<SubblockInfo> &subblock_list = allocated_subblock_key_map_[key];
    int qlen = subblock_list.size();
    for (int i = 0; i < qlen - 1; i++)
    {
        subblock_free_queue_.push(subblock_list.front());
        subblock_list.pop();
    }
}

void ClientMM::mm_free_key_all(std::string key)
{
    std::queue<SubblockInfo> &subblock_list = allocated_subblock_key_map_[key];
    int qlen = subblock_list.size();
    for (int i = 0; i < qlen; i++)
    {
        subblock_free_queue_.push(subblock_list.front());
        subblock_list.pop();
    }
}

void ClientMM::mm_alloc(size_t size, UDPNetworkManager *nm, std::string key, __OUT ClientMMAllocCtx *ctx)
{
    mm_alloc(size, nm, ctx);
    allocated_subblock_key_map_[key].push(last_allocated_info_);
}

void ClientMM::mm_alloc(size_t size, UDPNetworkManager *nm, __OUT ClientMMAllocCtx *ctx)
{
    // struct timeval st, et;
    // gettimeofday(&st, NULL);
    int ret = 0;
    size_t aligned_size = get_aligned_size(size);
    int num_subblocks_required = aligned_size / subblock_sz_;
    assert(num_subblocks_required == 1);

    assert(subblock_free_queue_.size() > 0);
    SubblockInfo alloc_subblock = subblock_free_queue_.front();
    subblock_free_queue_.pop();

    if (subblock_free_queue_.size() == 0)
    {
        ret = dyn_get_new_block_from_server(nm);
        if (ret == -1)
        {
            ctx->addr_list[0] = 0;
            return;
        }
    }

    SubblockInfo next_subblock = subblock_free_queue_.front();

    ctx->need_change_prev = false;
    ctx->num_subblocks = num_subblocks_required;
    for (int i = 0; i < num_replication_; i++)
    {
        ctx->addr_list[i] = alloc_subblock.addr_list[i];
        ctx->rkey_list[i] = alloc_subblock.rkey_list[i];
        ctx->server_id_list[i] = alloc_subblock.server_id_list[i];

        ctx->next_addr_list[i] = next_subblock.addr_list[i];
        ctx->next_addr_list[i] |= next_subblock.server_id_list[i];

        ctx->prev_addr_list[i] = last_allocated_info_.addr_list[i];
        ctx->prev_addr_list[i] |= last_allocated_info_.server_id_list[i];
        // print_log(DEBUG, "\t   [%s] allocating %lx on server(%d)", __FUNCTION__,
        //     ctx->addr_list[i], ctx->server_id_list[i]);
    }

    last_allocated_info_ = alloc_subblock;
}

void ClientMM::mm_alloc_log_info(RecoverLogInfo *log_info, __OUT ClientMMAllocCtx *ctx)
{
    uint32_t size = log_info->local_header_addr->key_length + log_info->local_header_addr->value_length + sizeof(KVLogHeader);
    int num_subblock_required = get_aligned_size(size) / subblock_sz_;
    uint64_t pr_addr = log_info->remote_addr;
    uint8_t server_id = log_info->server_id;

    for (int i = 0; i < num_replication_; i++)
    {
        ctx->addr_list[i] = last_allocated_info_.addr_list[i];
        ctx->rkey_list[i] = last_allocated_info_.rkey_list[i];
        ctx->server_id_list[i] = last_allocated_info_.server_id_list[i];
    }
    ctx->num_subblocks = num_subblock_required;

    KVLogHeader *cur_log_header = log_info->local_header_addr;
    ctx->prev_addr_list[0] = cur_log_header->prev_addr;
}

void ClientMM::mm_alloc_subtable(UDPNetworkManager *nm, __OUT ClientMMAllocSubtableCtx *ctx)
{
    uint32_t my_server_id = nm->get_server_id();
    uint32_t alloc_hint = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers = nm->get_num_servers();
    int ret = 0;

    struct MrInfo mr_info_list[num_idx_rep_];
    uint8_t server_id_list[num_idx_rep_];
    for (int i = 0; i < num_idx_rep_; i++)
    {
        uint32_t server_id = (pr_server_id + i) % num_servers;
        server_id_list[i] = server_id;
        ret = alloc_from_sid(server_id, nm, TYPE_SUBTABLE, &mr_info_list[i]);
        // assert(ret == 0);
        ctx[i].addr = mr_info_list[i].addr;
        ctx[i].server_id = server_id;
        // print_log(DEBUG, "allocating subtable on %d %lx", server_id, mr_info_list[i].addr);
    }

    ret = reg_new_space(mr_info_list, server_id_list, nm, TYPE_SUBTABLE);
    // assert(ret == 0);
}

int ClientMM::get_last_log_recover_info(__OUT RecoverLogInfo *recover_log_info)
{
    size_t last_idx = recover_log_info_list_.size();
    if (last_idx > 1)
    {
        memcpy(recover_log_info, &recover_log_info_list_[last_idx - 2], sizeof(RecoverLogInfo));
    }
    else
    {
        memset(recover_log_info, 0, sizeof(RecoverLogInfo));
    }
    return 0;
}

void ClientMM::free_recover_buf()
{
    ibv_dereg_mr(recover_mr_);
    recover_log_info_list_.clear();
    free(recover_buf_);
}

int ClientMM::alloc_from_sid(uint32_t server_id, UDPNetworkManager *nm, int alloc_type,
                             __OUT struct MrInfo *mr_info)
{
    struct KVMsg request, reply;
    memset(&request, 0, sizeof(struct KVMsg));
    memset(&reply, 0, sizeof(struct KVMsg));

    request.id = nm->get_server_id();
    if (alloc_type == TYPE_KVBLOCK || alloc_type == TYPE_BASELINE)
    {
        request.type = REQ_ALLOC;
    }
    else
    {
        // assert(alloc_type == TYPE_SUBTABLE);
        request.type = REQ_ALLOC_SUBTABLE;
    }
    serialize_kvmsg(&request);

    int ret = nm->nm_send_udp_msg_to_server(&request, server_id);
    // assert(ret == 0);
    ret = nm->nm_recv_udp_msg(&reply, NULL, NULL);
    // assert(ret == 0);
    deserialize_kvmsg(&reply);

    // if (alloc_type == TYPE_KVBLOCK) {
    //     // assert(reply.type == REP_ALLOC);
    // } else {
    //     // assert(reply.type == REP_ALLOC_SUBTABLE);
    // }

    memcpy(mr_info, &reply.body.mr_info, sizeof(struct MrInfo));
    return 0;
}

int ClientMM::local_reg_subblocks(const struct MrInfo *mr_info_list, const uint8_t *server_id_list)
{
    ClientMMBlock *new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
    memset(new_mm_block, 0, sizeof(ClientMMBlock));

    for (int i = 0; i < num_replication_; i++)
    {
        memcpy(&new_mm_block->mr_info_list[i], &mr_info_list[i], sizeof(struct MrInfo));
        new_mm_block->server_id_list[i] = server_id_list[i];
    }

    // add subblocks to subblock list
    // printf("subblock_num: %d\n", subblock_num_);
    // getchar();
    for (int i = 0; i < subblock_num_; i++)
    {
        SubblockInfo tmp_info;
        for (int r = 0; r < num_replication_; r++)
        {
            tmp_info.addr_list[r] = new_mm_block->mr_info_list[r].addr + i * subblock_sz_;
            // assert((tmp_info.addr_list[r] & 0xFF) == 0);
            tmp_info.rkey_list[r] = new_mm_block->mr_info_list[r].rkey;
            tmp_info.server_id_list[r] = new_mm_block->server_id_list[r];
        }
        subblock_free_queue_.push(tmp_info);
    }

    mm_blocks_.push_back(new_mm_block);

    return 0;
}

int ClientMM::local_reg_blocks(const struct MrInfo *mr_info_list, const uint8_t *server_id_list)
{
    ClientMMBlock *new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
    memset(new_mm_block, 0, sizeof(ClientMMBlock));

    for (int i = 0; i < num_replication_; i++)
    {
        memcpy(&new_mm_block->mr_info_list[i], &mr_info_list[i], sizeof(struct MrInfo));
        new_mm_block->server_id_list[i] = server_id_list[i];
    }

    mm_blocks_.push_back(new_mm_block);

    return 0;
}

int ClientMM::reg_new_space(const struct MrInfo *mr_info_list, const uint8_t *server_id_list,
                            UDPNetworkManager *nm, int alloc_type)
{
    int ret = 0;
    ClientMetaAddrInfo meta_info;
    if (alloc_type == TYPE_KVBLOCK)
    {
        meta_info.meta_info_type = TYPE_KVBLOCK;
    }
    else
    {
        assert(alloc_type == TYPE_SUBTABLE);
        meta_info.meta_info_type = TYPE_SUBTABLE;
    }

    // prepare meta info
    // print_log(DEBUG, "[%s] prepare meta info", __FUNCTION__);
    for (int i = 0; i < num_replication_; i++)
    {
        meta_info.server_id_list[i] = server_id_list[i];
        meta_info.addr_list[i] = mr_info_list[i].addr;
    }

    // send meta info to remote
    // print_log(DEBUG, "[%s] send meta info to remote", __FUNCTION__);
    for (int i = 0; i < num_replication_; i++)
    {
        uint32_t rkey = nm->get_server_rkey(i);
        // print_log(DEBUG, "[%s] writing to server(%d) addr(%lx) rkey(%x)",
        //     __FUNCTION__, i, client_meta_addr_, rkey);
        ret = nm->nm_rdma_write_inl_to_sid(&meta_info, sizeof(ClientMetaAddrInfo),
                                           client_meta_addr_, rkey, i);
        // assert(ret == 0);
    }
    client_meta_addr_ += sizeof(ClientMetaAddrInfo);

    // locally register subblocks
    if (alloc_type == TYPE_KVBLOCK)
    {
        // print_log(DEBUG, "[%s] register locally", __FUNCTION__);
        ret = local_reg_subblocks(mr_info_list, server_id_list);
        // assert(ret == 0);
    }

    // locally register blocks
    if (alloc_type == TYPE_BASELINE)
    {
        // print_log(DEBUG, "[%s] register locally", __FUNCTION__);
        ret = local_reg_blocks(mr_info_list, server_id_list);
        // assert(ret == 0);
    }
    return 0;
}

int ClientMM::init_reg_space(struct MrInfo mr_info_list[][MAX_REP_NUM], uint8_t server_id_list[][MAX_REP_NUM],
                             UDPNetworkManager *nm, int alloc_type)
{
    int ret = 0;
    assert(alloc_type == TYPE_KVBLOCK);

    std::queue<SubblockInfo> tmp_queue[num_memory_];
    for (int m = 0; m < num_memory_; m++)
    {
        ClientMetaAddrInfo meta_info;

        meta_info.meta_info_type = TYPE_KVBLOCK;
        for (int i = 0; i < num_replication_; i++)
        {
            meta_info.server_id_list[i] = server_id_list[m][i];
            meta_info.addr_list[i] = mr_info_list[m][i].addr;
        }

        for (int i = 0; i < num_replication_; i++)
        {
            uint32_t rkey = nm->get_server_rkey(i);
            // print_log(DEBUG, "[%s] write meta to server(%d) raddr(%lx) rkey(%x)", __FUNCTION__, i, client_meta_addr_, rkey);
            ret = nm->nm_rdma_write_inl_to_sid(&meta_info, sizeof(ClientMetaAddrInfo),
                                               client_meta_addr_, rkey, i);
            // assert(ret == 0);
        }
        client_meta_addr_ += sizeof(ClientMetaAddrInfo);

        ClientMMBlock *new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
        memset(new_mm_block, 0, sizeof(ClientMMBlock));
        for (int i = 0; i < num_replication_; i++)
        {
            memcpy(&new_mm_block->mr_info_list[i], &mr_info_list[m][i], sizeof(struct MrInfo));
            new_mm_block->server_id_list[i] = server_id_list[m][i];
        }

        for (int i = 0; i < subblock_num_; i++)
        {
            SubblockInfo tmp_info;
            for (int r = 0; r < num_replication_; r++)
            {
                tmp_info.addr_list[r] = new_mm_block->mr_info_list[r].addr + i * subblock_sz_;
                tmp_info.rkey_list[r] = new_mm_block->mr_info_list[r].rkey;
                tmp_info.server_id_list[r] = new_mm_block->server_id_list[r];
            }
            tmp_queue[m].push(tmp_info);
        }
        mm_blocks_.push_back(new_mm_block);
    }

    // merge queues
    for (int i = 0; i < num_memory_ * subblock_num_; i++)
    {
        int queue_id = i % num_memory_;
        SubblockInfo tmp_info = tmp_queue[queue_id].front();
        subblock_free_queue_.push(tmp_info);
        tmp_queue[queue_id].pop();
    }

    for (int i = 0; i < num_memory_; i++)
    {
        if (tmp_queue[i].size() > 0)
        {
            printf("!!!!err\n");
            exit(1);
        }
    }
    return 0;
}

int ClientMM::dyn_reg_new_space(const struct MrInfo *mr_info_list, const uint8_t *server_id_list,
                                UDPNetworkManager *nm, int alloc_type)
{
    int ret = 0;
    ClientMetaAddrInfo meta_info;
    if (alloc_type == TYPE_KVBLOCK || alloc_type == TYPE_BASELINE)
    {
        meta_info.meta_info_type = TYPE_KVBLOCK;
    }
    else
    {
        // assert(alloc_type == TYPE_SUBTABLE);
        meta_info.meta_info_type = TYPE_SUBTABLE;
    }

    // prepare meta info
    // print_log(DEBUG, "[%s] prepare meta info", __FUNCTION__);
    for (int i = 0; i < num_replication_; i++)
    {
        meta_info.server_id_list[i] = server_id_list[i];
        meta_info.addr_list[i] = mr_info_list[i].addr;
    }

    // send meta info to remote
    // print_log(DEBUG, "[%s] send meta info to remote", __FUNCTION__);
    for (int i = 0; i < num_replication_; i++)
    {
        uint32_t rkey = nm->get_server_rkey(i);
        // print_log(DEBUG, "[%s] writing to server(%d) addr(%lx) rkey(%x)", __FUNCTION__, i, client_meta_addr_, rkey);
        ret = nm->nm_rdma_write_inl_to_sid_sync(&meta_info, sizeof(ClientMetaAddrInfo), client_meta_addr_, rkey, i);
        // assert(ret == 0);
    }
    client_meta_addr_ += sizeof(ClientMetaAddrInfo);

    // locally register subblocks
    if (alloc_type == TYPE_KVBLOCK)
    {
        // print_log(DEBUG, "[%s] register locally", __FUNCTION__);
        ret = local_reg_subblocks(mr_info_list, server_id_list);
        // assert(ret == 0);
    }

    // locally register blocks
    if (alloc_type == TYPE_BASELINE)
    {
        // print_log(DEBUG, "[%s] register locally", __FUNCTION__);
        ret = local_reg_blocks(mr_info_list, server_id_list);
        // assert(ret == 0);
    }

    return 0;
}

int ClientMM::mm_recover_prepare_space(UDPNetworkManager *nm)
{
    int ret = 0;
    // print_log(DEBUG, "  [%s] 1. create recoer space and register mr", __FUNCTION__);
    IbInfo ib_info;
    nm->get_ib_info(&ib_info);
    recover_buf_ = malloc(REC_SPACE_SIZE);
    recover_mr_ = ibv_reg_mr(ib_info.ib_pd, recover_buf_, REC_SPACE_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // assert(recover_mr_ != NULL);
    log_header_st_ptr_ = (void *)((uint64_t)recover_buf_ + CLIENT_META_LEN);
    return 0;
}

int ClientMM::mm_traverse_log(UDPNetworkManager *nm)
{
    int ret = 0;

    // 1. traverse remote chained log
    // print_log(DEBUG, "  [%s] 3. traverse remote chained log", __FUNCTION__);
    ClientLogMetaInfo *log_meta_info = (ClientLogMetaInfo *)recover_buf_;

    uint64_t ptr_addr = log_meta_info->pr_log_head;
    uint64_t ptr_sid = log_meta_info->pr_server_id;
    int cnt = 0;
    KVLogHeader *log_header_ptr = (KVLogHeader *)log_header_st_ptr_;
    while (ptr_addr != 0)
    {
        // print_log(DEBUG, "  [%s]      server: %d addr: %lx", __FUNCTION__, ptr_sid, ptr_addr);
        uint32_t rkey = nm->get_server_rkey(ptr_sid);
        ret = nm->nm_rdma_read_from_sid(log_header_ptr + cnt, recover_mr_->lkey, sizeof(KVLogHeader),
                                        ptr_addr, rkey, ptr_sid);
        // assert(ret == 0);
        // print_log(DEBUG, "  [%s]      valid(%d) gc(%d) commit(%d)", __FUNCTION__,
        //     log_is_valid(log_header_ptr + cnt), log_is_gc(log_header_ptr + cnt), log_is_committed(log_header_ptr + cnt));

        // save the read log header info
        RecoverLogInfo recover_info;
        recover_info.local_header_addr = log_header_ptr + cnt;
        recover_info.remote_addr = ptr_addr;
        recover_info.server_id = ptr_sid;
        recover_log_info_list_.push_back(recover_info);

        // record the allocated information
        // printf("%lx\n", ptr_addr);
        assert((ptr_addr & 0xFF) == 0);
        uint64_t allocated_pr_addr = ptr_addr | ptr_sid;
        recover_addr_is_allocated_map_[allocated_pr_addr] = true;

        // set pointer to next log header
        KVLogHeader *cur_log_header = log_header_ptr + cnt;
        // print_log(DEBUG, "  [%s]      header read: next addr(%lx) press to continue", __FUNCTION__, cur_log_header->next_addr);
        // getchar();
        ptr_sid = cur_log_header->next_addr & 0xFF;
        ptr_addr = cur_log_header->next_addr ^ (uint64_t)ptr_sid;
        cnt++;
    }
    // printf("traversed: %d\n", cnt);
    return 0;
}

int ClientMM::mm_get_addr_meta(UDPNetworkManager *nm)
{
    // print_log(DEBUG, "  [%s] 2. read meta addr info", __FUNCTION__);
    int ret = 0;
    uint64_t r_meta_addr = client_meta_addr_ - sizeof(ClientLogMetaInfo);
    uint32_t rkey = nm->get_server_rkey(0);
    ret = nm->nm_rdma_read_from_sid(recover_buf_, recover_mr_->lkey, CLIENT_META_LEN,
                                    r_meta_addr, rkey, 0);
    // assert(ret == 0);
    return 0;
}

int ClientMM::mm_recover_mm_blocks(UDPNetworkManager *nm)
{
    // print_log(DEBUG, "  [%s] start", __FUNCTION__);
    ClientMetaAddrInfo *meta_addr_info_list = (ClientMetaAddrInfo *)((uint64_t)recover_buf_ + sizeof(ClientLogMetaInfo));
    uint32_t num_log_entries = recover_log_info_list_.size();

    cur_mm_block_idx_ = -1;

    // recover the last allocated subblock info and the queue head info
    uint64_t last_allocated_pr_addr;
    uint64_t queue_head_pr_addr = recover_log_info_list_[num_log_entries - 1].remote_addr | recover_log_info_list_[num_log_entries - 1].server_id;
    if (num_log_entries > 1)
    {
        last_allocated_pr_addr = recover_log_info_list_[num_log_entries - 2].remote_addr | recover_log_info_list_[num_log_entries - 2].server_id;
    }

    // construct all subblock info
    std::queue<SubblockInfo> all_subblock_queue[num_memory_];
    SubblockInfo queue_head_info;
    while (meta_addr_info_list->meta_info_type != 0)
    {
        if (meta_addr_info_list->meta_info_type == TYPE_SUBTABLE)
        {
            meta_addr_info_list++;
            continue;
        }

        // print_log(DEBUG, "[%s] recovering %lx", __FUNCTION__, meta_addr_info_list->addr_list[0]);

        // create a new mmblock
        ClientMMBlock *new_mmblock = get_new_mmblock();
        for (int i = 0; i < num_replication_; i++)
        {
            new_mmblock->mr_info_list[i].addr = meta_addr_info_list->addr_list[i];
            new_mmblock->mr_info_list[i].rkey = nm->get_server_rkey(meta_addr_info_list->server_id_list[i]);
            new_mmblock->server_id_list[i] = meta_addr_info_list->server_id_list[i];
        }

        uint64_t pr_subblock_st_addr = new_mmblock->mr_info_list[0].addr;
        for (uint32_t i = 0; i < subblock_num_; i++)
        {
            uint64_t cur_pr_addr = (pr_subblock_st_addr + subblock_sz_ * i) | new_mmblock->server_id_list[0];
            if (cur_pr_addr == queue_head_pr_addr)
            {
                gen_subblock_info(new_mmblock, i, &queue_head_info);
            }
            else if (cur_pr_addr == last_allocated_pr_addr)
            {
                gen_subblock_info(new_mmblock, i, &last_allocated_info_);
            }
            else if (recover_addr_is_allocated_map_[cur_pr_addr] == true)
            {
                // print_log(DEBUG, "[%s] %lx is allocated\n", __FUNCTION__, cur_pr_addr);
                continue;
            }
            else
            {
                SubblockInfo cur_subblock_info;
                gen_subblock_info(new_mmblock, i, &cur_subblock_info);
                all_subblock_queue[new_mmblock->server_id_list[0]].push(cur_subblock_info);
            }
        }

        mm_blocks_.push_back(new_mmblock);
        meta_addr_info_list++;
    }

    // construct local allocation queue
    subblock_free_queue_.push(queue_head_info);
    while (true)
    {
        // stop condition
        bool need_stop = true;
        for (int i = 0; i < num_memory_; i++)
        {
            if (all_subblock_queue[i].size() > 0)
            {
                subblock_free_queue_.push(all_subblock_queue[i].front());
                all_subblock_queue[i].pop();
                need_stop = false;
            }
        }
        if (need_stop)
        {
            break;
        }
    }

    return 0;
}

void ClientMM::gen_subblock_info(ClientMMBlock *mm_block, uint32_t subblock_idx,
                                 __OUT SubblockInfo *subblock_info)
{
    for (int i = 0; i < num_replication_; i++)
    {
        subblock_info->addr_list[i] = mm_block->mr_info_list[i].addr + subblock_idx * subblock_sz_;
        subblock_info->rkey_list[i] = mm_block->mr_info_list[i].rkey;
        subblock_info->server_id_list[i] = mm_block->server_id_list[i];
    }
}

int ClientMM::mm_recovery(UDPNetworkManager *nm)
{
    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    int ret = 0;

    // 0. prepare recover buf
    ret = mm_recover_prepare_space(nm);
    // assert(ret == 0);
    gettimeofday(&local_recover_space_et_, NULL);

    // 1. get addr meta info
    ret = mm_get_addr_meta(nm);
    // assert(ret == 0);
    gettimeofday(&get_addr_meta_et_, NULL);

    // 2. traverse log
    ret = mm_traverse_log(nm);
    // assert(ret == 0);
    gettimeofday(&traverse_log_et_, NULL);

    // 3. recover mm blocks
    ret = mm_recover_mm_blocks(nm);
    // assert(ret == 0);

    return 0;
}

ClientMMBlock *ClientMM::get_new_mmblock()
{
    ClientMMBlock *new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
    memset(new_mm_block, 0, sizeof(ClientMMBlock));

    return new_mm_block;
}

uint32_t ClientMM::get_subblock_idx(uint64_t addr, ClientMMBlock *mm_block)
{
    return (addr - mm_block->mr_info_list[0].addr) / subblock_sz_;
}

void ClientMM::get_time_bread_down(std::vector<struct timeval> &time_vec)
{
    time_vec.push_back(local_recover_space_et_);
    time_vec.push_back(get_addr_meta_et_);
    time_vec.push_back(traverse_log_et_);
}

// memory management
void ClientMM::mm_alloc_baseline(size_t size, UDPNetworkManager *nm, __OUT ClientMMAllocCtx *ctx)
{
    // n_dyn_req_ ++;
    int ret = 0;

    int num_blocks_required = size / subblock_sz_;
    assert(num_blocks_required == 1);

    assert(mm_blocks_.size() > 0);
    ClientMMBlock *alloc_block = mm_blocks_.front();
    mm_blocks_.pop();

    if (mm_blocks_.size() == 0)
    {
        ret = dyn_get_new_block_from_server_baseline(nm);
        if (ret == -1)
        {
            ctx->addr_list[0] = 0;
            return;
        }
    }

    ClientMMBlock *next_block = mm_blocks_.front();

    for (int i = 0; i < num_replication_; i++)
    {
        ctx->addr_list[i] = alloc_block->mr_info_list[i]->addr;
        ctx->rkey_list[i] = alloc_block->mr_info_list[i]->rkey;
        ctx->server_id_list[i] = alloc_block->server_id_list[i];

        ctx->next_addr_list[i] = next_block->mr_info_list[i]->addr;
        ctx->next_addr_list[i] |= next_block->server_id_list[i];

        ctx->prev_addr_list[i] = last_allocated_info_base_->mr_info_list[i]->addr;
        ctx->prev_addr_list[i] |= last_allocated_info_base_->server_id_list[i];
    }

    last_allocated_info_base_ = alloc_block;
}