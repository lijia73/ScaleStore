#ifndef DDCKV_CLIENT_MM_H_
#define DDCKV_CLIENT_MM_H_

#include <infiniband/verbs.h>

#include <vector>
#include <boost/fiber/all.hpp>
#include <thread>
#include <mutex>
#include <queue>

#include "kv_utils.h"
#include "nm.h"
#include "spinlock.h"
#include "hashtable.h"

#define MAX_NUM_SUBBLOCKS 4
#define MAX_WATER_MARK 0.7

typedef struct TagClientMMBlock
{
    struct MrInfo mr_info_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
    bool *bmap;
    uint32_t num_allocated;
    int32_t prev_free_subblock_idx;
    int32_t next_free_subblock_idx;
    int32_t next_free_subblock_cnt;

    uint64_t next_mmblock_addr[MAX_REP_NUM];
} ClientMMBlock;

enum ClientAllocType
{
    TYPE_SUBTABLE = 1,
    TYPE_KVBLOCK = 2,
    TYPE_BASELINE = 3
};

typedef struct TagClientMMAllocCtx
{
    uint8_t server_id_list[MAX_REP_NUM];
    uint64_t addr_list[MAX_REP_NUM];
    uint64_t prev_addr_list[MAX_REP_NUM]; // TODO: modify address as address + serverid
    uint64_t next_addr_list[MAX_REP_NUM];
    uint32_t rkey_list[MAX_REP_NUM];

    uint32_t num_subblocks;
    bool need_change_prev;

    uint32_t size_;
} ClientMMAllocCtx;

typedef struct TagClientMMAllocSubtableCtx
{
    uint8_t server_id;
    uint64_t addr;
} ClientMMAllocSubtableCtx;

typedef struct TagRecoverLogInfo
{
    KVLogHeader *local_header_addr;
    uint64_t remote_addr;
    uint8_t server_id;
} RecoverLogInfo;

typedef struct TagSubblockInfo
{
    uint64_t addr_list[MAX_REP_NUM];
    uint32_t rkey_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
} SubblockInfo;

class ClientMM
{
private:
    uint32_t num_replication_;
    uint32_t num_idx_rep_;
    uint32_t num_memory_;

    std::vector<ClientMMBlock *> mm_blocks_;
    spinlock_t mm_blocks_lock_;
    uint32_t cur_mm_block_idx_;

    uint32_t subblock_num_;
    uint32_t last_allocated_;

    uint8_t pr_log_server_id_;
    uint64_t pr_log_head_;

    uint64_t client_meta_addr_;
    uint64_t client_gc_addr_;
    uint64_t client_gc_nums_addr_;

    std::mutex alloc_new_block_lock_;
    bool is_allocing_new_block_;

    // for gc
    void *gc_buf_;
    struct ibv_mr *gc_mr_;
    std::vector<ClientGCAddrInfo> gc_info_list_;

    // for recovery
    void *recover_buf_;
    struct ibv_mr *recover_mr_;
    std::vector<RecoverLogInfo> recover_log_info_list_;
    std::unordered_map<uint64_t, bool> recover_addr_is_allocated_map_;
    void *log_header_st_ptr_;

    // modification
    std::queue<SubblockInfo> subblock_free_queue_;
    SubblockInfo last_allocated_info_;
    std::queue<SubblockInfo> subblock_free_queue_remote_;
    uint64_t last_freed_addr_;

    ClientMMBlock last_allocated_info_base_;

    std::map<std::string, std::queue<SubblockInfo>> allocated_subblock_key_map_;

    // uint32_t n_dyn_req_;

    struct timeval local_recover_space_et_;
    struct timeval get_addr_meta_et_;
    struct timeval traverse_log_et_;

    // private methods
private:
    int init_get_new_block_from_server(UDPNetworkManager *nm);
    int init_reg_space(struct MrInfo mr_inf_list[][MAX_REP_NUM], uint8_t server_id_list[][MAX_REP_NUM],
                       UDPNetworkManager *nm, int reg_type);
    int32_t dyn_get_new_block_from_server(UDPNetworkManager *nm);
    int32_t dyn_get_new_block_from_server_baseline(UDPNetworkManager *nm);
    int get_new_block_from_server(UDPNetworkManager *nm);
    int local_reg_subblocks(const struct MrInfo *mr_info_list, const uint8_t *server_id_list);
    int local_reg_blocks(const struct MrInfo *mr_info_list, const uint8_t *server_id_list);
    int reg_new_space(const struct MrInfo *mr_info_list, const uint8_t *server_id_list,
                      UDPNetworkManager *nm, int reg_type);
    int dyn_reg_new_space(const struct MrInfo *mr_info_list, const uint8_t *server_id_list,
                          UDPNetworkManager *nm, int reg_type);
    int32_t alloc_from_sid(uint32_t server_id, UDPNetworkManager *nm, int alloc_type,
                           __OUT struct MrInfo *mr_info);
    void update_mm_block_next(ClientMMBlock *mm_block);
    int remote_write_meta_addr(UDPNetworkManager *nm);

    int mm_recover_prepare_space(UDPNetworkManager *nm);
    int mm_gc_prepare_space(UDPNetworkManager *nm);

    int get_remote_log_header(UDPNetworkManager *nm, uint8_t server_id, uint64_t r_addr,
                              KVLogHeader *local_addr);
    int mm_traverse_log(UDPNetworkManager *nm);
    int mm_get_addr_meta(UDPNetworkManager *nm);
    int mm_recover_mm_blocks(UDPNetworkManager *nm);

    uint32_t get_subblock_idx(uint64_t addr, ClientMMBlock *cur_block);
    ClientMMBlock *get_new_mmblock();

    void gen_subblock_info(ClientMMBlock *mm_block, uint32_t subblock_idx, __OUT SubblockInfo *subblock_info);

    int free_block_to_server(UDPNetworkManager *nm, uint64_t *addr_list, const uint8_t *server_id_list);
    int free_from_sid(UDPNetworkManager *nm, const struct MrInfo mr_info, const uint32_t server_id);
    void mm_free_local(uint64_t *addr_list, uint32_t *rkey_list, const uint8_t *server_id_list);
    void mm_free_remote(uint64_t *addr_list, uint32_t *rkey_list, const uint8_t *server_id_list);

    // for gc
    void init_gc_buf_();
    void free_gc_buf();
    bool isbelongmine(uint64_t free_addr);

    // inline private methods
private:
    inline uint32_t get_alloc_hint_rr()
    {
        return last_allocated_++;
    }

    inline float get_water_mark()
    {
        float num_used = 0;
        for (size_t i = 0; i < mm_blocks_.size(); i++)
        {
            num_used += mm_blocks_[i]->num_allocated;
        }
        return num_used / (mm_blocks_.size() * subblock_num_);
    }

    // public methods
public:
    uint64_t mm_block_sz_;
    uint64_t subblock_sz_;

    ClientMM(const struct GlobalConfig *conf,
             UDPNetworkManager *nm);
    ~ClientMM();

    void get_log_head(__OUT uint64_t *pr_log_head, __OUT uint64_t *bk_log_head);

    void mm_alloc(size_t size, UDPNetworkManager *nm, __OUT ClientMMAllocCtx *ctx);
    void mm_alloc(size_t size, UDPNetworkManager *nm, std::string key, __OUT ClientMMAllocCtx *ctx);
    void mm_alloc_log_info(RecoverLogInfo *log_info, __OUT ClientMMAllocCtx *ctx);

    void mm_free_key(std::string key);
    void mm_free_key_all(std::string key);

    void mm_alloc_subtable(UDPNetworkManager *nm, __OUT ClientMMAllocSubtableCtx *ctx);

    int get_last_log_recover_info(__OUT RecoverLogInfo *recover_log_info);
    void free_recover_buf();

    void get_time_bread_down(std::vector<struct timeval> &time_vec);

    // memory management
    void mm_alloc_baseline(size_t size, UDPNetworkManager *nm, __OUT ClientMMAllocCtx *ctx);
    int mm_free_baseline(UDPNetworkManager *nm, ClientMMAllocCtx *ctx);

    void mm_alloc_improvement(size_t size, UDPNetworkManager *nm, __OUT ClientMMAllocCtx *ctx);
    int mm_free_improvement(UDPNetworkManager *nm, ClientMMAllocCtx *ctx);

    int syn_gc_info(UDPNetworkManager *nm);
    int mm_recovery(UDPNetworkManager *nm);
    // inline public methods
public:
    inline uint64_t get_remote_meta_ptr()
    {
        return client_meta_addr_;
    }

    inline uint32_t get_num_mm_blocks()
    {
        return mm_blocks_.size();
    }

    inline bool should_alloc_new()
    {
        float water_mark = get_water_mark();
        return water_mark > MAX_WATER_MARK;
    }

    inline bool should_start_gc()
    {
        ClientMMBlock *cur_mmblock = mm_blocks_[cur_mm_block_idx_];
        return cur_mmblock->next_free_subblock_cnt < MAX_NUM_SUBBLOCKS;
    }

    inline ClientMMBlock *get_cur_mm_block()
    {
        return mm_blocks_[cur_mm_block_idx_];
    }

    inline void get_log_head(__OUT uint8_t *pr_log_server_id, __OUT uint64_t *pr_log_head)
    {
        *pr_log_server_id = pr_log_server_id_;
        *pr_log_head = pr_log_head_;
    }

    inline size_t get_aligned_size(size_t size)
    {
        if ((size % subblock_sz_) == 0)
        {
            return size;
        }
        size_t aligned = ((size / subblock_sz_) + 1) * subblock_sz_;
        return aligned;
    }
};

#endif