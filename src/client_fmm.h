#ifndef DDCKV_CLIENT_FMM_H_
#define DDCKV_CLIENT_FMM_H_

#include <map>

#include <pthread.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <sys/time.h>

#include <string>
#include <unordered_map>
#include <boost/fiber/all.hpp>

#include "client_mm.h"
#include "nm.h"
#include "kv_utils.h"
#include "hashtable.h"
#include "ib.h"
#include "kv_debug.h"

#define CLINET_INPUT_BUF_LEN (512 * 1024 * 1024)
#define CORO_LOCAL_BUF_LEN (32 * 1024 * 1024)

enum MMRequestType
{
    MM_REQ_ALLOC_BASELINE,
    MM_REQ_ALLOC_IMPROVEMENT,
    MM_REQ_FREE_BASELINE,
    MM_REQ_FREE_IMPROVEMENT
};

enum MMOpsRetCode
{
    MM_OPS_SUCCESS = 0,
    MM_OPS_FAIL_RETURN,
    MM_OPS_FAIL_REDO
};

typedef struct TagMMReqCtx
{
    // input
    uint64_t coro_id;
    uint8_t req_type;
    KVInfo *kv_info;

    uint32_t size_;

    ClientMMAllocCtx mm_alloc_ctx;

    // return
    bool is_finished;
    union
    {
        void *value_addr; // for alloc return value
        int ret_code;
    } ret_val;

    volatile bool *should_stop;

} MMReqCtx;

class ClientFMM
{
private:
    ClientMM *mm_;
    UDPNetworkManager *nm_;

    uint32_t my_server_id_;
    uint32_t num_replication_;
    uint32_t num_memory_;
    uint32_t num_idx_rep_;

    uint8_t pr_log_server_id_;
    uint64_t pr_log_head_;
    uint64_t pr_log_tail_;

    uint64_t remote_global_meta_addr_;
    uint64_t remote_meta_addr_;
    uint64_t remote_gc_addr_;
    uint64_t remote_root_addr_;

    uint64_t server_st_addr_;
    uint64_t server_data_len_;

    float miss_rate_threash_;

    RaceHashRoot *race_root_;
    struct ibv_mr *race_root_mr_;

    void *local_buf_;
    struct ibv_mr *local_buf_mr_;

    void *input_buf_;
    struct ibv_mr *input_buf_mr_;

    uint64_t *coro_local_addr_list_;

    std::map<std::string, LocalCacheEntry *> addr_cache_;
    std::map<uint32_t, struct MrInfo *> server_mr_info_map_;

    // core bind information
    uint32_t main_core_id_;
    uint32_t poll_core_id_;
    uint32_t bg_core_id_;
    uint32_t gc_core_id_;

    // crash testing information
    std::map<uint8_t, bool> server_crash_map_;
    std::vector<ClientMetaAddrInfo> meta_addr_info_;

    // private inline methods
private:
    inline int get_race_root()
    {
        int ret = nm_->nm_rdma_read_from_sid((void *)race_root_, race_root_mr_->lkey, sizeof(RaceHashRoot),
                                             remote_root_addr_, server_mr_info_map_[0]->rkey, 0);
        // assert(ret == 0);
        return 0;
    }

    inline int write_race_root()
    {
        int ret = 0;
        for (int i = 0; i < num_replication_; i++)
        {
            ret = nm_->nm_rdma_write_to_sid((void *)race_root_, race_root_mr_->lkey, sizeof(RaceHashRoot),
                                            remote_root_addr_, server_mr_info_map_[i]->rkey, i);
            // assert(ret == 0);
        }
        return 0;
    }

    inline char *get_key(KVInfo *kv_info)
    {
        return (char *)((uint64_t)kv_info->l_addr + sizeof(KVLogHeader));
    }

    inline char *get_value(KVInfo *kv_info)
    {
        return (char *)((uint64_t)kv_info->l_addr + sizeof(KVLogHeader) + kv_info->key_len);
    }

    inline KVLogHeader *get_header(KVInfo *kv_info)
    {
        return (KVLogHeader *)kv_info->l_addr;
    }

    inline void update_cache(std::string key_str, RaceHashSlot *slot_info, uint64_t *r_slot_addr_list)
    {
        // char key_buf[128] = {0};
        // memcpy(key_buf, get_key(kv_info), kv_info->key_len);
        // std::string tmp_key(key_buf);

        std::map<std::string, LocalCacheEntry *>::iterator it = addr_cache_.find(key_str);
        if (it != addr_cache_.end())
        {
            LocalCacheEntry *entry = it->second;
            // check if is miss
            if (*(uint64_t *)(&entry->l_slot_ptr) != *(uint64_t *)slot_info)
            {
                entry->miss_cnt++;
                memcpy(&entry->l_slot_ptr, slot_info, sizeof(RaceHashSlot));
                for (int i = 0; i < num_idx_rep_; i++)
                {
                    entry->r_slot_addr[i] = r_slot_addr_list[i];
                }
            }
            // update access cnt
            entry->acc_cnt++;
            return;
        }

        LocalCacheEntry *tmp_value = (LocalCacheEntry *)malloc(sizeof(LocalCacheEntry));
        memcpy(&tmp_value->l_slot_ptr, slot_info, sizeof(RaceHashSlot));
        tmp_value->acc_cnt = 1;
        tmp_value->miss_cnt = 0;

        for (int i = 0; i < num_idx_rep_; i++)
        {
            tmp_value->r_slot_addr[i] = r_slot_addr_list[i];
        }

        addr_cache_[key_str] = tmp_value;
        // print_log(DEBUG, "\t[%s] %s->slot(%lx) kv(%lx)", __FUNCTION__, key_buf, r_slot_addr_list[0], HashIndexConvert40To64Bits(tmp_value->l_slot_ptr.pointer));
    }

    inline LocalCacheEntry *check_cache(std::string key_str)
    {
        // char key_buf[128] = {0};
        // memcpy(key_buf, get_key(kv_info), kv_info->key_len);
        // std::string tmp_key(key_buf);

        std::map<std::string, LocalCacheEntry *>::iterator it = addr_cache_.find(key_str);
        if (it == addr_cache_.end())
        {
            // print_log(DEBUG, "\t\t[%s] cache miss", __FUNCTION__);
            return NULL;
        }
        if (HashIndexConvert40To64Bits(it->second->l_slot_ptr.pointer) == 0)
        {
            free(it->second);
            addr_cache_.erase(it);
            // print_log(DEBUG, "\t\t[%s] cache empty pointer miss", __FUNCTION__);
            return NULL;
        }

        float miss_rate = ((float)it->second->miss_cnt / it->second->acc_cnt);
        if (miss_rate > miss_rate_threash_)
        {
            return NULL;
        }
        // print_log(DEBUG, "\t\t[%s] cache hit", __FUNCTION__);
        return it->second;
    }

    inline void remove_cache(std::string key_str)
    {
        std::map<std::string, LocalCacheEntry *>::iterator it = addr_cache_.find(key_str);
        if (it != addr_cache_.end())
        {
            addr_cache_.erase(it);
        }
    }

    inline bool delete_cache(KVInfo *kv_info)
    {
        char key_buf[256];
        memset(key_buf, 0, 256);
        memcpy(key_buf, get_key(kv_info), kv_info->key_len);
        std::string tmp_key(key_buf);

        return addr_cache_.erase(tmp_key);
    }

    inline bool check_key(KVLogHeader *log_header, KVInfo *kv_info)
    {
        uint64_t r_key_addr = (uint64_t)log_header + sizeof(log_header);
        uint64_t l_key_addr = (uint64_t)kv_info->l_addr + sizeof(KVLogHeader);
        return CheckKey((void *)r_key_addr, log_header->key_length, (void *)l_key_addr, kv_info->key_len);
    }

    inline int poll_completion(std::map<uint64_t, struct ibv_wc *> &wait_wrid_wc_map)
    {
        int ret = 0;
        while (ib_is_all_wrid_finished(wait_wrid_wc_map) == false)
        {
            // print_log(DEBUG, "\t\t[%s] fiber: %ld yielding", __FUNCTION__, boost::this_fiber::get_id());
            // boost::this_fiber::yield();
            boost::this_fiber::sleep_for(std::chrono::microseconds(10));
            ret = nm_->nm_check_completion(wait_wrid_wc_map);
            // kv_assert(ret == 0);
        }
        return ret;
    }

    inline int poll_completion(std::map<uint64_t, struct ibv_wc *> &wait_wrid_wc_map, volatile bool *should_stop)
    {
        int ret = 0;
        while (ib_is_all_wrid_finished(wait_wrid_wc_map) == false && (*should_stop) == false)
        {
            // print_log(DEBUG, "\t\t[%s] fiber: %ld yielding", __FUNCTION__, boost::this_fiber::get_id());
            if (*(should_stop))
            {
                return ret;
            }
            boost::this_fiber::yield();
            ret = nm_->nm_check_completion(wait_wrid_wc_map);
            // kv_assert(ret == 0);
        }
        return ret;
    }

    // private methods
private:
    bool init_is_finished();
    int sync_init_finish();
    int connect_ib_qps();
    int write_client_meta_info();
    int init_hash_table();

    IbvSrList *gen_write_kv_sr_lists(uint32_t coro_id, KVInfo *a_kv_info, ClientMMAllocCtx *r_mm_info, __OUT uint32_t *num_sr_lists);
    void free_write_kv_sr_lists(IbvSrList *sr_list);

    void init_mm_req_ctx(MMReqCtx *req_ctx, KVInfo *kv_info, char *operation);
    void update_kv_header(KVLogHeader *kv_header, ClientMMAllocCtx *alloc_ctx);

    // public methods
public:
    ClientFMM(const struct GlobalConfig *conf);
    ~ClientFMM();

    int alloc_baseline(MMReqCtx *ctx);
    int free_baseline(MMReqCtx *ctx);

    int alloc_improvement(MMReqCtx *ctx);
    int free_improvement(MMReqCtx *ctx);

    int load_seq_mm_requests(uint32_t num_ops, char *op_type);

    int get_num_rep();

    KVInfo *kv_info_list_;
    MMReqCtx *mm_req_ctx_list_;
    uint32_t num_total_operations_;
    uint32_t num_local_operations_;
    uint32_t num_coroutines_;
    int workload_run_time_;
    int micro_workload_num_;
};

typedef struct TagClientFMMFiberArgs
{
    ClientFMM *client;
    uint32_t ops_st_idx;
    uint32_t ops_num;
    uint32_t coro_id;

    uint32_t num_failed;

    // for count time
    struct timeval *st;
    struct timeval *et;

    // for count ops
    boost::fibers::barrier *b;
    volatile bool *should_stop;
    uint32_t ops_cnt;
    uint32_t thread_id;
} ClientFiberArgs;
void *client_ops_fb_cnt_ops_mm(void *arg);
#endif