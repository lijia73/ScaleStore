#ifndef DDCKV_SERVER_MM_
#define DDCKV_SERVER_MM_

#include <infiniband/verbs.h>

#include <map>
#include <unordered_map>
#include <vector>

#include "kv_utils.h"
#include "hashtable.h"

struct MMBlock {
    uint8_t  free;
    uint64_t addr;
    struct MMBlock * next;
};

class ServerMM {
private:
    uint64_t base_addr_;
    uint64_t base_len_;
    uint64_t client_meta_area_off_;
    uint64_t client_meta_area_len_;
    uint64_t client_gc_area_off_;
    uint64_t client_gc_area_len_;
    uint64_t client_hash_area_off_;
    uint64_t client_hash_area_len_;
    uint64_t client_kv_area_off_;
    uint64_t client_kv_area_len_;
    
    uint32_t block_size_;
    uint32_t num_blocks_;
    uint32_t num_free_blocks_;
    struct ibv_mr  * mr_;
    struct MMBlock * mm_blocks_;

    std::map<uint64_t, struct MMBlock *> block_map_;
    std::vector<bool> subtable_alloc_map_;

    void   * data_;

    // private methods
private:
    //init hash table index stored at client_hash_area_off_
    int init_root(void * root_addr);
    int init_subtable(void * subtable_addr);
    int init_hashtable();

public:
    ServerMM(uint64_t server_base_addr, uint64_t base_len, 
        uint32_t block_size, const struct IbInfo * ib_info);
    ~ServerMM();
    
    struct MMBlock * mm_alloc();
    int mm_free(uint64_t st_addr);

    uint64_t mm_alloc_subtable();

    uint32_t get_rkey();

    int get_client_gc_info(uint32_t client_id, __OUT struct MrInfo * mr_info);
    int get_mr_info(__OUT struct MrInfo * mr_info);
    
    uint64_t get_kv_area_addr();
    uint64_t get_subtable_st_addr();
};

#endif