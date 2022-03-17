#include "server_mm.h"

#include <unistd.h>
#include <sys/mman.h>
#include <assert.h>

#include "kv_debug.h"

#define MAP_HUGE_2MB        (21 << MAP_HUGE_SHIFT)
#define MAP_HUGE_1GB        (30 << MAP_HUGE_SHIFT)

ServerMM::ServerMM(uint64_t server_base_addr, uint64_t base_len, 
    uint32_t block_size, const struct IbInfo * ib_info) {
    this->block_size_ = block_size;
    this->base_addr_ = server_base_addr;
    this->base_len_  = base_len;
    int port_flag = PROT_READ | PROT_WRITE;
    int mm_flag   = MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB | MAP_HUGE_2MB;
    this->data_ = mmap((void *)this->base_addr_, this->base_len_, port_flag, mm_flag, -1, 0);
    // assert((uint64_t)this->data_ == this->base_addr_);

    this->client_meta_area_off_ = 0;
    this->client_meta_area_len_ = META_AREA_LEN;
    this->client_gc_area_off_ = this->client_meta_area_len_;
    this->client_gc_area_len_ = GC_AREA_LEN;
    this->client_hash_area_off_ = this->client_gc_area_off_ + this->client_gc_area_len_;
    this->client_hash_area_len_ = HASH_AREA_LEN;
    // assert(HASH_AREA_LEN > ROOT_RES_LEN + SUBTABLE_RES_LEN);
    this->client_kv_area_off_ = this->client_hash_area_off_ + this->client_hash_area_len_;
    this->client_kv_area_len_ = this->base_len_ - this->client_gc_area_len_ - this->client_hash_area_len_;

    //init hash index
    init_hashtable();

    int access_flag = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | 
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    this->mr_ = ibv_reg_mr(ib_info->ib_pd, this->data_, this->base_len_, access_flag);
    // print_log(DEBUG, "addr %lx rkey %x", mr_->addr, mr_->rkey);
    
    // init blocks
    this->block_map_.clear();
    this->num_blocks_ = this->client_kv_area_len_ / this->block_size_;
    this->num_free_blocks_ = this->num_blocks_;
    this->mm_blocks_  = (struct MMBlock *)malloc(sizeof(struct MMBlock) * this->num_blocks_);
    uint64_t kv_area_addr = this->client_kv_area_off_ + this->base_addr_;
    // print_log(DEBUG, "kv_area_addr(%lx) block_size(%x)", kv_area_addr, block_size_);
    // print_log(DEBUG, "meta_area_addr(%lx) meta_area_len(%x)", client_meta_area_off_ + base_addr_, META_AREA_LEN);
    // print_log(DEBUG, "gc_area_addr(%lx) gc_area_len(%x)", client_gc_area_off_ + base_addr_, GC_AREA_LEN);
    // print_log(DEBUG, "hash_area_addr(%lx) hash_area_len(%x)", client_hash_area_off_ + base_addr_, client_hash_area_len_);
    for (uint64_t i = 0; i < this->num_blocks_; i ++) {
        uint64_t block_addr = kv_area_addr + i * this->block_size_;
        this->mm_blocks_[i].free = 1;
        this->mm_blocks_[i].addr = block_addr;
        if (i != this->num_blocks_ - 1) {
            this->mm_blocks_[i].next = &this->mm_blocks_[i + 1];
        } else {
            this->mm_blocks_[i].next = NULL;
        }
        this->block_map_[block_addr] = &this->mm_blocks_[i];
    }
}

ServerMM::~ServerMM() {
    munmap(data_, this->base_len_);
    free(this->mm_blocks_);
    this->block_map_.clear();
}

struct MMBlock * ServerMM::mm_alloc() {
    if (this->num_free_blocks_ == 0) {
        return NULL;
    }

    std::map<uint64_t, struct MMBlock *>::iterator it;
    for (it = this->block_map_.begin(); it != this->block_map_.end(); it ++){
        if (it->second->free == 1) {
            break;
        }
    }
    this->num_free_blocks_ --;
    it->second->free = 0;
    return it->second;
}

int ServerMM::mm_free(uint64_t st_addr) {
    if (this->block_map_[st_addr] == NULL)
        return -1;
    if (this->block_map_[st_addr]->free != 0)
        return -1;
    this->block_map_[st_addr]->free = 1;
    this->num_free_blocks_++;
    return 0;
}

uint64_t ServerMM::mm_alloc_subtable() {
    int ret = 0;
    uint64_t subtable_st_addr = base_addr_ + client_hash_area_off_ + roundup_256(ROOT_RES_LEN);
    for (size_t i = 0; i < subtable_alloc_map_.size(); i ++) {
        if (subtable_alloc_map_[i] == 0) {
            subtable_alloc_map_[i] = 1;
            return subtable_st_addr + i * roundup_256(SUBTABLE_LEN);
        }
    }
    return 0;
}

uint32_t ServerMM::get_rkey() {
    return this->mr_->rkey;
}

int ServerMM::get_client_gc_info(uint32_t client_id, __OUT struct MrInfo * mr_info) {
    uint64_t single_gc_len = 1024 * 1024;
    uint64_t client_gc_off = client_id * single_gc_len;
    if (client_gc_off + single_gc_len >= this->client_gc_area_len_) {
        return -1;
    }
    mr_info->addr = this->client_gc_area_off_ + client_gc_off + this->base_addr_;
    mr_info->rkey = this->mr_->rkey;
    return 0;
}

int ServerMM::get_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = this->base_addr_;
    mr_info->rkey = this->mr_->rkey;
    return 0;
}

int ServerMM::init_root(void * root_addr) {
    RaceHashRoot * root = (RaceHashRoot *)root_addr;
    root->global_depth = RACE_HASH_GLOBAL_DEPTH;
    root->init_local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
    root->max_global_depth = RACE_HASH_MAX_GLOBAL_DEPTH;
    root->prefix_num = 1 << RACE_HASH_MAX_GLOBAL_DEPTH;
    root->subtable_res_num = root->prefix_num;
    root->subtable_init_num = RACE_HASH_INIT_SUBTABLE_NUM;
    root->subtable_hash_range = RACE_HASH_ADDRESSABLE_BUCKET_NUM;
    root->subtable_bucket_num = RACE_HASH_SUBTABLE_BUCKET_NUM;
    root->seed = rand();
    root->root_offset = client_hash_area_off_;
    root->subtable_offset = root->root_offset + roundup_256(ROOT_RES_LEN);
    root->kv_offset = client_kv_area_off_;
    root->kv_len = client_kv_area_len_;
    root->lock = 0;

    return 0;
}

int ServerMM::init_subtable(void * subtable_addr) {
    // RaceHashBucket * bucket = (RaceHashBucket *)subtable_addr;
    uint64_t max_subtables = (base_addr_ + client_hash_area_off_ + client_hash_area_len_ - (uint64_t)subtable_addr) / roundup_256(SUBTABLE_LEN);

    subtable_alloc_map_.resize(max_subtables);
    for (int i = 0; i < max_subtables; i ++) {
        uint64_t cur_subtable_addr = (uint64_t)subtable_addr + i * roundup_256(SUBTABLE_LEN);
        subtable_alloc_map_[i] = 0;
        for (int j = 0; j < RACE_HASH_ADDRESSABLE_BUCKET_NUM; j ++) {
            RaceHashBucket * bucket = (RaceHashBucket *)cur_subtable_addr + j;
            bucket->local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
            bucket->prefix = i;
            bucket ++;
        }
    }

    return 0;
}

int ServerMM::init_hashtable() {
    uint64_t root_addr = base_addr_ + client_hash_area_off_;
    uint64_t subtable_st_addr = get_subtable_st_addr();
    init_root((void *)(root_addr));
    init_subtable((void *)(subtable_st_addr));
    return 0;
}

uint64_t ServerMM::get_kv_area_addr() {
    return client_kv_area_off_ + base_addr_;
}

uint64_t ServerMM::get_subtable_st_addr() {
    return client_hash_area_off_ + base_addr_ + roundup_256(ROOT_RES_LEN);
}