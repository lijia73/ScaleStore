#ifndef DDCKV_MASTER_H_
#define DDCKV_MASTER_H_

#include <stdint.h>
#include <netdb.h>

#include <vector>

#include "kv_utils.h"

class Master {
    uint32_t udp_sock_;
    uint16_t udp_port_;
    bool should_stop_;

    struct sockaddr_in * my_addr_;
    std::vector<struct sockaddr_in *> server_addr_list_;

private:
    int master_on_register(const struct KVMsg * request, 
        struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int master_on_recover(const struct KVMsg * request, 
        struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int master_on_headtbeat(const struct KVMsg * request,
        struct sockaddr_in * src_addr, socklen_t src_addr_len);

public:
    Master(GlobalConfig * config);
    ~Master();

    void * thread_main();
    void stop();
};

#endif