#include <stdlib.h>
#include <assert.h>
#include <arpa/inet.h>

#include "master.h"
#include "kv_utils.h"

Master::Master(GlobalConfig * config) {
    int ret = 0;
    udp_sock_ = socket(AF_INET, SOCK_DGRAM, 0);
    udp_port_ = config->master_port;

    struct timeval timeout;
    timeout.tv_sec  = 1;
    timeout.tv_usec = 0;

    my_addr_ = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
    // assert(my_addr_ != NULL);
    memset(my_addr_, 0, sizeof(struct sockaddr_in));

    my_addr_->sin_family = AF_INET;
    my_addr_->sin_port   = htons(udp_port_);
    my_addr_->sin_addr.s_addr = htonl(INADDR_ANY);

    ret = bind(udp_sock_, (struct sockaddr *)&server_addr_list_[0], sizeof(struct sockaddr_in));
    // assert(ret >= 0);
}

Master::~Master() {
    free(my_addr_);
    close(udp_sock_);
}

void * Master::thread_main() {
    struct sockaddr_in client_addr;
    socklen_t          client_addr_len = sizeof(struct sockaddr_in);
    struct KVMsg request;
    int rc = 0;
    while (!should_stop_) {
        rc = recvfrom(udp_sock_, &request, sizeof(struct KVMsg), 
            0, (struct sockaddr *)&client_addr, &client_addr_len);
        // assert(rc == sizeof(struct KVMsg));
        deserialize_kvmsg(&request);

        if (request.type == REQ_REGISTER) {
            rc = master_on_register(&request, &client_addr, client_addr_len);
        } else if (request.type == REQ_RECOVER) {
            rc = master_on_recover(&request, &client_addr, client_addr_len);
        }
    }
}