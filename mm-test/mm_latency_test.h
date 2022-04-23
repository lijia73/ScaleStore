#ifndef DDCMM_LATENCY_TEST_H_
#define DDCMM_LATENCY_TEST_H_

#include "client.h"
#include "client_fmm.h"

int test_alloc_baseline_lat(ClientFMM &client);
int test_free_baseline_lat(ClientFMM &client);
int test_alloc_improvement_lat(ClientFMM &client);
int test_free_improvement_lat(ClientFMM &client);

int test_fast_gc(ClientFMM &client);
int test_slow_gc(ClientFMM &client);

#endif