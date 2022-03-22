add_library(mm_latency_test mm_latency_test.cc)
add_executable(mm_latency_test_baseline_client mm_latency_test_baseline_client.cc)
add_executable(mm_test_server mm_test_server.cc)

target_link_libraries(mm_latency_test
    libddckv
    ycsb_test
    pthread
    ibverbs
)

target_link_libraries(mm_latency_test_baseline_client
    mm_latency_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(mm_test_server
    libddckv
    pthread
    ibverbs
)