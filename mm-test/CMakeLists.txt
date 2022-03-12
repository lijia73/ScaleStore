add_library(latency_test latency_test.cc)
add_executable(latency_test_client latency_test_client.cc)

target_link_libraries(latency_test
    libddckv
    ycsb_test
    pthread
    ibverbs
)

target_link_libraries(latency_test_client
    latency_test
    libddckv
    pthread
    ibverbs
)