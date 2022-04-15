#include "mm_test.h"
#include "client_fmm.h"

static void timer_fb_func(volatile bool *should_stop, int seconds)
{
    boost::this_fiber::sleep_for(std::chrono::seconds(seconds));
    *should_stop = true;
    // printf("stopped!\n");
}

static void timer_fb_func_ms(volatile bool *should_stop, int milliseconds)
{
    boost::this_fiber::sleep_for(std::chrono::milliseconds(milliseconds));
    *should_stop = true;
    // printf("stopped!\n");
}

static int mm_test_tpt(ClientFMM &client, MMRunClientArgs *args)
{
    int ret = 0;
    ret = client.load_seq_mm_requests(client.micro_workload_num_, args->op_type);
    assert(ret == 0);

    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    ClientFMMFiberArgs *fb_args_list = (ClientFMMFiberArgs *)malloc(sizeof(ClientFMMFiberArgs) * client.num_local_operations_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i++)
    {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = args->should_stop;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i++)
    {
        boost::fibers::fiber fb(client_ops_fb_cnt_ops_mm, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb;
    if (args->thread_id == 0)
    {
        printf("%d initializes timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
        boost::fibers::fiber fb(timer_fb_func_ms, args->should_stop, client.workload_run_time_);
        timer_fb = std::move(fb);
    }
    else
    {
        printf("%d wait for timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
    }

    printf("%d passed barrier\n", args->thread_id);
    if (args->thread_id == 0)
    {
        timer_fb.join();
    }
    uint32_t ops_cnt = 0;
    uint32_t num_failed = 0;
    for (int i = 0; i < client.num_coroutines_; i++)
    {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
        num_failed += fb_args_list[i].num_failed;
        printf("fb%d finished\n", fb_args_list[i].coro_id);
    }
    printf("thread: %d %d ops/s\n", args->thread_id, ops_cnt / 10);
    printf("%d failed\n", num_failed);

    // update counter
    if (strcmp(args->op_type, "ALLOC_IMPROVEMENT") == 0)
    {
        args->ret_num_alloc_ops = ops_cnt;
        args->ret_fail_alloc_num = num_failed;
    }
    else
    {
        assert(strcmp(args->op_type, "FREE_IMPROVEMENT") == 0);
        args->ret_num_free_ops = ops_cnt;
        args->ret_fail_free_num = num_failed;
    }
    free(fb_args_list);
    return 0;
}

void *run_client(void *_args)
{
    MMRunClientArgs *args = (MMRunClientArgs *)_args;

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    config.main_core_id = args->main_core_id;
    config.poll_core_id = args->poll_core_id;
    config.server_id += args->thread_id;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    // assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i++)
    {
        if (CPU_ISSET(i, &cpuset))
        {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }

    ClientFMM client(&config);

    pthread_t polling_tid = client.start_polling_thread();

    args->op_type = "ALLOC_IMPROVEMENT";
    client.workload_run_time_ = 500;
    if (args->thread_id == 0)
    {
        printf("press to sync start %s\n", args->op_type);
        getchar();
    }
    printf("before alloc start barrier", args->op_type);
    pthread_barrier_wait(args->alloc_start_barrier);
    printf("after alloc start barrier", args->op_type);

    printf("%d start %s\n", args->thread_id, args->op_type);
    ret = mm_test_tpt(client, args);
    assert(ret == 0);
    printf("%d %s finished\n", args->thread_id, args->op_type);
    pthread_barrier_wait(args->alloc_finish_barrier);

    args->op_type = "FREE_IMPROVEMENT";
    client.workload_run_time_ = 500;
    if (args->thread_id == 0)
    {
        pthread_barrier_init(args->timer_barrier, NULL, args->num_threads);
        *args->should_stop = false;
        printf("press to sync start %s\n", args->op_type);
        getchar();
    }
    pthread_barrier_wait(args->free_start_barrier);

    printf("%d start %s\n", args->thread_id, args->op_type);
    ret = mm_test_tpt(client, args);
    assert(ret == 0);
    printf("%d %s finished\n", args->thread_id, args->op_type);
    pthread_barrier_wait(args->free_finish_barrier);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}