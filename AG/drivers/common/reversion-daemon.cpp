#include <reversion-daemon.h>

void* run_daemon(void *argc) {
    time_t last_ts;
    struct timespec request;
    struct timespec remain;
    bool* runnable = (bool*)argc;
    while (*runnable) {
	request.tv_sec = TICK_RATE;
	request.tv_nsec = 0;
	remain.tv_sec = 0;
	remain.tv_nsec = 0;
	clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, 
			&request, &remain);
	time_t current_time;
	time(&current_time);
	time_t slept_time = request.tv_sec - remain.tv_sec;
	last_ts = current_time;
	cout<<"Daemon Running: "<<slept_time<<endl;
    }
    return NULL;
}

ReversionDaemon::ReversionDaemon() {
    runnable = false;
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0) {
	perror("clock_gettime: CLOCK_MONOTONIC");
    }
}

void ReversionDaemon::run() {
    runnable = true;
    pthread_create(&tid, NULL, run_daemon, &runnable);
}

void ReversionDaemon::stop() {
    runnable = false;
}

void ReversionDaemon::add_map_info(struct map_info mi) {
}


