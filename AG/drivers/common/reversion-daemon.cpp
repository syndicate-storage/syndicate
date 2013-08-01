#include <reversion-daemon.h>

void* run_daemon(void *argc) {
    time_t last_ts;
    struct timespec request;
    struct timespec remain;
    timer_spec* revd_ts = (timer_spec*)argc;
    while (revd_ts->run) {
	request.tv_sec = revd_ts->min_timeout;
	request.tv_nsec = 0;
	remain.tv_sec = 0;
	remain.tv_nsec = 0;
	int rc = clock_nanosleep(CLOCK_MONOTONIC, 0/*TIMER_ABSTIME*/, 
			&request, &remain);
	if (rc < 0)
	    perror("clock_nanosleep");
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0) {
	    perror("clock_gettime: CLOCK_MONOTONIC");
	}
	time_t current_time = ts.tv_sec;
	time_t slept_time = request.tv_sec - remain.tv_sec;
	last_ts = current_time;
	ReversionDaemon::invalidate_map_info(revd_ts->map_set, slept_time);
    }
    return NULL;
}

ReversionDaemon::ReversionDaemon() {
    runnable = false;
    revd_ts.min_timeout = TICK_RATE;
    revd_ts.run = &runnable;
    revd_ts.map_set = &map_set;
}

void ReversionDaemon::run() {
    runnable = true;
    pthread_create(&tid, NULL, run_daemon, &revd_ts);
}

void ReversionDaemon::stop() {
    runnable = false;
}

void ReversionDaemon::add_map_info(struct map_info* mi) {
    map_set.insert(mi);
    set<struct map_info*, mi_time_stamp_comp>::iterator itr = map_set.begin();
    struct map_info* xmi = *itr;
    revd_ts.min_timeout = xmi->reval_sec;
}

void ReversionDaemon::invalidate_map_info(set<struct map_info*, 
					mi_time_stamp_comp>* map_set,
					time_t sleep_time) {
    set<struct map_info*, mi_time_stamp_comp>::iterator itr;
    for (itr = map_set->begin(); itr != map_set->end(); itr++) {
	struct map_info* mi = *itr;
	mi->mi_time += sleep_time;
	if (mi->mi_time >= mi->reval_sec) {
	    mi->mi_time = 0;
	    //Invalidate the mapping and associated state...
	    if (mi->invalidate_entry) { 
		mi->invalidate_entry(mi->entry);
		mi->entry = NULL;
	    }
	    else 
		cerr<<"No invalidation callback!"<<endl;
	}
	else {
	    //No point in going beyond this point
	    break;
	}
    }
    return;
}

