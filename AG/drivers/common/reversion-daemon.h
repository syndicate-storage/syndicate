/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
   
   Wathsala Vithanage (wathsala@princeton.edu)
*/

#ifndef _REVERSION_DAEMON_H_
#define _REVERSION_DAEMON_H_

#define TICK_RATE 60

#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>

#include <set>

#include <map-parser.h>

using namespace std;

typedef struct {
    bool operator()(struct map_info* lmi, struct map_info* rmi) {
	return (lmi->reval_sec < rmi->reval_sec);
    }
} mi_time_stamp_comp;

typedef struct {
    uint64_t  min_timeout;
    bool*     run;
    set<struct map_info*, mi_time_stamp_comp>* map_set;
} timer_spec;


void* run_daemon(void *argc);

class ReversionDaemon {
    private:
	bool	    runnable;
	pthread_t   tid;
	timer_spec  revd_ts;
	set<struct map_info*, mi_time_stamp_comp> map_set;

    public:
	ReversionDaemon();
	void run();
	void stop();
	void add_map_info(struct map_info* mi);
	static void invalidate_map_info(set<struct map_info*, 
					mi_time_stamp_comp>* map_set,
					time_t sleep_time);
};

#endif //_REVERSION_DAEMON_H_

