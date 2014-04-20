/*
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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
#include <util.h>

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
	void remove_map_info(struct map_info* mi);
	static void invalidate_map_info(set<struct map_info*, 
					mi_time_stamp_comp>* map_set,
					time_t sleep_time);
};

#endif //_REVERSION_DAEMON_H_

