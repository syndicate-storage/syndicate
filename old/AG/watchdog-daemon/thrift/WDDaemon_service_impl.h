#ifndef _WDDAEMON_SERVICE_IMPL_H_
#define _WDDAEMON_SERVICE_IMPL_H_

#include <time.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <syslog.h>

#include <string>
#include <map>
#include <set>
#include <iostream>

#include <thrift-common.h>

using namespace std;

typedef struct _AGDaemonID_local {
    string addr;
    int32_t port;
    int16_t freq;
    time_t pulse_ts;
    map<int32_t, string> ag_map;
    int32_t id;
} AGDaemonID_local;

typedef struct _AGDaemonID_comp {
    bool operator() (AGDaemonID_local *lhs, AGDaemonID_local *rhs) const {
	return (lhs->pulse_ts > rhs->pulse_ts);
    }
} AGDaemonID_comp;

typedef set<AGDaemonID_local*, AGDaemonID_comp>::iterator fwd_itr;

void* AGDaemonID_local_timeout_thread (void* cls);

void delete_elements();

class WDDaemon_service_impl {
    private:
	int32_t current_id;
    public:
	WDDaemon_service_impl();
	int32_t register_agd(AGDaemonID_local *agdl);
	int32_t unregister_agd(int32_t id);
	void pulse(int32_t id, set<int32_t> live_set, 
		    set<int32_t> dead_set);
};

#endif //_WDDAEMON_SERVICE_IMPL_H_

