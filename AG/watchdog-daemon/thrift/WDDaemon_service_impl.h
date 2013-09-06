#ifndef _WDDAEMON_SERVICE_IMPL_H_
#define _WDDAEMON_SERVICE_IMPL_H_

#include <time.h>
#include <sys/types.h>

#include <string>
#include <map>
#include <set>

using namespace std;

typedef struct _AGDaemonID_local {
    string addr;
    int32_t port;
    int16_t freq;
    time_t pulse_ts;
    map<int32_t, string> ag_map;
} AGDaemonID_local;

class WDDaemon_service_impl {
    private:
	int32_t current_id;
	map<int32_t, AGDaemonID_local*> agd_map;
    public:
	WDDaemon_service_impl();
	int32_t register_agd(AGDaemonID_local *agdl);
	int32_t unregister_agd(int32_t id);
	void pulse(int32_t id, set<int32_t> live_set, 
		    set<int32_t> dead_set);
};

#endif //_WDDAEMON_SERVICE_IMPL_H_

