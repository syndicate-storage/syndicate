#include <WDDaemon_service_impl.h>

#include <iostream>

WDDaemon_service_impl::WDDaemon_service_impl() {
    current_id = 0;
}

int32_t WDDaemon_service_impl::register_agd(AGDaemonID_local *agdl) {
    agd_map[current_id] = agdl; 
    map<int32_t, string> ag_map = agdl->ag_map;
    cout<<">>> "<<ag_map[0]<<endl;
    current_id++; 
    return (current_id - 1);
}

int32_t WDDaemon_service_impl::unregister_agd(int32_t id) {
}

void WDDaemon_service_impl::pulse(int32_t id, set<int32_t> live_set, 
				    set<int32_t> dead_set) {
    AGDaemonID_local *agdl = agd_map[id];
    time(&(agdl->pulse_ts));
    //Check the dead_set and act accordingly.
}


