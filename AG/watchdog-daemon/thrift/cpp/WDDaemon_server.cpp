#include <WDDaemon_server.h>

WDDaemonHandler::WDDaemonHandler() {
    wdsi = new WDDaemon_service_impl();
}

void WDDaemonHandler::WDDaemonHandler::pulse(const int32_t id, 
					     const std::set<int32_t> & live_set, 
					     const std::set<int32_t> & dead_set) {
    printf("pulse\n");
    wdsi->pulse(id, live_set, dead_set);
}

int32_t WDDaemonHandler::register_agd(const  ::watchdog::AGDaemonID& agdid) {
    printf("register_agd\n");
    AGDaemonID_local *agd_local = new AGDaemonID_local;
    agd_local->addr = agdid.addr;
    agd_local->port = agdid.port;
    agd_local->freq = agdid.frequency; 
    int32_t ret = wdsi->register_agd(agd_local);
    return ret;
}

int32_t WDDaemonHandler::unregister_agd(const int32_t id) {
    printf("unregister_agd\n");
    int32_t ret = wdsi->unregister_agd(id);
    return ret;
}

