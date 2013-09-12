#include <AGDaemon_service_impl.h>

#include <iostream>

extern set<int32_t> live_set;
extern set<int32_t> dead_set;
extern int32_t	    agd_id;

AGDaemon_service_impl::AGDaemon_service_impl() {
}

PingResponse_local AGDaemon_service_impl::ping() {
    PingResponse_local lpr;
    lpr.id = agd_id;
    lpr.dead_set = dead_set;
    lpr.live_set = live_set;
    return lpr;
}

int32_t AGDaemon_service_impl::restart(int32_t id) {
    return 0;
}

