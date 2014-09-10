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
#include <AGDaemon_service_impl.h>

#include <iostream>

extern set<int32_t> live_set;
extern set<int32_t> dead_set;
extern int32_t	    agd_id;

AGDaemon_service_impl::AGDaemon_service_impl() {
    openlog(SYNDICATE_AG_SYSLOG_IDENT, 
	    LOG_CONS | LOG_PID | LOG_PERROR,
	    LOG_USER);
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

