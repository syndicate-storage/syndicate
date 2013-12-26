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
#ifndef _AGDAEMON_SERVICE_IMPL_H_
#define _AGDAEMON_SERVICE_IMPL_H_

#include <time.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdio.h>
#include <syslog.h>

#include <string>
#include <map>
#include <set>
#include <vector>
#include <iostream>

#include <thrift-common.h>

using namespace std;

typedef struct _PingResponse_local {
    int32_t id;
    set<int32_t> live_set;
    set<int32_t> dead_set;
} PingResponse_local;

class AGDaemon_service_impl {
    public:
	AGDaemon_service_impl();
	
	PingResponse_local ping();

	int32_t restart(int32_t id);
};

#endif //_AGDAEMON_SERVICE_IMPL_H_


