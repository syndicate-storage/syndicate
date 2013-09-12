#ifndef _AGDAEMON_SERVICE_IMPL_H_
#define _AGDAEMON_SERVICE_IMPL_H_

#include <time.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdio.h>

#include <string>
#include <map>
#include <set>
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


