#include <WDDaemon_service_impl.h>

#include <iostream>

set<AGDaemonID_local*, AGDaemonID_comp> agd_set;

void* AGDaemonID_local_timeout_thread (void* cls) {
    struct timespec request;
    struct timespec remain;
    while (true) {
	request.tv_sec = 10;
	request.tv_nsec = 0;
	remain.tv_sec = 0;
	remain.tv_nsec = 0;
	int rc = clock_nanosleep(CLOCK_MONOTONIC, 0,
	                                    &request, &remain);
	if (rc < 0)
	        perror("clock_nanosleep");
	//Traverse the set...
	set<AGDaemonID_local*, AGDaemonID_comp>::reverse_iterator itr;
	for (itr = agd_set.rbegin(); itr != agd_set.rend(); itr++) {
	    AGDaemonID_local *agdl = *itr;
	    time_t current_time;
	    time(&current_time);
	    if ((current_time - agdl->pulse_ts) >= agdl->freq * 2) {
		//ping
		thrift_connection *tc = thrift_connect(agdl->addr, agdl->port, false);
		PingResponse pr;
		tc->ag_client->ping(pr);
	    }
	}
    }
    return NULL;
}

WDDaemon_service_impl::WDDaemon_service_impl() {
    current_id = 0;
    base_id = rand(); 
    pthread_t tid;
    pthread_create(&tid, NULL, AGDaemonID_local_timeout_thread, NULL);
}

int32_t WDDaemon_service_impl::register_agd(AGDaemonID_local *agdl) {
    time(&(agdl->pulse_ts));
    agd_map[current_id] = agdl; 
    agd_set.insert(agdl);
    map<int32_t, string> ag_map = agdl->ag_map;
    current_id++; 
    cout<<"Registered: "<<(base_id + current_id) - 1<<endl;
    return ((base_id + current_id) - 1);
}

int32_t WDDaemon_service_impl::unregister_agd(int32_t id) {
    return 0;
}

void WDDaemon_service_impl::pulse(int32_t id, set<int32_t> live_set, 
				    set<int32_t> dead_set) {
    AGDaemonID_local *agdl = agd_map[id - base_id];
    set<AGDaemonID_local*, AGDaemonID_comp>::iterator agd_it;
    agd_it = agd_set.find(agdl);
    agd_set.erase(agd_it);
    time(&(agdl->pulse_ts));
    agd_set.insert(agdl);
    set<int32_t>::iterator it;
    cout<<"ID: "<<id<<endl;
    cout<<"Livet set size: "<<live_set.size()<<endl;
    for (it = live_set.begin(); it != live_set.end(); it++) {
	cout<<"Live ID: "<<*it<<endl;
	cout<<"Live AG: "<<agdl->ag_map[*it]<<endl;
    }
    cout<<"Dead set size: "<<dead_set.size()<<endl;
    for (it = dead_set.begin(); it != dead_set.end(); it++) {
	cout<<"Dead ID: "<<*it<<endl;
	cout<<"Dead AG: "<<agdl->ag_map[*it]<<endl;
    }
    //Check the dead_set and act accordingly.
}


