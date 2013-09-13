#include <WDDaemon_service_impl.h>

#include <iostream>


int32_t base_id;
set<AGDaemonID_local*, AGDaemonID_comp> agd_set;
map<int32_t, AGDaemonID_local*> agd_map;
set<AGDaemonID_local*> del_set;

void delete_elements() {
    set<AGDaemonID_local*>::iterator itr;
    for (itr = del_set.begin(); itr != del_set.end(); itr++) {
	AGDaemonID_local *agdl = *itr;
	fwd_itr del_itr = agd_set.find(agdl);
	agd_set.erase(del_itr);
	agd_map.erase(agdl->id - base_id);
	delete agdl;
    }
    del_set.clear();
}

void* AGDaemonID_local_timeout_thread (void* cls) {
    struct timespec request;
    struct timespec remain;
    while (true) {
	request.tv_sec = 30;
	request.tv_nsec = 0;
	remain.tv_sec = 0;
	remain.tv_nsec = 0;
	int rc = clock_nanosleep(CLOCK_MONOTONIC, 0,
	                                    &request, &remain);
	if (rc < 0)
	        perror("clock_nanosleep");
	//Traverse the set...
	set<AGDaemonID_local*, AGDaemonID_comp>::reverse_iterator itr;
	fwd_itr fitr;
	for (itr = agd_set.rbegin(); itr != agd_set.rend(); itr++) {
	    AGDaemonID_local *agdl = *itr;
	    time_t current_time;
	    time(&current_time);
	    if ((current_time - agdl->pulse_ts) >= agdl->freq * 2) {
		syslog(LOG_WARNING, "%s:%i Pulse rate lower than anticipated (%i)\n", agdl->addr.c_str(), agdl->port, agdl->freq);
		//ping
		PingResponse pr;
		thrift_connection *tc = thrift_connect(agdl->addr.c_str(), agdl->port, false);
		if (tc->is_connected) {
		    tc->ag_client->ping(pr);
		    if (pr.id != agdl->id) {
			syslog(LOG_CRIT, "%s:%i Returns an unexpected ID\n", agdl->addr.c_str(), agdl->port);
			del_set.insert(agdl);
			continue;
		    }
		    fitr = agd_set.find(*itr);
		    agd_set.erase(fitr);
		    time(&(agdl->pulse_ts));
		    agd_set.insert(agdl);
		}
		else {
		    syslog(LOG_CRIT, "%s at %s:%i", tc->err, agdl->addr.c_str(), agdl->port);
		    del_set.insert(agdl);
		}
		thrift_disconnect(tc);
	    }
	    else {
		break;
	    }
	}
	delete_elements();
    }
    return NULL;
}

WDDaemon_service_impl::WDDaemon_service_impl() {
    openlog(SYNDICATE_WD_SYSLOG_IDENT, 
	    LOG_CONS | LOG_PID | LOG_PERROR,
	    LOG_USER);
    current_id = 0;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    srand(ts.tv_sec + ts.tv_nsec);
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
    agdl->id = (base_id + current_id) - 1;
    syslog(LOG_INFO, "Registered daemon %i at %s:%i\n", ((base_id + current_id) - 1), agdl->addr.c_str(), agdl->port);
    return ((base_id + current_id) - 1);
}

int32_t WDDaemon_service_impl::unregister_agd(int32_t id) {
    return 0;
}

void WDDaemon_service_impl::pulse(int32_t id, set<int32_t> live_set, 
				    set<int32_t> dead_set) {
    AGDaemonID_local *agdl = agd_map[id - base_id];
    if (!agdl) {
	syslog(LOG_WARNING, "Stale ot bogus daemon at %s:%i sent pulse\n", agdl->addr.c_str(), agdl->port);
	return;
    }
    set<AGDaemonID_local*, AGDaemonID_comp>::iterator agd_it;
    agd_it = agd_set.find(agdl);
    if (agd_it == agd_set.end()) {
	syslog(LOG_WARNING, "Stale ot bogus daemon at %s:%i sent pulse\n", agdl->addr.c_str(), agdl->port);
	return;
    }
    agd_set.erase(agd_it);
    time(&(agdl->pulse_ts));
    agd_set.insert(agdl);
    set<int32_t>::iterator it;
    cout<<"ID: "<<id<<endl;
    cout<<"Live set size: "<<live_set.size()<<endl;
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


