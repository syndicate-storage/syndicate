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
#include <daemon-config.h>

daemon_config* get_daemon_config(const char* cfg_file, const char **argv) {
    daemon_config *cfg = new daemon_config;
    parse_daemon_config(cfg_file, cfg);
    //parse_cmd_line(argv, cfg);
    return cfg;
}

void parse_daemon_config(const char* cfg_file, daemon_config *cfg) {
    if (cfg == NULL || cfg_file == NULL)
	return;
    Config conf;
    try {
	conf.readFile(cfg_file);
    }
    catch (const FileIOException &e) {
	cerr<<"Unable to open the config file: "<<cfg_file<<endl;
	return;
    }
    catch (const ParseException &e) {
	cerr<<"unable to parse the config file: "<<cfg_file<<endl;
	return;
    }
    const Setting& root = conf.getRoot();
    int i = 0;
    try {
	//Set ag_deamon_port
	conf.lookupValue(AG_DAEMON_PORT, cfg->ag_daemon_port);
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<AG_DAEMON_PORT<<endl;
    }
    try {
	//Set watchdog_addr
	conf.lookupValue(WD_DAEMON_ADDR, cfg->watchdog_addr);
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<WD_DAEMON_ADDR<<endl;
    }
    try {
	//Set watchdog_port
	conf.lookupValue(WD_DAEMON_PORT, cfg->watchdog_daemon_port);
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<WD_DAEMON_PORT<<endl;
    }
    try {
	//Set email
	conf.lookupValue(ADMIN_EMAIL, cfg->admin_email);
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<ADMIN_EMAIL<<endl;
    }
    try {
	//Set send_notification
	conf.lookupValue(NOTIFY, cfg->send_notification);
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<NOTIFY<<endl;
    }
    try {
	//Set start_daemon
	conf.lookupValue(START_DAEMON, cfg->start_daemon);
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<START_DAEMON<<endl;
    }
    try{
	//Set AG daemon ports list
	const Setting &ports = root[AG_DAEMON_PORT_LIST];
	int nr_ports = ports.getLength();
	cfg->ag_port_list.resize(nr_ports);
	for (i=0; i<nr_ports; i++) {
	    int port = ports[i];
	    cfg->ag_port_list[i] = port;
	}
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<AG_DAEMON_PORT_LIST<<endl;
    }
    try {
	//Set AG daemon addr list
	const Setting &addrs = root[AG_DAEMON_ADDR_LIST];
	int nr_addrs = addrs.getLength();
	cfg->ag_port_list.resize(nr_addrs);
	for (i=0; i<nr_addrs; i++) {
	    string addr = addrs[i];
	    cfg->ag_addr_list[i] = addr;
	}
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<AG_DAEMON_ADDR_LIST<<endl;
    }
    try {
	//Set AG list...
	const Setting &ags = root[AG_LIST];
	int nr_ags = ags.getLength();
	cfg->ag_list.resize(nr_ags);
	for (i=0; i<nr_ags; i++) {
	    string ag_str = ags[i];
	    cfg->ag_list[i] = ag_str; 
	} 
    }
    catch (const SettingNotFoundException &e) {
	cerr<<"Setting not found: "<<AG_LIST<<endl;
    }
}

void parse_cmd_line(const char **argv, daemon_config *cfg) {
}

