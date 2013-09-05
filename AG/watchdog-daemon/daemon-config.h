#ifndef _DAEMON_CONFIG_H_
#define _DAEMON_CONFIG_H_

#include <iostream>
#include <iomanip>
#include <cstdlib>
#include <vector>
#include <libconfig.h++>

using namespace std;
using namespace libconfig;

#define AG_DAEMON_PORT	    (const char*)"ag_daemon_port"
#define WD_DAEMON_PORT	    (const char*)"watchdog_daemon_port"
#define WD_DAEMON_ADDR	    (const char*)"watchdog_daemon_addr"
#define ADMIN_EMAIL	    (const char*)"email"
#define AG_LIST		    (const char*)"ag_list"
#define START_DAEMON	    (const char*)"start_daemon"
#define NOTIFY		    (const char*)"send_notification"
#define AG_DAEMON_ADDR_LIST (const char*)"ag_daemon_addr_list"
#define AG_DAEMON_PORT_LIST (const char*)"ag_daemon_port_list"

typedef struct _daemon_config {
    /*Meaningful only to AG daemon*/
    //AG_LIST
    vector<string>  ag_list;
    //AG_DAEMON_PORT
    int		    ag_daemon_port;

    /*Meaningful only to watchdog daemon*/
    //AG_DAEMON_ADDR_LIST
    vector<string>  ag_addr_list;
    //AG_DAEMON_PORT_LIST
    vector<int>	    ag_port_list;  
    //ADMIN_EMAIL
    string	    admin_email;
    //NOTIFY
    bool	    send_notification;
    //START_DAEMON
    bool	    start_daemon;

    /*Meanungful to both AG daemon and watchdog daemon*/
    //WD_DAEMON_PORT
    int		    watchdog_daemon_port;
    //WD_DAEMON_ADDR
    string	    watchdog_addr;
} daemon_config;

//Returns a daemon config built from both config file and command line args.
//Command line args can override config file paramenters.
daemon_config* get_daemon_config(const char* cfg_file, const char **argv);

//Update fields in cfg from the values from the cfg file only if field is NULL.
void parse_daemon_config(const char* cfg_file, daemon_config *cfg);

//Update cfg fields from the parameters passed as command line arguments.
void parse_cmd_line(const char **argv, daemon_config *cfg);

#endif //_DAEMON_CONFIG_H_

