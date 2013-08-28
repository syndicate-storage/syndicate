#ifndef _DAEMON_CONFIG_H_
#define _DAEMON_CONFIG_H_

typedef struct _daemon_config {
    char** ag_list;
    char* ag_daemon_port;
    char* watchdog_daemon_port;
    char* admin_email;
    bool  send_notification;
    bool  start_daemon;
} daemon_config;

//Returns a daemon config built from both config file and command line args.
//Command line args can override config file paramenters.
daemon_config* get_daemon_config(char *cfg_file, char **argv);

//Update fields in cfg from the values from the cfg file only if field is NULL.
void parse_daemon_config(char *cfg_file, daemon_config *cfg);

//Update cfg fields from the parameters passed as command line arguments.
void parse_cmd_line(char **argv, daemon_config *cfg);

#endif //_DAEMON_CONFIG_H_

