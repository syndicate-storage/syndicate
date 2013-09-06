#ifndef _AG_MAIN_H_
#define _AG_MAIN_H_

#include <daemon-config.h>
#include <thrift-common.h>

#include <signal.h>
#include <unistd.h>

typedef struct _pulse_data {
    daemon_config	*dc;
    thrift_connection	*tc; 
    int32_t		id;
} pulse_data;

int mask_all_signals();

int unmask_signal(int signum, sighandler_t sighand);

void SIGCHLD_handler(int);

void* generate_pulses(void*);

char** tokenize_command(char *cmd, char **port);

int start_ag(char **cmd);

string get_ag_descriptor(char *host, char* port);

#endif //_AG_MAIN_H_

