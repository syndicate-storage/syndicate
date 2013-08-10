#ifndef _AG_UTIL_H_
#define _AG_UTIL_H_

#include <iostream>
#include <sstream>

#include <sys/types.h>
#include <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <util.h>

#define FIFO_PREFIX "/tmp/syndicate-ag."
#define FIFO_PREFIX_LEN\
    strlen(FIFO_PREFIX)

#define DRIVER_TERMINATE_STR "TERM"
#define DRIVER_TERMINATE     1
#define DRIVER_RECONF_STR    "RECONF"
#define DRIVER_RECONF	     2

using namespace std;

typedef void* (*driver_event_handler)(void*);

struct _driver_events {
    driver_event_handler term_deh;
    driver_event_handler reconf_deh;
    int fifo_fd;
    _driver_events():term_deh(NULL), 
		    reconf_deh(NULL), 
		    fifo_fd(-1){}
};

//Delete all the files in a given directory
void clean_dir(const char *dir_name);

void add_driver_event_handler(int event, driver_event_handler deh);

void remove_driver_event_handler(int event);

void* driver_event_loop(void *);

void driver_event_start();

#endif //_AG_UTIL_H_

