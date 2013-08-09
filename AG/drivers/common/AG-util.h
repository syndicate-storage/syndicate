#ifndef _AG_UTIL_H_
#define _AG_UTIL_H_

#include <iostream>

#include <sys/types.h>
#include <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#define DRIVER_TERMINATE_STR "TERM"
#define DRIVER_TERMINATE     1
#define DRIVER_RECONF_STR    "RECONF"
#define DRIVER_RECONF	     2

using namespace std;

typedef void* (*driver_event_handler)(void*);

struct _driver_events {
    driver_event_handler term_deh;
    driver_event_handler reconf_deh;
    _driver_events():term_deh(NULL), reconf_deh(NULL){}
};

//Delete all the files in a given directory
void clean_dir(const char *dir_name);

void add_driver_event_handler(int event, driver_event_handler deh);

void remove_driver_event_handler(driver_event_handler deh);

void driver_event_loop(void *);

#endif //_AG_UTIL_H_

