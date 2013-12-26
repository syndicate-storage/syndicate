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
#define DRIVER_TERMINATE     0
#define DRIVER_RECONF_STR    "RCON"
#define DRIVER_RECONF	     1

#define NR_CMDS		     3
#define DRIVER_CMD_LEN	     4

#define DRIVER_RDONLY(lock)\
    pthread_rwlock_rdlock(lock);\
    cout<<"driver_rdonly_lock"<<endl;

#define DRIVER_RDWR(lock)\
    pthread_rwlock_wrlock(lock);\
    cout<<"driver_wronly_lock"<<endl;

#define DRIVER_UNLOCK(lock)\
    pthread_rwlock_unlock(lock);\
    cout<<"driver_unlock"<<endl;

#define DRIVER_RETURN(val,lock)\
    pthread_rwlock_unlock(lock);\
    cout<<"driver_unlock"<<endl;\
    return val;

using namespace std;

typedef void* (*driver_event_handler)(void*);

struct _driver_events {
    driver_event_handler deh[NR_CMDS];
    void* deh_arg[NR_CMDS];
    int fifo_fd;
    pthread_t tid;
    _driver_events(): fifo_fd(-1){}
};

//Delete all the files in a given directory
void clean_dir(const char *dir_name);

void add_driver_event_handler(int event, driver_event_handler deh,
			      void *args);

void remove_driver_event_handler(int event);

void* handle_command (char *cmd);

void* driver_event_loop(void *);

void driver_event_start();

int controller_signal_handler(pid_t pid, int flags);

#endif //_AG_UTIL_H_

