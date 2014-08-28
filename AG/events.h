/*
   Copyright 2014 The Trustees of Princeton University

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
#ifndef _AG_EVENTS_H_
#define _AG_EVENTS_H_

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

#include "AG.h"

#define AG_EVENT_PAYLOAD_LEN    4096
#define AG_EVENT_TERMINATE_ID   0
#define AG_EVENT_RECONF_ID      1
#define AG_EVENT_REPUBLISH_ID   2
#define AG_EVENT_DRIVER_IOCTL_ID 3

#define AG_NUM_EVENTS            4

using namespace std;

typedef int (*AG_event_handler)( char*, void* );

struct AG_event_listener {
   
   // event handlers
   AG_event_handler handlers[AG_NUM_EVENTS];
   void* args[AG_NUM_EVENTS];
   
   // UNIX socket descriptor 
   int sock_fd;
   char* sock_path;
   
   // threading
   pthread_t thread;
   bool running;
};

// event handlers
int AG_add_event_handler(struct AG_event_listener* events, int event, AG_event_handler handler, void *args);
int AG_remove_event_handler(struct AG_event_listener* events, int event);

// event listener wrangling 
int AG_event_listener_init( struct AG_event_listener* event_listener, struct AG_opts* ag_opts );
int AG_event_listener_start( struct AG_event_listener* event_listener );
int AG_event_listener_stop( struct AG_event_listener* event_listener );
int AG_event_listener_free( struct AG_event_listener* event_listener );

// send an event
int AG_send_event( char const* sock_path, int event_type, char* event_buf, size_t event_buf_len );
int AG_send_driver_ioctl_event( char const* sock_path, char const* driver_query_type, char* payload, size_t payload_len );
int AG_parse_driver_ioctl( char const* msg, char** driver_query_type, char** event_payload, size_t* event_payload_len );

#endif //_AG_EVENTS_H_

