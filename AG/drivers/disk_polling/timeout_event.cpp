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

#include <map>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "timeout_event.h"

using namespace std;

/****************************************
 * Global Variables
 ****************************************/
static map<int, struct timeout_event*> timeout_events_map;

/****************************************
 * Private Functions
 ****************************************/
static void* handle_timeout_event_generator_thread(void* param);
static void _clear_timeout_events();

/****************************************
 * Implementations of Public Functions
 ****************************************/
void init_timeout() {
    _clear_timeout_events();
}

void uninit_timeout() {
    _clear_timeout_events();
}

int set_timeout_event(int id, int timeout, PFN_TIMEOUT_USER_EVENT_HANDLER handler) {
    bool bFound = false;

    std::map<int, struct timeout_event*>::iterator found = timeout_events_map.find(id);
    if(found != timeout_events_map.end()) {
        if(found->second != NULL) {
            bFound = true;
        }
    }

    if(!bFound) {
        // not found
        struct timeout_event* event = (struct timeout_event*)malloc(sizeof(struct timeout_event));
        memset(event, 0, sizeof(struct timeout_event));

        event->id = id;
        event->timeout = timeout;
        event->handler = handler;
        
        pthread_create(&(event->thread), NULL, &handle_timeout_event_generator_thread, event);
        
        timeout_events_map[id] = event;
        return 0;
    }

    return -1;
}

static void* handle_timeout_event_generator_thread(void* param) {
    printf("timeout thread started\n");

    struct timeout_event* event = (struct timeout_event*)param;
    sleep(event->timeout);
    
    // waiting...
    printf("timeout thread woke up\n");
    
    // remove from map
    timeout_events_map[event->id] = NULL;

    // detach
    pthread_detach(pthread_self());

    // func call
    if(event->handler != NULL) {
        (*(event->handler))(event);
    }

    // free structure
    free(event);
    
    return NULL;
}

/****************************************
 * Implementations of Private Functions
 ****************************************/
static void _clear_timeout_events() {
    std::map<int, struct timeout_event*>::iterator iter;
    for(iter=timeout_events_map.begin();iter!=timeout_events_map.end();iter++) {
        // free all
        if(iter->second != NULL) {
            pthread_cancel(iter->second->thread);
        }
    }
    timeout_events_map.clear();
}
