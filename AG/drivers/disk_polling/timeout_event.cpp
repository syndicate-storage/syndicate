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

#include <string.h>
#include <signal.h>

#include "timeout_event.h"

static struct timeout_event g_current_timeout_event;

static void handle_timeout_event(int sig_no);

void init_timeout() {
    memset(&g_current_timeout_event, 0, sizeof(struct timeout_event));
}

int set_timeout_event(int timeout, PFN_TIMEOUT_USER_EVENT_HANDLER handler) {
    if(g_current_timeout_event.running) {
        // error! already timer is running
        return -1;
    }
    
    g_current_timeout_event.timeout_handler_backup = signal(SIGALRM, handle_timeout_event);
    if(g_current_timeout_event.timeout_handler_backup == SIG_ERR) {
        return -1;
    }
    
    g_current_timeout_event.timeout = timeout;
    g_current_timeout_event.handler = handler;
    g_current_timeout_event.running = true;
    
    alarm(timeout);
}

static void handle_timeout_event(int sig_no) {
    alarm(0);
    struct timeout_event event_backup = g_current_timeout_event;
    
    // restore signal handler
    signal(SIGALRM, g_current_timeout_event.timeout_handler_backup);
    memset(&g_current_timeout_event, 0, sizeof(struct timeout_event));
    
    (*event_backup.handler)(sig_no, &event_backup);
}
