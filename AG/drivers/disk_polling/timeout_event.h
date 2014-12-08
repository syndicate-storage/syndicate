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

#ifndef TIMEOUT_EVENT_H
#define	TIMEOUT_EVENT_H

#include <unistd.h>

struct timeout_event;
typedef void (*PFN_TIMEOUT_USER_EVENT_HANDLER)(struct timeout_event* event);

struct timeout_event {
    int id;
    int timeout;
    PFN_TIMEOUT_USER_EVENT_HANDLER handler;
    pthread_t thread;
};

void init_timeout();
void uninit_timeout();

int set_timeout_event(int id, int timeout, PFN_TIMEOUT_USER_EVENT_HANDLER handler);

#endif	/* TIMEOUT_EVENT_H */

