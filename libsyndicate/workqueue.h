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

#ifndef _LIBSYNDICATE_WQ_H_
#define _LIBSYNDICATE_WQ_H_

#include "util.h"

#include <queue>

// work request flags 
// treat a work request like a promise: let the caller block on it until it's fulfilled
#define MD_WQ_PROMISE        0x1

using namespace std;

// syndicate workqueue callback type
typedef int (*md_wq_func_t)( struct md_wreq* wreq, void* cls );

// syndicate workqueue request
struct md_wreq {
   
   // callback to do work 
   md_wq_func_t work;
   
   // user-supplied arguments
   void* work_data;
   
   // flags controlling the lifecycle of this work request
   int flags;
   
   // promise semaphore, to wake up the caller.
   // only initialized of MD_WQ_PROMISE is specified
   sem_t promise_sem;
   int promise_ret;
};

// workqueue type 
typedef queue< struct md_wreq > md_wq_queue_t;

// syndicate workqueue
struct md_wq {
   
   // caller-specific data 
   void* cls;
  
   // worker thread 
   pthread_t thread;
   
   // is the thread running?
   bool running;
   
   // things to do (double-bufferred)
   md_wq_queue_t* work;
   
   md_wq_queue_t* work_1;
   md_wq_queue_t* work_2;
   
   // lock governing access to work
   pthread_mutex_t work_lock;
   
   // semaphore to signal the availability of work
   sem_t work_sem;
};

extern "C" {
 
struct md_wq* md_wq_new( int count );
int md_wq_init( struct md_wq* wq, void* cls );
int md_wq_start( struct md_wq* wq );
int md_wq_stop( struct md_wq* wq );
int md_wq_free( struct md_wq* wq, void** ret_ );

int md_wreq_init( struct md_wreq* wreq, md_wq_func_t work, void* work_data, int flags );
int md_wreq_free( struct md_wreq* wreq );

int md_wreq_promise_wait( struct md_wreq* wreq );
int md_wreq_promise_ret( struct md_wreq* wreq );

int md_wq_add( struct md_wq* wq, struct md_wreq* wreq );

int md_wq_wakeup( struct md_wq* wq );

void* md_wq_cls( struct md_wq* wq );

}

#endif 