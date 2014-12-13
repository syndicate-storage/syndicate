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

#ifndef _AG_WORKQUEUE_H_
#define _AG_WORKQUEUE_H_

#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>

#include <set>

#include "libsyndicate/workqueue.h"

#include "AG.h"
#include "map-info.h"

using namespace std;

// prototypes
struct AG_state;


extern "C" {

// high-level operations 
int AG_workqueue_add_publish( struct md_wq* wq, char const* fs_path, struct AG_driver_publish_info* pubinfo );
int AG_workqueue_add_reversion( struct md_wq* wq, char const* fs_path, struct AG_driver_publish_info* pubinfo );
int AG_workqueue_add_delete( struct md_wq* wq, char const* fs_path, struct AG_driver_publish_info* pubinfo );

}

#endif //_AG_REVERSIONER_H_

