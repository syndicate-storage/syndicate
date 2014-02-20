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

// simple interface for a Syndicate UG.

#ifndef _SYNDICATE_H_
#define _SYNDICATE_H_

#include "libsyndicate/libsyndicate.h"
#include "cache.h"
#include "stats.h"
#include "replication.h"
#include "fs.h"
#include "state.h"
#include "opts.h"

int syndicate_init( struct syndicate_opts* opts );

void syndicate_finish_init();

struct syndicate_state* syndicate_get_state();
struct md_syndicate_conf* syndicate_get_conf();

int syndicate_destroy( int wait_replicas );

#endif
