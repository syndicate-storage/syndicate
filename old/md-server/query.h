/*
   Copyright 2012 Jude Nelson

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

#ifndef _QUERY_H_
#define _QUERY_H_

#include "libsyndicate.h"
#include "cache.h"
#include <queue>

//#include <ev.h>


using namespace std;

struct query_entry {
   md_query::md_packet pkt;
   struct sockaddr_in remote_addr;
   socklen_t addrlen;
};

struct query_cache_entry {
   int64_t version;
   uint64_t mtime_sec;
   uint32_t mtime_nsec;
   uid_t owner;
   gid_t volume;
   mode_t mode;
   off_t size;
};

struct query_thread {
   pthread_t thread;
   md_cache* cache;
   struct md_query_server* qsrv;
};

int query_init( struct md_syndicate_conf* conf );
int query_invalidate_caches( char* path );
int query_shutdown();

#endif
