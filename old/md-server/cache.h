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

#ifndef _MD_CACHE_H_
#define _MD_CACHE_H_

#include "libsyndicate.h"
#include <map>

using namespace std;

struct cache_entry {
   uint64_t atime;
   uid_t user;
   mode_t mode;
   
   char* data;
   size_t data_len;
};

typedef map<string, struct cache_entry> cache_map;
typedef map<uint64_t, string> time_map;

// read/write cache, with user permissions
class md_cache {
public:

   md_cache( uint64_t max_size );
   ~md_cache();

   int get( char const* path, char** serialized, size_t* len, uid_t user );
   int put( char const* path, char* serialized, size_t len, uid_t user, mode_t mode );
   int clear( char const* path );

   uint64_t size;
private:

   int do_clear( string s );
   
   pthread_rwlock_t lock;
   uint64_t max_size;
   
   cache_map data;
   time_map data_freq;
};


// cache!
extern md_cache* Cache;

void cache_init();
void cache_shutdown();

#endif