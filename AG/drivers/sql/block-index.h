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

#ifndef _BLOCK_INDEX_H_
#define _BLOCK_INDEX_H_

#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

#include <map>
#include <vector>
#include <string>
#include <iostream>

#include <gateway-ctx.h>

#define MAX_INDEX_SIZE 1024
#define AG_BLOCK_SIZE()\
    (sysconf(_SC_PAGESIZE) * 10)

using namespace std;

typedef struct {
    off_t   start_block_id;
    off_t   start_block_offset;
    off_t   end_block_id;
    off_t   end_block_offset;
} block_translation_info;

typedef struct {
    off_t   start_row;
    off_t   start_byte_offset;
    off_t   end_row;
    off_t   end_byte_offset;
} block_index_entry;


typedef map<string, vector<block_index_entry*>*> BlockMap;
typedef map<string, pthread_mutex_t> MutexMap;

void invalidate_entry(void* cls);

block_translation_info volume_block_to_ag_block(struct gateway_ctx *ctx);

class BlockIndex {
    private:
	BlockMap	    blk_map;
	MutexMap	    mutex_map;
	pthread_mutex_t	    *map_mutex;
	void free_block_index(vector<block_index_entry*> *);
    public:
	BlockIndex();
	block_index_entry* alloc_block_index_entry();
	void update_block_index(string file_name, off_t block_id, 
				block_index_entry* blkie);
	const block_index_entry* get_block(string file_name,
					   off_t block_id);
	const block_index_entry* get_last_block(string file_name, off_t *block_id);
	void invalidate_entry(string file_name);
};

#endif //_BLOCK_INDEX_H_

