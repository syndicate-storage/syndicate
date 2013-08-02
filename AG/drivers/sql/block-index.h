#ifndef _BLOCK_INDEX_H_
#define _BLOCK_INDEX_H_

#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

#include <map>
#include <vector>
#include <string>
#include <iostream>


#define MAX_INDEX_SIZE 1024

using namespace std;

typedef struct {
    off_t   start_row;
    off_t   start_byte_offset;
    off_t   end_row;
    off_t   end_byte_offset;
} block_index_entry;


typedef map<string, vector<block_index_entry*>*> BlockMap;
typedef map<string, pthread_mutex_t> MutexMap;

void invalidate_entry(void* cls);

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
