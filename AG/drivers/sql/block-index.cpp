#include "block-index.h"
#include <stdio.h>

BlockIndex::BlockIndex()
{
    //Initialize file locks here...
}

block_index_entry* BlockIndex::alloc_block_index_entry()
{
    block_index_entry *blkie = new block_index_entry;
    blkie->start_row = 0;
    blkie->start_byte_offset = 0;
    blkie->end_row = 0;
    blkie->end_byte_offset = 0;
    return blkie;
}

void BlockIndex::update_block_index(string file_name, off_t block_id, block_index_entry* blkie)
{
    vector<block_index_entry*> *blk_list = NULL;
    block_index_entry *old_entry = NULL;
    BlockMap::iterator itr = blk_map.find(file_name);
    if (itr != blk_map.end()) {
	blk_list = itr->second;
	if((old_entry = (*blk_list)[block_id]) != NULL) {
	    (*blk_list)[block_id] = blkie;
	    delete old_entry;
	}
	else {
	    blk_list->resize(blk_list->size() + 1);
	    (*blk_list)[block_id] = blkie;
	}
    }
    else {
	blk_list = new vector<block_index_entry*>();
	blk_list->reserve(MAX_INDEX_SIZE);
	blk_list->resize(1);
	(*blk_list)[block_id] = blkie;
	blk_map[file_name] = blk_list;
    }
}

const block_index_entry* BlockIndex::get_block(string file_name, off_t block_id)
{
    if (block_id < 0)
	return NULL;
    vector<block_index_entry*> *blk_list = NULL;
    BlockMap::iterator itr = blk_map.find(file_name);
    if (itr != blk_map.end()) {
	blk_list = itr->second;
	return (*blk_list)[block_id];
    }
    else 
	return NULL;
}

const block_index_entry* BlockIndex::get_last_block(string file_name)
{
    vector<block_index_entry*> *blk_list = NULL;
    BlockMap::iterator itr = blk_map.find(file_name);
    if (itr != blk_map.end()) {
	blk_list = itr->second;
	block_index_entry *blkie =  blk_list->back();
	return blkie;
    }
    else 
	return NULL;
}

