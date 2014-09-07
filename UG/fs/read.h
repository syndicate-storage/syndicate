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

#ifndef _READ_H_
#define _READ_H_

#include "libsyndicate/cache.h"
#include "fs_entry.h"
#include "consistency.h"

enum fs_entry_block_read_status_t {
   READ_NOT_STARTED = 0,
   READ_DOWNLOAD_NOT_STARTED = 1,
   READ_PRIMARY = 2,
   READ_REPLICA = 3,
   READ_FINISHED = 4,
   READ_ERROR = 5
};

// pending outcome of a block read
struct fs_entry_read_block_future {
   
   // block info
   char const* fs_path;         // NOTE: this is a reference to the client-supplied fs_path, not a duplicate.  DO NOT FREE IT
   uint64_t gateway_id;         // which gateway hosts this copy of the block
   int64_t file_version;
   uint64_t block_id;
   int64_t block_version;
   
   fs_entry_block_read_status_t status;
   
   bool has_dlctx;
   struct md_download_context dlctx;           // download context; only used if the data is being downloaded (NULL otherwise)
   
   int curr_RG;                 // if trying replicas, this is the handle to the current RG
   char* curr_URL;              // current URL (valid during the download)
   
   bool is_AG;                  // if true, we're downloading from an AG (so don't try the RGs)
   
   char* result;                // pointer to the buffer to hold block data (NULL at first; filled in when ready).  *ALWAYS* aligned on a block boundry
   off_t result_len;            // length of the above buffer (*AWLAYS* the volume block size)
   off_t result_start;          // offset in result where the logical read on this block begins
   off_t result_end;            // offset in result where the logical read on this block ends
   bool result_is_partial_head; // true if we will read part of result at the start of the read request buffer
   bool result_is_partial_tail; // true if we will read part of result at the end of the read request buffer
   bool result_allocd;          // if true, then result was dyanmically allocated (instead of pointing to an offset in a client's read buffer).
   
   int retry_count;             // how many times the download has been retried?
   
   int err;                     // set to nonzero on error
   bool eof;                    // set if the block couldn't be found (indicates EOF; only relevant for an AG)
   bool size_unknown;           // set to true if we don't know the size of the file in advance (only possible for an AG)
   bool downloaded;             // set to true if we downloaded data 
   
   sem_t sem;                   // posted when data is available
};


// set of block reads 
typedef set<struct fs_entry_read_block_future*> fs_entry_read_block_future_set_t;

// look-up read future from download context 
typedef map<struct md_download_context*, struct fs_entry_read_block_future*> fs_entry_download_to_future_map_t;

// function to call after finalization of a read future on download (either in error or on success)
typedef int (*fs_entry_read_block_future_download_finalizer_func)( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_block_future* fut, void* cls );

// read context
struct fs_entry_read_context {
   
   fs_entry_read_block_future_set_t* reads;
   
   fs_entry_download_to_future_map_t* download_to_future;
   
   md_download_set dlset;       // for downloading reads
};

// read context 
int fs_entry_read_context_init( struct fs_entry_read_context* read_ctx );
int fs_entry_read_context_free_all( struct fs_core* core, struct fs_entry_read_context* read_ctx );

int fs_entry_read_context_add_block_future( struct fs_entry_read_context* read_ctx, struct fs_entry_read_block_future* block_fut );
int fs_entry_read_context_remove_block_future( struct fs_entry_read_context* read_ctx, struct fs_entry_read_block_future* block_fut );
size_t fs_entry_read_context_size( struct fs_entry_read_context* read_ctx );
int fs_entry_read_context_run_local( struct fs_core* core, char const* fs_path, struct fs_entry* fent, struct fs_entry_read_context* read_ctx );
bool fs_entry_read_context_has_downloading_blocks( struct fs_entry_read_context* read_ctx );
int fs_entry_read_context_setup_downloads( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_context* read_ctx );
int fs_entry_read_context_run_downloads( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_context* read_ctx );
int fs_entry_read_context_run_downloads_ex( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_context* read_ctx, bool write_locked, fs_entry_read_block_future_download_finalizer_func finalizer, void* finalizer_cls );

// block futures 
int fs_entry_read_block_future_init( struct fs_entry_read_block_future* block_fut, uint64_t gateway_id, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version,
                                     char* result_buf, off_t result_buf_len, off_t block_read_start, off_t block_read_end, bool free_result_buf );
int fs_entry_read_block_future_free( struct fs_entry_read_block_future* block_fut );

// bufferred blocks 
int fs_entry_update_bufferred_block( struct fs_file_handle* fh, struct fs_entry* fent, fs_entry_read_block_future_set_t* reads );

int fs_entry_read_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_buf, size_t block_len );
ssize_t fs_entry_read_block_local( struct fs_core* core, char const* fs_path, uint64_t block_id, char* block_buf, size_t block_len );

ssize_t fs_entry_read( struct fs_core* core, struct fs_file_handle* fh, char* buf, size_t count, off_t offset );

#endif