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

#ifndef _GATEWAY_CTX_H_
#define _GATEWAY_CTX_H_

#include <sys/types.h>
#include <unistd.h>

#include <libsyndicate.h>
#include <block-index.h>

//template <typename T>
struct gateway_ctx {
    int request_type;
    // file info 
    char const* file_path;
    // data buffer (manifest or remote block data)
    char* data;
    ssize_t data_len;
    off_t data_offset;
    off_t num_read;
    // file block info
    off_t block_id;
    // bounded SQL query
    unsigned char* sql_query_bounded;
    // unbounded SQL query
    unsigned char* sql_query_unbounded;
    // ODBC handle
    //T& odh; 
    // is this corresponds to .db_info file?
    bool is_db_info;
    // are we done?
    bool complete;
    // map_info
    struct map_info *mi;
    // block size of the volume
    ssize_t blocking_factor;
};

#endif //_GATEWAY_CTX_H_

