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

#ifndef _SHELL_CTX_H_
#define _SHELL_CTX_H_

#include <sys/types.h>
#include <unistd.h>
#include <map-parser.h>

struct shell_ctx {
    int request_type;
    // file info 
    char const* file_path;
    // file descriptor
    int fd;
    
    // data buffer (manifest or remote block data)
    char* data;
    ssize_t data_len;
    off_t data_offset;
    bool need_padding;
    
    // command name
    char const* proc_name;
    // arguments array
    char **argv;
    // env array
    char **envp;
    // file block info
    off_t block_id;
    // map_info
    struct map_info *mi;
    // blocking factor
    ssize_t blocking_factor;
};

#endif //_GATEWAY_CTX_H_

