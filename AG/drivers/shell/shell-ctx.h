/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
   
   Wathsala Vithanage (wathsala@princeton.edu)
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
    // file id
    uint id;
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

