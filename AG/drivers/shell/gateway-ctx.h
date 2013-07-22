#ifndef _GATEWAY_CTX_H_
#define _GATEWAY_CTX_H_

#include <sys/types.h>
#include <unistd.h>

struct gateway_ctx {
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
    off_t num_read;
    // command name
    char const* proc_name;
    // arguments array
    char **argv;
    // env array
    char **envp;
    // file block info
    off_t block_id;
    // are we done?
    bool complete;
};

#endif //_GATEWAY_CTX_H_

