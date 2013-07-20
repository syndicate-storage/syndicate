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
};

#endif //_GATEWAY_CTX_H_

