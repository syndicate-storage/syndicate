#ifndef _SQL_DRIVER_H_
#define _SQL_DRIVER_H_

#include <map>
#include <string>
#include <set>
#include "libgateway.h"
#include "libsyndicate.h"
#include "map-parser.h"
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

using namespace std;

#define GATEWAY_REQUEST_TYPE_NONE       0
#define GATEWAY_REQUEST_TYPE_LOCAL_FILE 1
#define GATEWAY_REQUEST_TYPE_MANIFEST   2
#define SYNDICATEFS_AG_DB_PROTO         "synadb://"
#define SYNDICATEFS_AG_DB_DIR           1
#define SYNDICATEFS_AG_DB_FILE          2

struct gateway_ctx {
    int request_type;
    // file info 
    char const* file_path;
    // data buffer (manifest or remote block data)
    char* data;
    size_t data_len;
    size_t data_offset;
    off_t num_read;
    // file block info
    uint64_t block_id;
    // input descriptor
    int fd;
};

struct path_comp {
    bool operator()(char *path1, char *path2) 
    {
	int p1_nr_comps = 0;
	int p2_nr_comps = 0;
	if (strcmp(path1, path2) == 0)
	    return false;
	size_t p1_len = strlen(path1);
	size_t p2_len = strlen(path2);
	size_t i;
	for (i=0; i<p1_len; i++) {
	    if (path1[i] == '/')
		p1_nr_comps++;
	}
	for (i=0; i<p2_len; i++) {
	    if (path2[i] == '/')
		p2_nr_comps++;
	}
	if (p1_nr_comps == p2_nr_comps) 
	    return true;
	else 
	    return p1_nr_comps < p2_nr_comps;
	
    }
};   

typedef map<string, struct md_entry*> content_map;
static int publish(const char *fpath, int type, const char* sql_query);

#endif //_SQL_DRIVER_H_

