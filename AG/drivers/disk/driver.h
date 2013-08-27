#ifndef _DISK_DRIVER_H_
#define _DISK_DRIVER_H_

#include <map>
#include <string>
#include <sstream>

#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <ftw.h>

#include <libgateway.h>
#include <ms-client.h>
#include <libsyndicate.h>
#include <AG-util.h>

using namespace std;

#define GATEWAY_REQUEST_TYPE_NONE 0
#define GATEWAY_REQUEST_TYPE_LOCAL_FILE 1
#define GATEWAY_REQUEST_TYPE_MANIFEST 2

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
   // blocking factor
   ssize_t blocking_factor;
};

typedef map<string, struct md_entry*> content_map;
static int publish_to_volumes(const char *fpath, const struct stat *sb,
	int tflag, struct FTW *ftwbuf); 
static int publish(const char *fpath, const struct stat *sb,
	int tflag, struct FTW *ftwbuf, uint64_t volume_id);
void init();
void* term_handler(void *cls);

#endif //_DISK_DRIVER_H_

