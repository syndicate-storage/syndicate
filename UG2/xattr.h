/*
   Copyright 2015 The Trustees of Princeton University

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

#ifndef _UG_XATTR_H_
#define _UG_XATTR_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/client.h>
#include <libsyndicate/url.h>

#include "inode.h"

#include <sys/types.h>
#include <attr/xattr.h>

// in case we didn't include xattr.h
#ifndef ENOTATTR
#define ENOATTR ENODATA
#endif

#define UG_XATTR_COORDINATOR       "user.syndicate_coordinator"
#define UG_XATTR_CACHED_BLOCKS     "user.syndicate_cached_blocks"
#define UG_XATTR_CACHED_FILE_PATH  "user.syndicate_cached_file_path"
#define UG_XATTR_READ_TTL          "user.syndicate_read_ttl"
#define UG_XATTR_WRITE_TTL         "user.syndicate_write_ttl"

#define UG_XATTR_NAMESPACE_RG            "user.syndicate_RG."
#define UG_XATTR_NAMESPACE_RG_SECRET     "user.syndicate_RG_secret."

#define UG_XATTR_NAMESPACE_UG            "user.syndicate_UG."
#define UG_XATTR_NAMESPACE_UG_SECRET     "user.syndicate_UG_secret."

typedef ssize_t (*UG_xattr_get_handler_t)( struct fskit_core*, struct fskit_entry*, char const*, char*, size_t );
typedef int (*UG_xattr_set_handler_t)( struct fskit_core*, struct fskit_entry*, char const*, char const*, size_t, int );
typedef int (*UG_xattr_delete_handler_t)( struct fskit_core*, struct fskit_entry*, char const* );

struct UG_xattr_handler_t {
   char const* name;
   UG_xattr_get_handler_t get;
   UG_xattr_set_handler_t set;
   UG_xattr_delete_handler_t del;
};

struct UG_xattr_namespace_handler {
   char const* prefix;
   UG_xattr_get_handler_t get;
   UG_xattr_set_handler_t set;
   UG_xattr_delete_handler_t del;
};

// extended attributes
ssize_t UG_getxattr( struct SG_gateway* gateway, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume );
int UG_setxattr( struct SG_gateway* gateway, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume );
int UG_setxattr_ex( struct SG_gateway* gateway, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume, mode_t mode );
ssize_t UG_listxattr( struct SG_gateway* gateway, char const* path, char *list, size_t size, uint64_t user, uint64_t volume );
int UG_removexattr( struct SG_gateway* gateway, char const* path, char const *name, uint64_t user, uint64_t volume );

// not POSIX-y
int UG_chownxattr( struct SG_gateway* gateway, char const* path, char const *name, uint64_t new_user );
int UG_chmodxattr( struct SG_gateway* gateway, char const* path, char const* name, mode_t new_mode );

int UG_download_xattr( struct SG_gateway* gateway, uint64_t volume, uint64_t file_id, char const* name, char** value );
int UG_get_or_set_xattr( struct SG_gateway* gateway, struct fskit_entry* fent, char const* name, char const* proposed_value, size_t proposed_value_len, char** value, size_t* value_len, mode_t mode );

#endif
