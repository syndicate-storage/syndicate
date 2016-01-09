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

extern "C" {

// extended attributes
ssize_t UG_xattr_getxattr( struct SG_gateway* gateway, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume );
int UG_xattr_setxattr( struct SG_gateway* gateway, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume );
ssize_t UG_xattr_listxattr( struct SG_gateway* gateway, char const* path, char *list, size_t size, uint64_t user, uint64_t volume );
int UG_xattr_removexattr( struct SG_gateway* gateway, char const* path, char const *name, uint64_t user, uint64_t volume );

}

#endif
