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

#ifndef _XATTR_H_
#define _XATTR_H_

#include "fs_entry.h"

#include <sys/types.h>
#include <attr/xattr.h>

// in case we didn't include xattr.h
#ifndef ENOTATTR
#define ENOATTR ENODATA
#endif

#define SYNDICATE_XATTR_COORDINATOR       "user.syndicate_coordinator"
#define SYNDICATE_XATTR_CACHED_BLOCKS     "user.syndicate_cached_blocks"
#define SYNDICATE_XATTR_READ_TTL          "user.syndicate_read_ttl"
#define SYNDICATE_XATTR_WRITE_TTL         "user.syndicate_write_ttl"

typedef ssize_t (*xattr_get_handler)( struct fs_core*, struct fs_entry*, char*, size_t );
typedef int (*xattr_set_handler)( struct fs_core*, struct fs_entry*, char const*, size_t, int );
typedef int (*xattr_delete_handler)( struct fs_core*, struct fs_entry* );

struct syndicate_xattr_handler {
   char const* name;
   xattr_get_handler get;
   xattr_set_handler set;
   xattr_delete_handler del;
};

// extended attributes
ssize_t fs_entry_getxattr( struct fs_core* core, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume );
int fs_entry_setxattr( struct fs_core* core, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume );
ssize_t fs_entry_listxattr( struct fs_core* core, char const* path, char *list, size_t size, uint64_t user, uint64_t volume );
int fs_entry_removexattr( struct fs_core* core, char const* path, char const *name, uint64_t user, uint64_t volume );

#endif
