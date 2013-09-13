/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#ifndef _XATTR_H_
#define _XATTR_H_

#include "fs_entry.h"

#define SYNDICATE_XATTR_MTIME          "user.syndicate_modtime"
#define SYNDICATE_XATTR_COORDINATOR    "user.syndicate_coordinator"

// extended attributes
ssize_t fs_entry_getxattr( struct fs_core* core, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume );
int fs_entry_setxattr( struct fs_core* core, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume );
ssize_t fs_entry_listxattr( struct fs_core* core, char const* path, char *list, size_t size, uint64_t user, uint64_t volume );
int fs_entry_removexattr( struct fs_core* core, char const* path, char const *name, uint64_t user, uint64_t volume );

#endif
