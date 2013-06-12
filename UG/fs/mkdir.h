/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _MKDIR_H_
#define _MKDIR_H_

#include "fs_entry.h"
#include "url.h"
#include "storage.h"
#include "unlink.h"
#include "consistency.h"

int fs_entry_mkdir( struct fs_core* core, char const* path, mode_t mode, uid_t user, gid_t vol );

#endif