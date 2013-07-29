/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _MKFS_H_
#define _MKFS_H_

#include "fs_entry.h"

int fs_entry_mkfs( struct fs_core* core, char const* root, uint64_t user, uint64_t vol );

#endif