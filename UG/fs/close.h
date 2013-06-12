/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#ifndef _CLOSE_H_
#define _CLOSE_H_

#include "fs_entry.h"

int fs_entry_close( struct fs_core* core, struct fs_file_handle* fh );
int fs_file_handle_close( struct fs_file_handle* fh );

#endif