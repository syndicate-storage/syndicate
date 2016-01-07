/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _RG_DISK_H_
#define _RG_DISK_H_

#include "libgateway.h"

struct disk_context {
   char* path;
   FILE* fh;
   ssize_t num_processed;
};

#endif
