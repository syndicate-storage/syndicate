/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _SYNDICATE_HTTPD_
#define _SYNDICATE_HTTPD_

#include "libsyndicate.h"
#include "syndicate.h"
#include "http-common.h"
#include "fs_entry.h"

#include <getopt.h>

#include <map>
#include <sys/types.h>
#include <sys/time.h>
#include <utime.h>

using namespace std;

struct httpd_GET_data {
   struct fs_file_handle* fh;
   struct syndicate_state* state;
   off_t offset;
};

struct httpd_connection_data {
   int fd;
   int err;
   ssize_t written;
};

#define HTTP_MODE "X-POSIX-Mode"

#define SYNDICATE_HTTPD_TMP "/tmp/syndicate-httpd-XXXXXX"

#endif
