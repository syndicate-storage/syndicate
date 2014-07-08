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


#ifndef _SYNDICATE_HTTPD_
#define _SYNDICATE_HTTPD_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/system.h"
#include "syndicate.h"
#include "http-common.h"
#include "fs_entry.h"
#include "server.h"
#include "opts.h"

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
