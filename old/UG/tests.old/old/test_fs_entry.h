/*
   Copyright 2011 Jude Nelson

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

#ifndef _TEST_FS_ENTRY_
#define _TEST_FS_ENTRY_

#include <limits.h>

#include "fs_entry.h"
#include "libsyndicate.h"

#define NUM_THREADS 10

struct test_thread_args {
   int id;
   struct fs_core* fs;
};

#define TEST_DIR "test_fs_entry/"

#define LOCAL_FILE "local.html"
#define LOCAL_FILE2 "local2.html"
#define LOCAL_DIR "localdir"

#endif