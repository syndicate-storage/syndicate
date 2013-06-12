/*
   Copyright 2012 Jude Nelson

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

#ifndef _SYNDICATE_TEST_
#define _SYNDICATE_TEST_

#include "libsyndicate.h"
#include "syndicate.h"
#include "http-common.h"
#include "fs.h"
#include "replication.h"

#include <map>
#include <sys/types.h>
#include <sys/time.h>
#include <utime.h>

using namespace std;


#define REPLICA_TESTFILE_FILE_VERSION 123456
#define REPLICA_TESTFILE_BLOCK_ID 0
#define REPLICA_TESTFILE_BLOCK_VERSION 1
#define REPLICA_TESTFILE_PATH "/tmp/syndicate-data-1/replica-file.123456/0.1"
#define REPLICA_TESTFILE_DATA_ROOT "/tmp"
#define REPLICA_TESTFILE_FS_PATH "/replica-file"
#define REPLICA_TESTFILE_FS_FULLPATH "/replica-file.123456/0.1"

#define REPLICA_DEFAULT_CONFIG "/etc/syndicate/syndicate-replica-server.conf"

#endif
