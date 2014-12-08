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

#ifndef DIRECTORY_MONITOR_H
#define	DIRECTORY_MONITOR_H

#include <string>
#include <stdlib.h>

#define MAX_NUM_DIRECTORY_OPENED        20

struct filestat_cache {
    char        *fpath;
    struct stat *sb;
    // tflag == FTW_D : directory
    // tflag == FTW_F : file
    int         tflag;
};

#define DIR_ENTRY_MODIFIED_FLAG_NEW         0
#define DIR_ENTRY_MODIFIED_FLAG_MODIFIED    1
#define DIR_ENTRY_MODIFIED_FLAG_REMOVED     2

typedef void (*PFN_DIR_ENTRY_MODIFIED_HANDLER)(int flag, const char *fpath, struct filestat_cache *cache);

void init_monitor();
void uninit_monitor();
int check_modified(const char *fpath, PFN_DIR_ENTRY_MODIFIED_HANDLER handler);

#endif	/* DIRECTORY_MONITOR_H */

