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

#include <map>
#include <string.h>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <ftw.h>

#include "directory_monitor.h"

using namespace std;

static map<string, struct filestat_cache*> cached_entry_map;
static map<string, struct filestat_cache*> current_entry_map;

static int check_current_entries(const char* parentDir);
static int current_entry_found(const char *fpath, const struct stat *sb, int tflag, struct FTW *ftwbuf);

static struct filestat_cache* make_stat_cache(const char *fpath, const struct stat *sb, int tflag);

static void add_cached_entry(const char *fpath, struct filestat_cache *pentry);
static void add_cached_entry(string spath, struct filestat_cache *pentry);
static void clear_cached_entries();

static void add_current_entry(const char *fpath, struct filestat_cache *pentry);
static void add_current_entry(string spath, struct filestat_cache *pentry);
static void clear_current_entries();

static void make_all_current_entries_cached();
static bool is_same_entry(struct filestat_cache *pentry1, struct filestat_cache *pentry2);

static int check_current_entries(const char* root) {
    int flags = FTW_PHYS;
    
    clear_current_entries();
    int root_len = strlen(root); 
    if ( root[root_len - 1] == '/') {
	   root_len--;
    }
    
    if (nftw(root, current_entry_found, MAX_NUM_DIRECTORY_OPENED, flags) == -1) {
	return -1;
    }
    return 0;
}

static struct filestat_cache* make_stat_cache(const char *fpath, const struct stat *sb, int tflag) {
    struct filestat_cache *pcache = (struct filestat_cache*)malloc(sizeof(struct filestat_cache));
    
    memset(pcache, 0, sizeof(struct filestat_cache));
    
    pcache->fpath = strdup(fpath);
    pcache->sb = (struct stat*)malloc(sizeof(struct stat));
    memcpy(pcache->sb, sb, sizeof(struct stat));
    pcache->tflag = tflag;
    
    return pcache;
}

static void add_cached_entry(const char *fpath, struct filestat_cache *pentry) {
    string spath(fpath);
    cached_entry_map[spath] = pentry;
}

static void add_cached_entry(string spath, struct filestat_cache *pentry) {
    cached_entry_map[spath] = pentry;
}

static void clear_cached_entries() {
    std::map<string, struct filestat_cache*>::iterator iter;
    for(iter=cached_entry_map.begin();iter!=cached_entry_map.end();iter++) {
        // free all
        if(iter->second != NULL) {
            if(iter->second->fpath != NULL) {
                free(iter->second->fpath);
            }

            if(iter->second->sb != NULL) {
                free(iter->second->sb);
            }
            
            free(iter->second);
        }
    }
    cached_entry_map.clear();
}

static void add_current_entry(const char *fpath, struct filestat_cache *pentry) {
    string spath(fpath);
    current_entry_map[spath] = pentry;
}

static void add_current_entry(string spath, struct filestat_cache *pentry) {
    current_entry_map[spath] = pentry;
}

static void clear_current_entries() {
    std::map<string, struct filestat_cache*>::iterator iter;
    for(iter=current_entry_map.begin();iter!=current_entry_map.end();iter++) {
        // free all
        if(iter->second != NULL) {
            if(iter->second->fpath != NULL) {
                free(iter->second->fpath);
            }

            if(iter->second->sb != NULL) {
                free(iter->second->sb);
            }
            
            free(iter->second);
        }
    }
    current_entry_map.clear();
}

static void make_all_current_entries_cached() {
    clear_cached_entries();
    std::map<string, struct filestat_cache*>::iterator iter;
    for(iter=current_entry_map.begin();iter!=current_entry_map.end();iter++) {
        add_cached_entry(iter->first, iter->second);
    }
    // must not free entries.
    current_entry_map.clear();
}

static int current_entry_found(const char *fpath, const struct stat *sb, int tflag, struct FTW *ftwbuf) {
    if(tflag == FTW_D || tflag == FTW_F) {
        struct filestat_cache *pcache = make_stat_cache(fpath, sb, tflag);
        add_current_entry(fpath, pcache);
    }
    
    return 0;
}

static bool is_same_entry(struct filestat_cache *pentry1, struct filestat_cache *pentry2) {
    if(pentry1 == NULL || pentry2 == NULL) {
        return false;
    }
    
    if(pentry1->sb != NULL && pentry2->sb != NULL) {
        if((pentry1->sb->st_size == pentry2->sb->st_size) 
                && (pentry1->sb->st_mtim.tv_sec == pentry2->sb->st_mtim.tv_sec)) {
            return true;
        }
    }
    return false;
}

void init_monitor() {
    clear_cached_entries();
    clear_current_entries();
}

int check_modified(const char *fpath, PFN_DIR_ENTRY_MODIFIED_HANDLER handler) {
    if(fpath == NULL) {
        return -1;
    }
    
    check_current_entries(fpath);
    
    // find new entry
    std::map<string, struct filestat_cache*>::iterator current_map_iter;
    for(current_map_iter=current_entry_map.begin();current_map_iter!=current_entry_map.end();current_map_iter++) {
        std::map<string, struct filestat_cache*>::iterator found = cached_entry_map.find(current_map_iter->first);
        if(found == cached_entry_map.end()) {
            // not found
            if(handler != NULL) {
                handler(DIR_ENTRY_MODIFIED_FLAG_NEW, current_map_iter->first, current_map_iter->second);
            }
        } else {
            if(!is_same_entry(found->second, current_map_iter->second)) {
                // modified
                if(handler != NULL) {
                    handler(DIR_ENTRY_MODIFIED_FLAG_MODIFIED, current_map_iter->first, current_map_iter->second);
                }
            }
        }
    }
    
    // find stale entry
    std::map<string, struct filestat_cache*>::iterator cached_map_iter;
    for(cached_map_iter=cached_entry_map.begin();cached_map_iter!=cached_entry_map.end();cached_map_iter++) {
        std::map<string, struct filestat_cache*>::iterator found = current_entry_map.find(cached_map_iter->first);
        if(found == current_entry_map.end()) {
            // not found
            if(handler != NULL) {
                handler(DIR_ENTRY_MODIFIED_FLAG_REMOVED, cached_map_iter->first, cached_map_iter->second);
            }
        }
    }
    
    make_all_current_entries_cached();
    return 0;
}

