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

// client library API, for when you make a Syndicate-powered app

#ifndef _SYNDICATE_CLIENT_H_
#define _SYNDICATE_CLIENT_H_

#include "libsyndicate/libsyndicate.h"
#include "fs_entry.h"
#include "log.h"
#include "opts.h"
   
// file handle wrapper 
typedef struct _syndicate_handle {
   int type;
   off_t offset;
   union {
      struct fs_file_handle* fh;
      struct fs_dir_handle* fdh;
   };
} syndicate_handle_t;

// NULL-terminated directory listing
typedef fs_dir_entry** syndicate_dir_listing_t;

extern "C" {

int syndicate_getattr(struct syndicate_state* state, const char *path, struct stat *statbuf);
int syndicate_mkdir(struct syndicate_state* state, const char *path, mode_t mode);
int syndicate_unlink(struct syndicate_state* state, const char *path);
int syndicate_rmdir(struct syndicate_state* state, const char *path);
int syndicate_rename(struct syndicate_state* state, const char *path, const char *newpath);
int syndicate_chmod(struct syndicate_state* state, const char *path, mode_t mode);
int syndicate_chown(struct syndicate_state* state, const char *path, uint64_t new_coordinator);
int syndicate_truncate(struct syndicate_state* state, const char *path, off_t newsize);
int syndicate_access(struct syndicate_state* state, const char *path, int mask);

syndicate_handle_t* syndicate_create(struct syndicate_state* state, const char *path, mode_t mode, int* rc );
syndicate_handle_t* syndicate_open(struct syndicate_state* state, const char *path, int flags, int* rc);
int syndicate_read(struct syndicate_state* state, char *buf, size_t size, syndicate_handle_t* fi);
int syndicate_write(struct syndicate_state* state, const char *buf, size_t size, syndicate_handle_t *fi);
off_t syndicate_seek(syndicate_handle_t* fi, off_t pos, int whence);
int syndicate_flush(struct syndicate_state* state, syndicate_handle_t *fi);
int syndicate_close(struct syndicate_state* state, syndicate_handle_t *fi);
int syndicate_fsync(struct syndicate_state* state, int datasync, syndicate_handle_t *fi);
int syndicate_ftruncate(struct syndicate_state* state, off_t offset, syndicate_handle_t *fi);
int syndicate_fgetattr(struct syndicate_state* state, struct stat *statbuf, syndicate_handle_t *fi);

syndicate_handle_t* syndicate_opendir(struct syndicate_state* state, const char *path, int* rc);
int syndicate_readdir(struct syndicate_state* state, syndicate_dir_listing_t* listing, syndicate_handle_t *fi);
int syndicate_closedir(struct syndicate_state* state, syndicate_handle_t *fi);
void syndicate_free_dir_listing( syndicate_dir_listing_t listing );

int syndicate_setxattr(struct syndicate_state* state, const char *path, const char *name, const char *value, size_t size, int flags);
int syndicate_getxattr(struct syndicate_state* state, const char *path, const char *name, char *value, size_t size);
int syndicate_listxattr(struct syndicate_state* state, const char *path, char *list, size_t size);
int syndicate_removexattr(struct syndicate_state* state, const char *path, const char *name);

int syndicate_client_init( struct syndicate_state* state, struct syndicate_opts* opts );

int syndicate_client_shutdown( struct syndicate_state* state, int wait_replicas );

}

#endif