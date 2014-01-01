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
#include "syndicate.h"
#include "log.h"

#define SYNDICATE_DATA (syndicate_get_state())

typedef struct _syndicate_handle {
   int type;
   union {
      struct fs_file_handle* fh;
      struct fs_dir_handle* fdh;
   };
} syndicate_handle_t;

// NULL-terminated directory listing
typedef fs_dir_entry** syndicate_dir_listing_t;

int syndicate_getattr(const char *path, struct stat *statbuf);
int syndicate_mkdir(const char *path, mode_t mode);
int syndicate_unlink(const char *path);
int syndicate_rmdir(const char *path);
int syndicate_rename(const char *path, const char *newpath);
int syndicate_chmod(const char *path, mode_t mode);
int syndicate_chown(const char *path, uint64_t new_coordinator);
int syndicate_truncate(const char *path, off_t newsize);
int syndicate_access(const char *path, int mask);

syndicate_handle_t* syndicate_create(const char *path, mode_t mode );
syndicate_handle_t* syndicate_open(const char *path, int flags);
int syndicate_read(const char *path, char *buf, size_t size, off_t offset, syndicate_handle_t* fi);
int syndicate_write(const char *path, const char *buf, size_t size, off_t offset, syndicate_handle_t *fi);
int syndicate_flush(const char *path, syndicate_handle_t *fi);
int syndicate_close(const char *path, syndicate_handle_t *fi);
int syndicate_fsync(const char *path, int datasync, syndicate_handle_t *fi);
int syndicate_ftruncate(const char *path, off_t offset, syndicate_handle_t *fi);
int syndicate_fgetattr(const char *path, struct stat *statbuf, syndicate_handle_t *fi);

syndicate_handle_t* syndicate_opendir(const char *path);
int syndicate_readdir(const char *path, syndicate_dir_listing_t* listing, syndicate_handle_t *fi);
int syndicate_closedir(const char *path, syndicate_handle_t *fi);
void syndicate_free_dir_listing( syndicate_dir_listing_t listing );

int syndicate_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
int syndicate_getxattr(const char *path, const char *name, char *value, size_t size);
int syndicate_listxattr(const char *path, char *list, size_t size);
int syndicate_removexattr(const char *path, const char *name);

int syndicate_init_full(char const* config_file,
                        int portnum,
                        char const* ms_url,
                        char const* volume_name,
                        char const* gateway_name,
                        char const* md_username,
                        char const* md_password,
                        char const* volume_pubkey_file,
                        char const* my_key_file,
                        char const* tls_key_file,
                        char const* tls_cert_file);

int syndicate_init_client(char const* ms_url, char const* volume_name, char const* gateway_name, char const* oid_username, char const* oid_password, char const* my_key_filename, char const* volume_pubky_filename );

#endif