/*
   Copyright 2014 The Trustees of Princeton University

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


#ifndef _MS_CLIENT_XATTR_H_
#define _MS_CLIENT_XATTR_H_

#include "libsyndicate/ms/core.h"


extern "C" {
   
// xattr API
int ms_client_getxattr( struct ms_client* client, uint64_t volume_id, uint64_t file_id, char const* xattr_name, char** xattr_value, size_t* xattr_value_len );
int ms_client_listxattr( struct ms_client* client, uint64_t volume_id, uint64_t file_id, char** xattr_names, size_t* xattr_names_len );
int ms_client_setxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, mode_t mode, int flags );
int ms_client_removexattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name );
int ms_client_chownxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, uint64_t new_owner );
int ms_client_chmodxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, mode_t new_mode );

}

#endif 