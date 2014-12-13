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

#include "libsyndicate/ms/xattr.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/url.h"

// get an xattr value.
// fails with -ENOENT if the file doesn't exist or isn't readable.
int ms_client_getxattr( struct ms_client* client, uint64_t volume_id, uint64_t file_id, char const* xattr_name, char** xattr_value, size_t* xattr_value_len ) {
   
   char* getxattr_url = ms_client_getxattr_url( client->url, volume_id, file_id, xattr_name );
   ms::ms_reply reply;
   int rc = 0;
   
   rc = ms_client_read( client, getxattr_url, &reply );
   
   free( getxattr_url );
   
   if( rc != 0 ) {
      errorf("ms_client_read(getxattr %s) rc = %d\n", xattr_name, rc );
      return rc;
   }
   else {
      
      // check for the value 
      if( !reply.has_xattr_value() ) {
         errorf("MS did not reply a value for %s\n", xattr_name );
         return -ENODATA;
      }
      
      
      // get the xattr 
      char* val = strdup( reply.xattr_value().c_str() );
      *xattr_value = val;
      *xattr_value_len = reply.xattr_value().size();
      
      return 0;
   }
}

// get the list of xattrs for this file.
// fails with -ENOENT if the file doesn't exist or isn't readable
// on success, populate xattr_names with a '\0'-separated list of xattr names (size stored to xattr_names_len).
int ms_client_listxattr( struct ms_client* client, uint64_t volume_id, uint64_t file_id, char** xattr_names, size_t* xattr_names_len ) {
   
   char* listxattr_url = ms_client_listxattr_url( client->url, volume_id, file_id );
   int rc = 0;
   ms::ms_reply reply;
   
   rc = ms_client_read( client, listxattr_url, &reply );
   
   free( listxattr_url );
   
   if( rc != 0 ) {
      errorf("ms_client_read(listxattr %" PRIX64 ") rc = %d\n", file_id, rc );
      return rc;
   }
   else {
      
      // get the total size...
      size_t names_len = 0;
      for( int i = 0; i < reply.xattr_names_size(); i++ ) {
         const string& xattr_name = reply.xattr_names(i);
         names_len += xattr_name.size() + 1;
      }
      
      // get the names, separating them with '\0'
      char* names = CALLOC_LIST( char, names_len + 1 );
      off_t offset = 0;
      for( int i = 0; i < reply.xattr_names_size(); i++ ) {
         const string& xattr_name = reply.xattr_names(i);
         strcpy( names + offset, xattr_name.c_str() );
         
         offset += xattr_name.size() + 1;
      }
      
      *xattr_names = names;
      *xattr_names_len = names_len;
      
      return 0;
   }
}

// set a file's xattr.
// flags is either 0, XATTR_CREATE, or XATTR_REPLACE (see setxattr(2))
// fails with -ENOENT if the file doesn't exist or either isn't readable or writable.  Fails with -ENODATA if the semantics in flags can't be met.
int ms_client_setxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, mode_t mode, int flags ) {
   
   // sanity check...can't have both XATTR_CREATE and XATTR_REPLACE
   if( (flags & (XATTR_CREATE | XATTR_REPLACE)) == (XATTR_CREATE | XATTR_REPLACE) ) {
      return -EINVAL;
   }
   
   // generate our update
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::SETXATTR, flags, ent );
   
   // add the xattr information (these won't be free'd, so its safe to cast)
   up.xattr_name = (char*)xattr_name;
   up.xattr_value = (char*)xattr_value;
   up.xattr_value_len = xattr_value_len;
   up.xattr_owner = client->owner_id;
   up.xattr_mode = mode;
   
   return ms_client_update_rpc( client, &up );
}

// remove an xattr.
// fails if the file isn't readable or writable, or the xattr exists and it's not writable
// succeeds even if the xattr doesn't exist (i.e. idempotent)
int ms_client_removexattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name ) {
   // generate our update 
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::REMOVEXATTR, 0, ent );
   
   // add the xattr information (these won't be free'd, so its safe to cast)
   up.xattr_name = (char*)xattr_name;
   
   return ms_client_update_rpc( client, &up );
}

// change the owner of an xattr 
// fails if we don't own the attribute
int ms_client_chownxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, uint64_t new_owner ) {
   // generate our update 
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::CHOWNXATTR, 0, ent );
   
   // add the xattr information 
   up.xattr_name = (char*)xattr_name;
   up.xattr_owner = new_owner;
   
   return ms_client_update_rpc( client, &up );
}

// change the mode of an xattr 
// fails if we don't own the attribute, or if it's not writable by us
int ms_client_chmodxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, mode_t new_mode ) {
   // generate our update 
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::CHMODXATTR, 0, ent );
   
   // add the xattr information 
   up.xattr_name = (char*)xattr_name;
   up.xattr_mode = new_mode;
   
   return ms_client_update_rpc( client, &up );
}
