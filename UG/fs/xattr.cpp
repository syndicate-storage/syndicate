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

#include "xattr.h"
#include "cache.h"

// general purpose handlers...
ssize_t xattr_set_undefined( struct fs_core* core, struct fs_entry* fent, char const* buf, size_t buf_len, int flags ) {
   return -ENOTSUP;
}

int xattr_del_undefined( struct fs_core* core, struct fs_entry* fent ) {
   return -ENOTSUP;
}


// prototype xattr handlers...
static ssize_t xattr_get_cached_blocks( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len );
static ssize_t xattr_get_coordinator( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len );

static struct syndicate_xattr_handler xattr_handlers[] = {
   {SYNDICATE_XATTR_COORDINATOR,        xattr_get_coordinator,          xattr_set_undefined,    xattr_del_undefined},      // TODO: set by gateway name?
   {SYNDICATE_XATTR_CACHED_BLOCKS,      xattr_get_cached_blocks,        xattr_set_undefined,    xattr_del_undefined},
   {NULL,                               NULL,                           NULL,                   NULL}
};


// look up an xattr handler 
static struct syndicate_xattr_handler* xattr_lookup_handler( char const* name ) {
   for( int i = 0; xattr_handlers[i].name != NULL; i++ ) {
      if( strcmp( xattr_handlers[i].name, name ) == 0 ) {
         return &xattr_handlers[i];
      }
   }
   
   return NULL;
}


// get size of all of the names of our xattrs
static size_t xattr_len_all(void) {
   size_t len = 0;
   for( int i = 0; xattr_handlers[i].name != NULL; i++ ) {
      len += strlen(xattr_handlers[i].name) + 1;        // include '\0'
   }
   return len;
}


// get concatenated names of all xattrs (delimited by '\0')
static ssize_t xattr_get_names( char* buf, size_t buf_len ) {
   size_t needed_len = xattr_len_all();
   if( needed_len >= buf_len ) {
      return -ERANGE;
   }
   
   ssize_t offset = 0;
   
   for( int i = 0; xattr_handlers[i].name != NULL; i++ ) {
      sprintf( buf + offset, "%s", xattr_handlers[i].name );
      
      offset += strlen(xattr_handlers[i].name);
      
      *(buf + offset) = '\0';
      
      offset ++;
   }
   
   return offset;
}

// get cached block vector, but as a string.
// string[i] == '1' if block i is cached.
// string[i] == '0' if block i is NOT cached.
static ssize_t xattr_get_cached_blocks( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len) {
   
   struct local {
      static int xattr_stat_block( char const* block_path, void* cls ) {
         // NOTE: block_vector must be a null-terminated string, memset'ed to '0''s
         char* block_vector = (char*)cls;
         size_t num_blocks = strlen(block_vector);
         
         // which block is this?
         char* block_name = md_basename( block_path, NULL );
         
         // try to parse the ID
         char* tmp = NULL;
         int64_t id = (int64_t)strtoll( block_name, &tmp, 10 );
         if( tmp != block_name ) {
            // parsed!  This block is present
            if( (unsigned)id < num_blocks ) {
               *(block_vector + id) = '1';
            }
         }
         
         free( block_name );
         
         return 0;
      }
   };
   
   off_t num_blocks = (fent->size / core->blocking_factor) + ((fent->size % core->blocking_factor) == 0 ? 0 : 1);
   if( (unsigned)num_blocks >= buf_len + 1 ) {
      if( buf_len == 0 || buf == NULL ) {
         // size query
         return num_blocks + 1;         // NULL-terminated
      }
      else {
         // not enough space 
         return -ERANGE;
      }
   }
   
   char* cached_file_url = fs_entry_local_file_url( core, fent->file_id, fent->version );
   char* cached_file_path = GET_PATH( cached_file_url );
   
   // enough space...
   memset( buf, '0', buf_len );
   buf[buf_len - 1] = '\0';
   
   ssize_t rc = fs_entry_cache_file_blocks_apply( cached_file_path, local::xattr_stat_block, buf );
   
   free( cached_file_url );
   
   if( rc == 0 )
      rc = num_blocks;
   
   return rc;
}


// get the name of a coordinator of a file 
static ssize_t xattr_get_coordinator( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len ) {
   char* gateway_name = NULL;
   int rc = ms_client_get_gateway_name( core->ms, SYNDICATE_UG, fent->coordinator, &gateway_name );
   
   if( rc != 0 || gateway_name == NULL ) {
      // not known
      return -ENOATTR;
   }
   else {
      if( buf == NULL || buf_len == 0 ) {
         // query for size
         size_t len = strlen(gateway_name) + 1;
         free( gateway_name );
         return (ssize_t)len;
      }
      
      if( strlen(gateway_name) >= buf_len ) {
         // not big enough
         free( gateway_name );
         return -ERANGE;
      }
      
      
      size_t len = strlen(gateway_name) + 1;
      
      strcpy( buf, gateway_name );
      
      free( gateway_name );
      
      return (ssize_t)len;
   }
}


// getxattr
ssize_t fs_entry_getxattr( struct fs_core* core, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume ) {
   
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }
   
   ssize_t ret = 0;
   
   // find the xattr handler for this attribute
   struct syndicate_xattr_handler* xattr_handler = xattr_lookup_handler( name );
   if( xattr_handler == NULL ) {
      // check directly 
      string name_str( name );
      fs_entry_xattrs::iterator itr = fent->xattrs->find( name_str );
      if( itr == fent->xattrs->end() ) {
         ret = -ENOATTR;
      }
      else {
         const string& val = itr->second;
         if( val.size() > size ) {
            ret = -ERANGE;      // buffer isn't big enough
         }
         else {
            memcpy( value, val.data(), val.size() );
         }
      }
   }
   else {
      ret = (*xattr_handler->get)( core, fent, value, size );
   }
   
   fs_entry_unlock( fent );
   
   return ret;
}

// setxattr
int fs_entry_setxattr( struct fs_core* core, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume ) {
   
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, true, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }
   
   ssize_t ret = 0;
   
   // find the xattr handler for this attribute
   struct syndicate_xattr_handler* xattr_handler = xattr_lookup_handler( name );
   if( xattr_handler == NULL ) {
      // check directly
      string name_str( name );
      
      // creating the attr?  verify it doesn't exist
      if( (flags & XATTR_CREATE) ) {
         fs_entry_xattrs::iterator itr = fent->xattrs->find( name_str );
         if( itr != fent->xattrs->end() ) {
            ret = -EEXIST;
         }
      }
      
      // replacing the attr?  verify that it exists
      if( (flags & XATTR_REPLACE) ) {
         fs_entry_xattrs::iterator itr = fent->xattrs->find( name_str );
         if( itr == fent->xattrs->end() ) {
            ret = -ENOATTR;
         }
      }
      
      // are we good?
      if( ret == 0 ) {
         string value_str( value, size );
         (*fent->xattrs)[ name_str ] = value_str;
      }
   }
   else {
      ret = (*xattr_handler->set)( core, fent, value, size, flags );
   }
   
   // TODO: synchronize xattrs with MS
   
   fs_entry_unlock( fent );

   return ret;
}

// listxattr
ssize_t fs_entry_listxattr( struct fs_core* core, char const* path, char *list, size_t size, uint64_t user, uint64_t volume ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   ssize_t rc = xattr_get_names( list, size );
   
   fs_entry_unlock( fent );

   return rc;
}

// removexattr
int fs_entry_removexattr( struct fs_core* core, char const* path, char const *name, uint64_t user, uint64_t volume ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, true, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }
   
   ssize_t ret = 0;
   
   // find the xattr handler for this attribute
   struct syndicate_xattr_handler* xattr_handler = xattr_lookup_handler( name );
   if( xattr_handler == NULL ) {
      // check directly
      string name_str( name );
      
      fs_entry_xattrs::iterator itr = fent->xattrs->find( name_str );
      if( itr != fent->xattrs->end() ) {
         fent->xattrs->erase( itr );
      }
      else {
         ret = -ENOATTR;
      }
   }
   else {
      ret = (*xattr_handler->del)( core, fent );
   }
   
   // TODO: synchronize xattrs with MS
   
   fs_entry_unlock( fent );

   return ret;
}
