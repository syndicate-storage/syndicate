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
int xattr_set_undefined( struct fs_core* core, struct fs_entry* fent, char const* buf, size_t buf_len, int flags ) {
   return -ENOTSUP;
}

int xattr_del_undefined( struct fs_core* core, struct fs_entry* fent ) {
   return -ENOTSUP;
}


// prototype special xattr handlers...
static ssize_t xattr_get_cached_blocks( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len );
static ssize_t xattr_get_coordinator( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len );
static ssize_t xattr_get_read_ttl( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len );
static ssize_t xattr_get_write_ttl( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len );

static int xattr_set_read_ttl( struct fs_core* core, struct fs_entry* fent, char const* buf, size_t buf_len, int flags );
static int xattr_set_write_ttl( struct fs_core* core, struct fs_entry* fent, char const* buf, size_t buf_len, int flags );

static struct syndicate_xattr_handler xattr_handlers[] = {
   {SYNDICATE_XATTR_COORDINATOR,        xattr_get_coordinator,          xattr_set_undefined,    xattr_del_undefined},      // TODO: set coordinator by gateway name?
   {SYNDICATE_XATTR_CACHED_BLOCKS,      xattr_get_cached_blocks,        xattr_set_undefined,    xattr_del_undefined},
   {SYNDICATE_XATTR_READ_TTL,           xattr_get_read_ttl,             xattr_set_read_ttl,     xattr_del_undefined},
   {SYNDICATE_XATTR_WRITE_TTL,          xattr_get_write_ttl,            xattr_set_write_ttl,    xattr_del_undefined},
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
static ssize_t xattr_get_builtin_names( char* buf, size_t buf_len ) {
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
            if( (size_t)id < num_blocks ) {
               *(block_vector + id) = '1';
            }
         }
         
         free( block_name );
         
         return 0;
      }
   };
   
   off_t num_blocks = (fent->size / core->blocking_factor) + ((fent->size % core->blocking_factor) == 0 ? 0 : 1);
   if( (size_t)num_blocks >= buf_len + 1 ) {
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
   if( buf_len > 0 ) {
      memset( buf, '0', buf_len );
      buf[buf_len - 1] = '\0';
   }
   
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

// get the read ttl 
static ssize_t xattr_get_read_ttl( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len ) {
   uint32_t read_ttl = fent->max_read_freshness;
   
   // how many bytes needed?
   ssize_t len = 2;
   if( read_ttl > 0 )
      len = (ssize_t)(log( (double)read_ttl )) + 1;
   
   if( buf == NULL || buf_len == 0 ) {
      // size query
      return len;
   }
   
   if( (size_t)len > buf_len ) {
      // not big enough
      return -ERANGE;
   }
   
   sprintf( buf, "%u", read_ttl );
   
   return len;
}


// get the write ttl 
static ssize_t xattr_get_write_ttl( struct fs_core* core, struct fs_entry* fent, char* buf, size_t buf_len ) {
   uint32_t write_ttl = fent->max_write_freshness;
   
   // how many bytes needed?
   ssize_t len = 2;
   if( write_ttl > 0 )
      len = (ssize_t)(log( (double)write_ttl )) + 1;
   
   if( buf == NULL || buf_len == 0 ) {
      // size query
      return len;
   }
   
   if( (size_t)len > buf_len ) {
      // not big enough
      return -ERANGE;
   }
   
   sprintf( buf, "%u", write_ttl );
   
   return len;
}


// set the read ttl 
static int xattr_set_read_ttl( struct fs_core* core, struct fs_entry* fent, char const* buf, size_t buf_len, int flags ) {
   // this attribute always exists...
   if( (flags & XATTR_CREATE) )
      return -EEXIST;
   
   // parse this
   char* tmp = NULL;
   uint32_t read_ttl = (uint32_t)strtoll( buf, &tmp, 10 );
   if( tmp == buf ) {
      // invalid 
      return -EINVAL;
   }
   
   fent->max_read_freshness = read_ttl;
   return 0;
}


// set the write ttl 
static int xattr_set_write_ttl( struct fs_core* core, struct fs_entry* fent, char const* buf, size_t buf_len, int flags ) {
   // this attribute always exists...
   if( (flags & XATTR_CREATE) )
      return -EEXIST;
   
   // parse this
   char* tmp = NULL;
   uint32_t write_ttl = (uint32_t)strtoll( buf, &tmp, 10 );
   if( tmp == buf ) {
      // invalid 
      return -EINVAL;
   }
   
   fent->max_write_freshness = write_ttl;
   return 0;
}


// download an extended attribute
int fs_entry_download_xattr( struct fs_core* core, uint64_t volume, uint64_t file_id, char const* name, char** value ) {
   char* val = NULL;
   size_t val_len = 0;
   int ret = 0;
   ret = ms_client_getxattr( core->ms, volume, file_id, name, &val, &val_len );
   if( ret < 0 ) {
      errorf("ms_client_getxattr( %s ) rc = %d\n", name, ret );
      ret = -ENOATTR;
   }
   else {
      *value = val;
      
      ret = (int)val_len;
   }
   
   return ret;
}


// cache an xattr.
static int fs_entry_cache_xattr( struct fs_core* core, char const* fs_path, uint64_t user, uint64_t volume, char const* name, char const* value, size_t value_len, int64_t cur_xattr_nonce ) {
   // write-lock so we can modify
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, user, volume, true, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   fs_entry_put_cached_xattr( fent, name, value, value_len, cur_xattr_nonce );
   
   fs_entry_unlock( fent );
   
   return 0;
}


// get the xattr, given a locked fs_entry.
// unlock the fent as soon as possible
// check the cache, and then check the MS
ssize_t fs_entry_do_getxattr( struct fs_core* core, struct fs_entry* fent, char const* name, char** value, size_t* value_len, int* _cache_status, bool unlock_before_download ) {
   // check the cache
   ssize_t ret = 0;
   char* val = NULL;
   size_t vallen = 0;
   
   int cache_status = fs_entry_get_cached_xattr( fent, name, &val, &vallen );
   
   uint64_t file_id = fent->file_id;
   uint64_t volume = fent->volume;
   
   if( unlock_before_download ) {
      // don't need fent to be around anymore...
      fs_entry_unlock( fent );
   }
   
   if( cache_status < 0 ) {
      // cache miss 
      ret = (ssize_t)fs_entry_download_xattr( core, file_id, volume, name, &val );
   }
   else {
      // cache hit
      ret = vallen;
   }
   
   if( ret >= 0 ) {
      // success!
      *value = val;
      *value_len = vallen;
      *_cache_status = cache_status;
   }
   
   return ret;
}


static ssize_t fs_entry_do_getxattr_and_unlock( struct fs_core* core, struct fs_entry* fent, char const* name, char** value, size_t* value_len, int* _cache_status ) {
   return fs_entry_do_getxattr( core, fent, name, value, value_len, _cache_status, true );
}


// getxattr
ssize_t fs_entry_getxattr( struct fs_core* core, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume ) {
   
   // revalidate this path--make sure the ent exists
   int revalidate_rc = fs_entry_revalidate_path( core, core->volume, path );
   if( revalidate_rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, revalidate_rc );
      return revalidate_rc;
   }
   
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }
   
   int64_t cur_xattr_nonce = fent->xattr_nonce;
   
   ssize_t ret = 0;
   char* val = NULL;
   size_t vallen = 0;
   int cache_status = 0;
   
   // find the xattr handler for this attribute
   struct syndicate_xattr_handler* xattr_handler = xattr_lookup_handler( name );
   if( xattr_handler == NULL ) {
      
      // NOTE: this unlocks fent
      ret = fs_entry_do_getxattr_and_unlock( core, fent, name, &val, &vallen, &cache_status );
      
      if( ret >= 0 ) {
         // success!
         if( value != NULL && size > 0 ) {
            // wanted the attribute, not just the size
            if( size < (size_t)ret ) {
               ret = -ERANGE;
            }
            else {
               // cache this?
               if( cache_status < 0 ) {
                  
                  // not cached...
                  err = fs_entry_cache_xattr( core, path, user, volume, name, val, ret, cur_xattr_nonce );
                  if( err < 0 ) {
                     errorf("fs_entry_cache_xattr(%s, %s) rc = %d\n", path, name, err );
                     free( val );
                     return err;
                  }
               }
               
               memcpy( value, val, ret );
            }
         }
         
         free( val );
      }
      
      else {
         errorf("fs_entry_do_getxattr(%s, %s) rc = %zd\n", path, name, ret );
      }
   }
   else {
      // built-in handler
      ret = (*xattr_handler->get)( core, fent, value, size );
      fs_entry_unlock( fent );
   }
   
   return ret;
}

// setxattr
int fs_entry_setxattr( struct fs_core* core, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume ) {
   
   if( core->gateway == GATEWAY_ANON ) {
      errorf("%s", "Setting extended attributes is forbidden for anonymous gateways\n");
      return -EPERM;
   }
   
   // bring the metadata up to date
   int revalidate_rc = fs_entry_revalidate_path( core, core->volume, path );
   if( revalidate_rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, revalidate_rc );
      return revalidate_rc;
   }
   
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
      
      struct md_entry ent;
      fs_entry_to_md_entry( core, &ent, fent, 0, NULL );        // parent information not needed
      
      ret = ms_client_setxattr( core->ms, &ent, name, value, size, flags );
      if( ret < 0 ) {
         errorf("ms_client_setxattr( %s %s ) rc = %zd\n", path, name, ret );
      }
      else {
         // cache this 
         fs_entry_put_cached_xattr( fent, name, value, size, fent->xattr_nonce );
      }
      
      md_entry_free( &ent );
   
   }
   else {
      // built-in handler
      ret = (*xattr_handler->set)( core, fent, value, size, flags );
   }
   
   fs_entry_unlock( fent );

   return ret;
}


// get an xattr, or set an xattr if not present.  There will be only one "set" winner globally, but "get" might return nothing (since the get and set do not occur as an atomic action)
// Meant for use by UG closures.
// fent must be at least read-locked
int fs_entry_get_or_set_xattr( struct fs_core* core, struct fs_entry* fent, char const* name, char const* proposed_value, size_t proposed_value_len, char** value, size_t* value_len ) {
   
   ssize_t ret = 0;
   int cache_status = 0;
   char* val = NULL;
   size_t vallen = 0;
   
   int64_t cur_xattr_nonce = fent->xattr_nonce;
   
   // find the xattr handler for this attribute
   struct syndicate_xattr_handler* xattr_handler = xattr_lookup_handler( name );
   if( xattr_handler == NULL ) {
      
      // attempt to set, but fail if we don't create.
      bool try_get = false;
      
      struct md_entry ent;
      fs_entry_to_md_entry( core, &ent, fent, 0, NULL );        // parent information not needed
      
      ret = ms_client_setxattr( core->ms, &ent, name, proposed_value, proposed_value_len, XATTR_CREATE );
      if( ret < 0 ) {
         errorf("ms_client_setxattr( %" PRIX64 " %s ) rc = %zd\n", fent->file_id, name, ret );
         
         if( ret == -EEXIST ) {
            // attr already existed.  Get it
            try_get = true;
         }
      }
      else {
         // set successfully!
         // cache this 
         fs_entry_put_cached_xattr( fent, name, proposed_value, proposed_value_len, cur_xattr_nonce );
      }
      
      md_entry_free( &ent );
   
      if( try_get ) {
         
         ret = fs_entry_do_getxattr( core, fent, name, &val, &vallen, &cache_status, false );
            
         if( ret >= 0 ) {
            // success!
            // cache this?
            if( cache_status < 0 ) {
               fs_entry_put_cached_xattr( fent, name, val, ret, cur_xattr_nonce );
            }
            
            *value = val;
            *value_len = (size_t)ret;
         }
      }
   }
   else {
      // built-in handler.
      while( true ) {
         ssize_t required_size = (*xattr_handler->get)( core, fent, NULL, 0 );
         
         char* buf = CALLOC_LIST( char, required_size + 1 );
         
         ret = (*xattr_handler->get)( core, fent, buf, required_size );
         if( ret == -ERANGE ) {
            // try again 
            free( buf );
            continue;
         }
         
         *value = buf;
         *value_len = (size_t)ret;
         break;
      }
      
      fs_entry_unlock( fent );
   }
   
   return ret;
}
   

// listxattr
ssize_t fs_entry_listxattr( struct fs_core* core, char const* path, char *list, size_t size, uint64_t user, uint64_t volume ) {
   // bring the metadata up to date
   int revalidate_rc = fs_entry_revalidate_path( core, core->volume, path );
   if( revalidate_rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, revalidate_rc );
      return revalidate_rc;
   }
   
   // resolve the entry
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }
   
   // copy these values, so we can unlock fent
   uint64_t file_id = fent->file_id;
   uint64_t volume_id = fent->volume;
   
   fs_entry_unlock( fent );
   
   char* remote_xattr_names = NULL;
   size_t remote_xattr_names_len = 0;

   // get data from the MS
   int remote_rc = ms_client_listxattr( core->ms, volume_id, file_id, &remote_xattr_names, &remote_xattr_names_len );
   
   if( remote_rc != 0 ) {
      errorf("ms_client_listxattr(%s %" PRIX64 ") rc = %d\n", path, file_id, remote_rc );
      
      return (ssize_t)remote_rc;
   }
   
   ssize_t rc = 0;
   
   // get the built-in attributes, and copy them into list
   ssize_t builtin_len = xattr_get_builtin_names( list, size );

   // range check 
   if( remote_xattr_names_len + builtin_len >= size ) {
      free( remote_xattr_names );
      return -ERANGE;
   }
   
   // combine them 
   memcpy( list + builtin_len, remote_xattr_names, remote_xattr_names_len );
   rc = builtin_len + remote_xattr_names_len;
   
   free( remote_xattr_names );
   
   return rc;
}


// removexattr
int fs_entry_removexattr( struct fs_core* core, char const* path, char const *name, uint64_t user, uint64_t volume ) {
   if( core->gateway == GATEWAY_ANON ) {
      errorf("%s", "Removing extended attributes is forbidden for anonymous gateways\n");
      return -EPERM;
   }
   
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
   
      struct md_entry ent;
      fs_entry_to_md_entry( core, &ent, fent, 0, NULL );        // parent information not needed
   
      ret = ms_client_removexattr( core->ms, &ent, name );
      if( ret < 0 ) {
         errorf("ms_client_removexattr( %s ) rc = %zd\n", name, ret );
      }
      
      md_entry_free( &ent );
   }
   else {
      ret = (*xattr_handler->del)( core, fent );
   }
   
   if( ret == 0 ) {
      // successfully removed; uncache 
      fs_entry_evict_cached_xattr( fent, name );
   }
   
   fs_entry_unlock( fent );

   return ret;
}
