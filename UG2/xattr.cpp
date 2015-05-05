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

#include "consistency.h"
#include "core.h"
#include "xattr.h"

// general purpose handlers...
int UG_xattr_set_undefined( struct fskit_core* core, struct fskit_entry* fent, char const* name, char const* buf, size_t buf_len, int flags ) {
   return -ENOTSUP;
}

int UG_xattr_del_undefined( struct fskit_core* core, struct fskit_entry* fent, char const* name ) {
   return -ENOTSUP;
}


// prototype special xattr handlers...
static ssize_t UG_xattr_get_cached_blocks( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len );
static ssize_t UG_xattr_get_cached_file_path( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len );
static ssize_t UG_xattr_get_coordinator( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len );
static ssize_t UG_xattr_get_read_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len );
static ssize_t UG_xattr_get_write_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len );

static int UG_xattr_set_read_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char const* buf, size_t buf_len, int flags );
static int UG_xattr_set_write_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char const* buf, size_t buf_len, int flags );

// default xattr handlers for built-in xattrs
static struct UG_xattr_handler_t xattr_handlers[] = {
   {UG_XATTR_COORDINATOR,        UG_xattr_get_coordinator,          UG_xattr_set_undefined,    UG_xattr_del_undefined},      // TODO: set coordinator by gateway name?
   {UG_XATTR_CACHED_BLOCKS,      UG_xattr_get_cached_blocks,        UG_xattr_set_undefined,    UG_xattr_del_undefined},
   {UG_XATTR_CACHED_FILE_PATH,   UG_xattr_get_cached_file_path,     UG_xattr_set_undefined,    UG_xattr_del_undefined},
   {UG_XATTR_READ_TTL,           UG_xattr_get_read_ttl,             UG_xattr_set_read_ttl,     UG_xattr_del_undefined},
   {UG_XATTR_WRITE_TTL,          UG_xattr_get_write_ttl,            UG_xattr_set_write_ttl,    UG_xattr_del_undefined},
   {NULL,                        NULL,                              NULL,                      NULL}
};


// look up an xattr handler for a given attribute name
// return a pointer to the handler on success
// return NULL if not found.
static struct UG_xattr_handler_t* UG_xattr_lookup_handler( char const* name ) {
   
   for( int i = 0; xattr_handlers[i].name != NULL; i++ ) {
      
      if( strcmp( xattr_handlers[i].name, name ) == 0 ) {
         
         return &xattr_handlers[i];
      }
   }
   
   return NULL;
}


// get size of all of the names of our xattrs
// always succeeds
static size_t UG_xattr_len_all( void ) {
   
   size_t len = 0;
   
   for( int i = 0; xattr_handlers[i].name != NULL; i++ ) {
      
      len += strlen(xattr_handlers[i].name) + 1;        // include '\0'
   }
   
   return len;
}


// get concatenated names of all xattrs (delimited by '\0')
// fill in buf with the names, if it is long enough (buf_len)
// return the number of bytes copied on success
// return -ERANGE if the buffer is not big enough.
static ssize_t UG_xattr_get_builtin_names( char* buf, size_t buf_len ) {
   
   size_t needed_len = UG_xattr_len_all();
   
   if( needed_len > buf_len ) {
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
// return the length of the buffer on success, and fill in *buf with the buffer if *buf is not NULL
// return -ENOMEM if OOM
// return -ERANGE if *buf is not NULL but buf_len is not long enough to hold the block vector
// NOTE: fent must be at least read-locked
static ssize_t UG_xattr_get_cached_blocks( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len) {
   
   struct local {
      
      // callback to fskit_entry_resolve_path_cls
      // *cls is a pointer to the null-terminated block vector we're filling in
      // returns 0 on success
      // return -ENOMEM on OOM
      static int xattr_stat_block( char const* block_path, void* cls ) {
         
         // NOTE: block_vector must be a null-terminated string, memset'ed to '0''s
         char* block_vector = (char*)cls;
         size_t num_blocks = strlen(block_vector);
         
         // which block is this?
         char* block_name = md_basename( block_path, NULL );
         if( block_name == NULL ) {
            
            return -ENOMEM;
         }
         
         // try to parse the ID
         int64_t id = 0;
         int rc = sscanf( block_name, "%" PRId64, &id );
         if( rc == 1 ) {
            
            // parsed!  This block is present
            if( (size_t)id < num_blocks ) {
               *(block_vector + id) = '1';
            }
         }
         
         free( block_name );
         
         return 0;
      }
   };
   
   struct SG_gateway* gateway = (struct SG_gateway*)fskit_core_get_user_data( core );
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   off_t num_blocks = (fent->size / block_size) + ((fent->size % block_size) == 0 ? 0 : 1);
   
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
   
   char* cached_file_path = NULL;
   ssize_t rc = 0;
   
   char* cached_file_url = md_url_local_file_url( conf->data_root, volume_id, UG_inode_file_id( *inode ), UG_inode_file_version( *inode ) );
   if( cached_file_url == NULL ) {
      
      return -ENOMEM;
   }
   
   cached_file_path = SG_URL_LOCAL_PATH( cached_file_url );
   
   // enough space...
   if( buf_len > 0 ) {
      
      memset( buf, '0', buf_len );
      buf[buf_len - 1] = '\0';
   }
   
   rc = md_cache_file_blocks_apply( cached_file_path, local::xattr_stat_block, buf );
   
   free( cached_file_url );
   
   if( rc == 0 ) {
      
      rc = num_blocks;
   }
   else if( rc == -ENOENT ) {
      
      // no cached data--all 0's
      SG_debug("No data cached for %" PRIX64 ".%" PRId64 "\n", UG_inode_file_id( *inode ), UG_inode_file_version( *inode ) );
      
      memset( buf, '0', buf_len );
      buf[buf_len - 1] = '\0';
      
      rc = num_blocks;
   }
   
   return rc;
}


// get cached file path.
// fill it in in *buf
// return the length of the path (including the '\0') on success, and fill in *buf if it is not NULL
// return -ERANGE if the buf is not NULL and the buffer is not long enough 
// return -ENOMEM on OOM
// NOTE: fent must be read-locked
static ssize_t UG_xattr_get_cached_file_path( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len) {
   
   char* cached_file_path = NULL;
   size_t len = 0;
   
   struct SG_gateway* gateway = (struct SG_gateway*)fskit_core_get_user_data( core );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   char* cached_file_url = md_url_local_file_url( conf->data_root, volume_id, UG_inode_file_id( *inode ), UG_inode_file_version( *inode ) );
   if( cached_file_url == NULL ) {
      
      return -ENOMEM;
   }

   cached_file_path = SG_URL_LOCAL_PATH( cached_file_url );
   
   len = strlen(cached_file_path);
   
   if( buf_len == 0 || buf == NULL ) {
      
      // size query
      free( cached_file_url );
      return len + 1;         // NULL-terminated
   }

   if( (size_t)len >= buf_len ) {
      
      // not enough space 
      free( cached_file_url );
      return -ERANGE;
   }
   
   // enough space...
   if( buf_len > 0 ) {
      
      strcpy( buf, cached_file_path );
   }
   
   free( cached_file_url );
   
   return len + 1;
}


// get the name of a coordinator of a file 
// return the length of the coordinator's name (plus the '\0'), and write it to *buf if buf is not NULL
// return -ERANGE if *buf is not NULL but not long enough 
// return -ENOATTR if the coordinator is not known to us (e.g. we're refreshing our cert bundle)
// return -ENOMEM if OOM
// NOTE: fent must be read-locked
static ssize_t UG_xattr_get_coordinator( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len ) {
   
   int rc = 0;
   char* gateway_name = NULL;
   
   struct SG_gateway* gateway = (struct SG_gateway*)fskit_core_get_user_data( core );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   rc = ms_client_get_gateway_name( ms, UG_inode_coordinator_id( *inode ), &gateway_name );
   
   if( rc != 0 || gateway_name == NULL ) {
      
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


// get the read ttl as a string
// return the string length (plus '\0') on success, and write the string to *buf if buf is not NULL
// return -ERANGE if buf is not NULL but not long enough 
// NOTE: fent must be read-locked
static ssize_t UG_xattr_get_read_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len ) {
   
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   uint32_t read_ttl = UG_inode_max_read_freshness( *inode );
   
   // how many bytes needed?
   ssize_t len = 2;
   if( read_ttl > 0 ) {
      len = (ssize_t)(log( (double)read_ttl )) + 1;
   }
   
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
// return the string length (plus '\0') on success, and write the string to *buf if buf is not NULL
// return -ERANGE if buf is not NULL, but not long enough 
static ssize_t UG_xattr_get_write_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char* buf, size_t buf_len ) {
   
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   uint32_t write_ttl = UG_inode_max_write_freshness( *inode );
   
   // how many bytes needed?
   ssize_t len = 2;
   if( write_ttl > 0 ) {
      
      len = (ssize_t)(log( (double)write_ttl )) + 1;
   }
   
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
// return 0 on success
// return -EEXIST if the caller specified XATTR_CREATE--this attribute is built-in and always exists 
// return -EINVAL if we couldn't parse the buffer
// NOTE: fent must be write-locked
static int UG_xattr_set_read_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char const* buf, size_t buf_len, int flags ) {
   
   // this attribute always exists...
   if( (flags & XATTR_CREATE) ) {
      return -EEXIST;
   }
   
   // parse this
   struct UG_inode* inode = NULL;
   char* tmp = NULL;
   uint32_t read_ttl = (uint32_t)strtoll( buf, &tmp, 10 );
   
   if( tmp == buf || *tmp != '\0' ) {
      // invalid 
      return -EINVAL;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   UG_inode_set_max_read_freshness( inode, read_ttl );
   
   return 0;
}


// set the write ttl 
// return 0 on success 
// return -EEXIST if the caller specified XATTR_CREAT--this attribute is built-in and always exists 
// return -EINVAL if we couldn't parse the buffer 
// NOTE: fent must be write-locked
static int UG_xattr_set_write_ttl( struct fskit_core* core, struct fskit_entry* fent, char const* name, char const* buf, size_t buf_len, int flags ) {
   
   // this attribute always exists...
   if( (flags & XATTR_CREATE) ) {
      return -EEXIST;
   }
   
   // parse this
   struct UG_inode* inode = NULL;
   char* tmp = NULL;
   uint32_t write_ttl = (uint32_t)strtoll( buf, &tmp, 10 );
   
   if( tmp == buf || *tmp != '\0' ) {
      
      // invalid 
      return -EINVAL;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   UG_inode_set_max_write_freshness( inode, write_ttl );
   
   return 0;
}


// go and get an xattr from the MS
// return the length of *value on success, and allocate *value and copy the xattr data into it.
// return -ENOMEM if out of memory
// return -ENOENT if the file doesn't exist
// return -EACCES if we're not allowed to read the file
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
// return other -errno on socket- and recv-related errors
int UG_download_xattr( struct SG_gateway* gateway, uint64_t volume, uint64_t file_id, char const* name, char** value ) {
   
   char* val = NULL;
   size_t val_len = 0;
   int ret = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   ret = ms_client_getxattr( ms, volume, file_id, name, &val, &val_len );
   if( ret < 0 ) {
      
      SG_error("ms_client_getxattr( %" PRIX64 " %s ) rc = %d\n", file_id, name, ret );
      
      if( ret == -404 ) {
         // no such file 
         ret = -ENOENT;
      }
      
      else {
         // no such attr/no data
         ret = -ENOATTR;
      }
   }
   else {
      
      *value = val;
      ret = (int)val_len;
   }
   
   return ret;
}


// fgetxattr(2), but with the option to unlock the inode during the network I/O.  If so, it will be ref'ed, unlocked, re-locked, and unref'ed (the xattr nonce will be preserved across the lock to ensure coherency)
// regardless, this method either uses a builtin getxattr handler, or downloads the xattr from the MS and caches it locally.
// return the length of the xattr, and allocate *value to contain the value
// return -ENOMEM if out of memory
// return -ERANGE if *value is not NULL but the xattr is too big to fit (the xattr will be cached locally nevertheless, so a subsequent fgetxattr will hit the cache)
// return -ENOENT if the file doesn't exist
// return -EACCES if we're not allowed to read the file
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
// return other -errno on socket- and recv-related errors
// NOTE: fent must be write-locked
ssize_t UG_fgetxattr_ex( struct SG_gateway* gateway, char const* path, struct fskit_entry* fent, char const *name, char *value, size_t size, uint64_t user, uint64_t volume, bool do_unlock ) {
   
   int rc = 0;
   uint64_t file_id = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct UG_xattr_handler_t* xattr_handler = UG_xattr_lookup_handler( name );
   char* value_buf = NULL;
   ssize_t value_buf_len = 0;
   
   // built-in handler?
   if( xattr_handler != NULL ) {
      
      rc = (*xattr_handler->get)( fs, fent, name, value, size );
      return rc;
   }
   
   // cached locally?
   file_id =  fskit_entry_get_file_id( fent );
   
   rc = fskit_fgetxattr( fs, fent, name, value, size );
   if( rc < 0 && rc != -ENOATTR && rc != -ERANGE ) {
      
      // error besides 'not found' or 'buffer not big enough'
      return rc;
   }
   
   if( do_unlock ) {
      
      // keep this entry around, but we don't really need it to remain locked while we get the xattr
      fskit_entry_ref_entry( fent );
      fskit_entry_unlock( fent );
   }
   
   // check on the MS
   value_buf_len = UG_download_xattr( gateway, volume, file_id, name, &value_buf );
   if( value_buf_len < 0 ) {
      
      SG_error("UG_download_xattr('%s'.'%s') rc = %d\n", path, name, (int)value_buf_len );
      
      if( do_unlock ) {
         fskit_entry_unref( fs, path, fent );
      }
      
      return rc;
   }
   
   if( do_unlock ) {
      fskit_entry_wlock( fent );
   }
   
   rc = 0;
   if( fskit_fgetxattr( fs, fent, name, NULL, 0 ) == -ENOATTR ) {
      
      // cache it, if we didn't receive one from the client intermittently
      rc = fskit_fsetxattr( fs, fent, name, value_buf, value_buf_len, 0 );
      if( rc < 0 ) {
         
         SG_warn("fskit_fsetxattr( %" PRIX64 ".'%s' ) rc = %d\n", fskit_entry_get_file_id( fent ), name, rc );
         rc = 0;
      }
   }
   
   SG_safe_free( value_buf );
   
   if( rc >= 0 ) {
         
      // now that we've revalidated the xattr, get it.
      rc = fskit_fgetxattr( fs, fent, name, value, size );
   }
   
   if( do_unlock ) {
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, path, fent );
   }
   
   return rc;
}

// getxattr(2)
// return -ENOMEM on OOM 
// return -ENOENT if the file doesn't exist
// return -EACCES if we're not allowed to read the file or the attribute
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
ssize_t UG_getxattr( struct SG_gateway* gateway, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume ) {
   
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   // revalidate...
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // look up...
   struct fskit_entry* fent = fskit_entry_resolve_path( fs, path, user, volume, true, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   // get the xattr, and cache it locally if need be
   rc = UG_fgetxattr_ex( gateway, path, fent, name, value, size, user, volume, true );
   
   fskit_entry_unlock( fent );
   
   return rc;
}


// setxattr, with xattr modes
// return the length of the value written on success
// return -ENOMEM on OOM 
// return -ENOENT if the file doesn't exist
// return -EEXIST if the XATTR_CREATE flag was set but the attribute existed 
// return -ENOATTR if the XATTR_REPLACE flag was set but the attribute did not exist
// return -EACCES if we're not allowed to write to the file
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
int UG_setxattr_ex( struct SG_gateway* gateway, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume, mode_t mode ) {
   
   int rc = 0;
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct UG_xattr_handler_t* xattr_handler = UG_xattr_lookup_handler( name );
   struct md_entry inode_data;
   
   if( SG_gateway_id( gateway ) == SG_GATEWAY_ANON ) {
      return -EPERM;
   }
   
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      return rc;
   }
   
   fent = fskit_entry_resolve_path( fs, path, user, volume, true, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   // built-in handler?
   if( xattr_handler != NULL ) {
      
      rc = (*xattr_handler->set)( fs, fent, name, value, size, flags );
      fskit_entry_unlock( fent );
      return rc;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   // nope; upload to MS
   rc = UG_inode_export( &inode_data, inode, 0, NULL );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      return rc;
   }
   
   rc = ms_client_setxattr( ms, &inode_data, name, value, size, mode, flags );
   if( rc < 0 ) {
      
      SG_error("ms_client_setxattr('%s'.'%s') rc = %d\n", path, name, rc );
   }
   else {
      
      // cache!
      rc = fskit_fsetxattr( fs, fent, name, value, size, flags );
      if( rc < 0 ) {
         
         SG_error("fskit_fsetxattr('%s'.%s') rc = %d\n", path, name, rc );
      }
   }
   
   fskit_entry_unlock( fent );
   return rc;
}


// setxattr(2), with default xattr mode 
int UG_setxattr( struct SG_gateway* gateway, char const* path, char const* name, char const* value, size_t size, int flags, uint64_t user, uint64_t volume ) {
   return UG_setxattr_ex( gateway, path, name, value, size, flags, user, volume, 0744 );
}


// try to get an xattr, but set it if it is not defined.  There will be only one "set" winner globally, but "get" might return nothing (since the get and set do not occur as an atomic action)
// return 0 on success, and put the attr's value into *value and its length into *value_len.  The buffer will be malloc'ed by this call.
// return -EPERM if this is an anonymous gateway
// return -ENOMEM on OOM 
// return -ENOENT if the file doesn't exist
// return -EACCES if we're not allowed to write to the file
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
// NOTE: fent must be write-locked
int UG_get_or_set_xattr( struct SG_gateway* gateway, struct fskit_entry* fent, char const* name, char const* proposed_value, size_t proposed_value_len, char** value, size_t* value_len, mode_t mode ) {
   
   int rc = 0;
   char* val = NULL;
   ssize_t vallen = 0;
   bool try_get = false;
   struct md_entry inode_data;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct UG_xattr_handler_t* xattr_handler = NULL;
   
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   if( SG_gateway_id( gateway ) == SG_GATEWAY_ANON ) {
      return -EPERM;
   }
   
   // find the xattr handler for this attribute
   xattr_handler = UG_xattr_lookup_handler( name );
   if( xattr_handler == NULL ) {
      
      // attempt to set, but fail if we don't create.
      rc = UG_inode_export( &inode_data, inode, 0, NULL );
      if( rc != 0 ) {
         
         // OOM 
         return rc;
      }
      
      rc = ms_client_setxattr( ms, &inode_data, name, proposed_value, proposed_value_len, XATTR_CREATE, mode );
      if( rc < 0 ) {
         
         if( rc != -EEXIST ) {
            SG_error("ms_client_setxattr(%" PRIX64 ".'%s') rc = %d\n", UG_inode_file_id( *inode ), name, rc );
         }
         else {
            
            try_get = true;
         }
      }
      else {
         
         // succeeded!
         // put it 
         rc = fskit_fsetxattr( fs, fent, name, proposed_value, proposed_value_len, 0 );
         if( rc < 0 ) {
            
            SG_error("fskit_fsetxattr(%" PRIX64 ".'%s') rc = %d\n", UG_inode_file_id( *inode ), name, rc );
         }
         
         *value = SG_CALLOC( char, proposed_value_len );
         if( *value == NULL ) {
            
            // OOM
            rc = -ENOMEM;
         }
         else {
            
            memcpy( *value, proposed_value, proposed_value_len );
            *value_len = proposed_value_len;
         }
      }
      
      md_entry_free( &inode_data );
      
      if( rc < 0 ) {
         
         // had an error...         
         return rc;
      }
      
      if( try_get ) {
         
         // failed to set.  try to get the attribute instead...
         vallen = UG_download_xattr( gateway, volume_id, UG_inode_file_id( *inode ), name, &val );
         if( vallen < 0 ) {
            
            SG_error("UG_download_xattr( %" PRIX64 ".'%s' ) rc = %d\n", UG_inode_file_id( *inode ), name, (int)vallen);
            return vallen;
         }
         
         // cache it
         rc = fskit_fsetxattr( fs, fent, name, val, vallen, 0 );
         if( rc < 0 ) {
            
            // not strictly an error, since we can go get it later
            SG_warn("fskit_fsetxattr( %" PRIX64 ".'%s' ) rc = %d\n", UG_inode_file_id( *inode ), name, rc );
            rc = 0;
         }
         
         // save it!
         *value = val;
         *value_len = (size_t)vallen;
      }
   }
   else {
      // built-in handler.
      while( true ) {
         
         vallen = (*xattr_handler->get)( fs, fent, name, NULL, 0 );
         
         val = SG_CALLOC( char, vallen + 1 );
         if( val == NULL ) {
            
            rc = -ENOMEM;
            break;
         }
         
         rc = (*xattr_handler->get)( fs, fent, name, val, vallen );
         if( rc == -ERANGE ) {
            
            // try again 
            SG_safe_free( val );
            continue;
         }
         
         *value = val;
         *value_len = (size_t)rc;
         break;
      }
   }
   
   return rc;
}
   

// listxattr(2)--get back a list of xattrs from the MS
// return 0 on success, and fill in *list (if non-null) with \0-separated names for xattrs (up to size bytes)
// return -ENOMEM on OOM 
// return -ENOENT if the file doesn't exist
// return -ERANGE if list is not NULL, but too small
// return -EACCES if we're not allowed to write to the file
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
ssize_t UG_listxattr( struct SG_gateway* gateway, char const* path, char *list, size_t size, uint64_t user, uint64_t volume ) {
   
   int rc = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct fskit_entry* fent = NULL;
   
   uint64_t file_id = 0;
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct UG_inode* inode = NULL;
   
   char* remote_xattr_names = NULL;
   size_t remote_xattr_names_len = 0;
   
   ssize_t builtin_len = 0;
   
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   fent = fskit_entry_resolve_path( fs, path, user, volume, true, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   file_id = UG_inode_file_id( *inode );
   
   // don't allow this entry to get deleted...
   fskit_entry_unlock( fent );
   
   // check on the MS
   rc = ms_client_listxattr( ms, volume_id, file_id, &remote_xattr_names, &remote_xattr_names_len );
   if( rc < 0 ) {
      
      SG_error("ms_client_listxattr('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   if( size <= 0 || list == NULL ) {
      
      // buffer size query 
      rc = UG_xattr_len_all() + remote_xattr_names_len;
   }
   else {
      
      // get the built-in attributes, and copy them into list
      builtin_len = UG_xattr_get_builtin_names( list, size );
      if( builtin_len < 0 || remote_xattr_names_len + builtin_len > size ) {
         
         // buffer not big enough
         rc = -ERANGE;
      }
      
      else {
      
         // combine built-in and remote 
         memcpy( list + builtin_len, remote_xattr_names, remote_xattr_names_len );
         rc = builtin_len + remote_xattr_names_len;
      }
   }
   
   SG_safe_free( remote_xattr_names );
   
   return rc;
}


// removexattr(2)--delete an xattr on the MS and locally
// return 0 on success
// return -ENOMEM on OOM 
// return -EPERM if this is an anonymous gateway
// return -ENOENT if the file doesn't exist
// return -ERANGE if list is not NULL, but too small
// return -EACCES if we're not allowed to write to the file
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
int UG_removexattr( struct SG_gateway* gateway, char const* path, char const *name, uint64_t user, uint64_t volume ) {
   
   int rc = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct fskit_entry* fent = NULL;
   
   struct UG_inode* inode = NULL;
   struct md_entry inode_data;
   
   struct UG_xattr_handler_t* xattr_handler = NULL;
   
   if( SG_gateway_id( gateway ) == SG_GATEWAY_ANON ) {
      
      return -EPERM;
   }
   
   // refresh...
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      return rc;
   }
   
   fent = fskit_entry_resolve_path( fs, path, user, volume, true, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   // built-in?
   xattr_handler = UG_xattr_lookup_handler( name );
   if( xattr_handler != NULL ) {
      
      rc = (*xattr_handler->del)( fs, fent, name );
   }
   else {
      
      // ask the MS
      
      rc = UG_inode_export( &inode_data, inode, 0, NULL );
      if( rc != 0 ) {
         
         fskit_entry_unlock( fent );
         return rc;
      }
      
      rc = ms_client_removexattr( ms, &inode_data, name );
      if( rc < 0 ) {
         
         SG_error("ms_client_removexattr( '%s'.'%s' ) rc = %d\n", path, name, rc );
      }
      
      md_entry_free( &inode_data );
   }
   
   if( rc == 0 ) {
      
      // successfully removed; uncache 
      fskit_fremovexattr( fs, fent, name );
   }
   
   fskit_entry_unlock( fent );

   return rc;
}


// change ownership of an xattr 
// return 0 on success
// return -ENOMEM on OOM 
// return -EPERM if this is an anonymous gateway
// return -ENOENT if the file doesn't exist
// return -EACCES if we're not the owner
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
int UG_chownxattr( struct SG_gateway* gateway, char const* path, char const* name, uint64_t new_user ) {
   
   int rc = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct fskit_entry* fent = NULL;
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct UG_inode* inode = NULL;
   struct md_entry inode_data;
   
   if( SG_gateway_id( gateway ) == SG_GATEWAY_ANON ) {
      
      return -EPERM;
   }
   
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      return rc;
   }
   
   fent = fskit_entry_resolve_path( fs, path, SG_gateway_user_id( gateway ), volume_id, true, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   rc = UG_inode_export( &inode_data, inode, 0, NULL );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      return rc;
   }
   
   // change ownership!
   rc = ms_client_chownxattr( ms, &inode_data, name, new_user );
   if( rc < 0 ) {
      
      SG_error("ms_client_chownxattr( '%s'.'%s' to %" PRIu64 " ) rc = %d\n", path, name, new_user, rc );
   }
   else {
      
      // uncache
      fskit_fremovexattr( fs, fent, name );
   }
   
   fskit_entry_unlock( fent );
   return rc;
}


// change mode of an xattr 
// return 0 on success
// return -ENOMEM on OOM 
// return -EPERM if this is an anonymous gateway
// return -ENOENT if the file doesn't exist
// return -EACCES if we're not the owner
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500 
// return between -499 and -400 if the HTTP error was in the range 400 to 499
int UG_chmodxattr( struct SG_gateway* gateway, char const* path, char const* name, mode_t new_mode ) {
   
   
   int rc = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct fskit_entry* fent = NULL;
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct UG_inode* inode = NULL;
   struct md_entry inode_data;
   
   if( SG_gateway_id( gateway ) == SG_GATEWAY_ANON ) {
      
      return -EPERM;
   }
   
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      return rc;
   }
   
   fent = fskit_entry_resolve_path( fs, path, SG_gateway_user_id( gateway ), volume_id, true, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   rc = UG_inode_export( &inode_data, inode, 0, NULL );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      return rc;
   }
   
   // change ownership!
   rc = ms_client_chmodxattr( ms, &inode_data, name, new_mode );
   if( rc < 0 ) {
      
      SG_error("ms_client_chmodxattr( '%s'.'%s' to %o ) rc = %d\n", path, name, new_mode, rc );
   }
   else {
      
      // uncache
      fskit_fremovexattr( fs, fent, name );
   }
   
   fskit_entry_unlock( fent );
   return rc;
   
}
