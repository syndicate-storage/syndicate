/*
   Copyright 2015 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// top-level application-facing Syndicate User Gateway API

#ifndef _UG_CLIENT_H_
#define _UG_CLIENT_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/client.h>
#include <libsyndicate/opts.h>
#include <libsyndicate/gateway.h>

#include "xattr.h"

#include <fskit/fskit.h>

#define UG_TYPE_FILE FSKIT_ENTRY_TYPE_FILE
#define UG_TYPE_DIR FSKIT_ENTRY_TYPE_DIR

// types known to this gateway 
#define SYNDICATE_UG 1
#define SYNDICATE_RG 2
#define SYNDICATE_AG 3
   
// macro to try to perform an operation on the MS that can be done either locally (e.g. if we're the coordinator, or the inode is a directory), or remotely
// If the remote operation fails due to the remote gateway being unavailable, try to become the coordinator.  If we succeed, run the operation locally.
// Put the return value in *rc.
#define UG_try_or_coordinate( gateway, path, coordinator_id, local_oper, remote_oper, rc ) \
   do { \
      \
      struct UG_state* _state = (struct UG_state*)SG_gateway_cls( (gateway) ); \
      uint64_t _current_coordinator = coordinator_id; \
      uint64_t _caps = ms_client_get_gateway_caps( SG_gateway_ms( gateway ), SG_gateway_id( gateway ) ); \
      int _rc = 0; \
      \
      /* remote? */ \
      while( _current_coordinator != SG_gateway_id( (gateway) ) ) { \
         /* remote */ \
         _rc = remote_oper; \
         if( _rc == 0 ) { \
            *rc = _rc; \
            break; \
         } \
         \
         /* if we failed due to a network error, then try to become the coordinator and re-issue the command */ \
         else if( !SG_client_request_is_remote_unavailable( _rc ) ) { \
            /* failed for some other reason */ \
            *rc = _rc; \
            break; \
         } \
         else if( (_caps & SG_CAP_COORDINATE) != 0 ) { \
            \
            /* we can try to become the coordinator */  \
            _rc = UG_chcoord( _state, (path), &_current_coordinator ); \
            if( _rc != 0 ) { \
                \
               if( _rc == -EAGAIN ) { \
                   /* try again */ \
                   SG_warn("UG_chcoord('%s' to %" PRIu64 ") rc = %d\n", (path), SG_gateway_id( (gateway) ), _rc ); \
                   continue; \
               } \
               else { \
                   *rc = _rc; \
                   /* failed to talk to the MS */ \
                   SG_error("UG_chcoord('%s' to %" PRIu64 ") rc = %d\n", (path), SG_gateway_id( (gateway) ), _rc ); \
                   break; \
               } \
            } \
            else { \
               \
               /* made local! */ \
               _current_coordinator = SG_gateway_id( (gateway) ); \
               break; \
            } \
         } \
         else { \
            /* failed, and we can't coordinate */ \
            *rc = _rc; \
            break; \
         } \
      } \
         \
      if( _current_coordinator == SG_gateway_id( (gateway) ) ) { \
         \
         /* local */ \
         *rc = local_oper; \
         break; \
      } \
      \
   } while( 0 );

   
extern "C" {

// file handle wrapper 
typedef struct _UG_handle {
   int type;
   off_t offset;
   union {
      struct fskit_file_handle* fh;
      struct fskit_dir_handle* dh;
   };
} UG_handle_t;

// high-level metadata API
int UG_stat( struct UG_state* state, char const* path, struct stat *statbuf );
int UG_stat_raw( struct UG_state* state, char const* path, struct md_entry* ent );
int UG_mkdir( struct UG_state* state, char const* path, mode_t mode );
int UG_unlink( struct UG_state* state, char const* path );
int UG_rmdir( struct UG_state* state, char const* path );
int UG_rename( struct UG_state* state, char const* path, char const* newpath );
int UG_chmod( struct UG_state* state, char const* path, mode_t mode );
int UG_chown( struct UG_state* state, char const* path, uint64_t new_owner );
int UG_utime( struct UG_state* state, char const* path, struct utimbuf *ubuf );
int UG_chcoord( struct UG_state* state, char const* path, uint64_t* new_coordinator_response );
int UG_truncate( struct UG_state* state, char const* path, off_t newsize );
int UG_access( struct UG_state* state, char const* path, int mask );
int UG_invalidate( struct UG_state* state, char const* path );
int UG_refresh( struct UG_state* state, char const* path );

// low-level metadata API
int UG_update( struct UG_state* state, char const* path, struct SG_client_WRITE_data* write_data );

// high-level file data API
UG_handle_t* UG_create( struct UG_state* state, char const* path, mode_t mode, int* rc  );
UG_handle_t* UG_publish( struct UG_state* state, char const* path, struct md_entry* ent_data, int* ret_rc );
UG_handle_t* UG_open( struct UG_state* state, char const* path, int flags, int* rc );
int UG_read( struct UG_state* state, char *buf, size_t size, UG_handle_t* fi );
int UG_write( struct UG_state* state, char const* buf, size_t size, UG_handle_t *fi );
int UG_getblockinfo( struct UG_state* state, uint64_t block_id, int64_t* block_version, unsigned char* hash, UG_handle_t* fi );
int UG_putblockinfo( struct UG_state* state, uint64_t block_id, int64_t block_version, unsigned char* hash, UG_handle_t* fi );
off_t UG_seek( UG_handle_t* fi, off_t pos, int whence );
int UG_close( struct UG_state* state, UG_handle_t *fi );
int UG_fsync( struct UG_state* state, UG_handle_t *fi );
int UG_ftruncate( struct UG_state* state, off_t offset, UG_handle_t *fi );
int UG_fstat( struct UG_state* state, struct stat *statbuf, UG_handle_t *fi );

// low-level file data API
int UG_vacuum_begin( struct UG_state* state, char const* path, struct UG_vacuum_context** vctx );
int UG_vacuum_wait( struct UG_vacuum_context* vctx );

// high-level directory data API
UG_handle_t* UG_opendir( struct UG_state* state, char const* path, int* rc );
int UG_readdir( struct UG_state* state, struct md_entry*** listing, size_t num_children, UG_handle_t *fi );
int UG_rewinddir( UG_handle_t* fi );
off_t UG_telldir( UG_handle_t* fi );
int UG_seekdir( UG_handle_t* fi, off_t loc );
int UG_closedir( struct UG_state* state, UG_handle_t *fi );
void UG_free_dir_listing( struct md_entry** listing );

// high-level xattr API
int UG_setxattr( struct UG_state* state, char const* path, char const* name, char const* value, size_t size, int flags );
int UG_getxattr( struct UG_state* state, char const* path, char const* name, char *value, size_t size );
int UG_listxattr( struct UG_state* state, char const* path, char *list, size_t size );
int UG_removexattr( struct UG_state* state, char const* path, char const* name );

// low-level; used internally
int UG_send_WRITE( struct UG_state* state, char const* fs_path, struct SG_client_WRITE_data* write_data, struct md_entry* new_inode_state );

}

#endif
