/*
   Copyright 2015 The Trustees of Princeton University

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

#include "client.h"
#include "consistency.h"
#include "inode.h"
#include "read.h"
#include "sync.h"
#include "write.h"
#include "xattr.h"
#include "replication.h"

// propagate updated inode metadata
// always succeeds
// NOTE: inode->entry must be write-locked
static int UG_update_propagate( struct UG_inode* inode, int64_t* write_nonce, uint64_t* owner_id, mode_t* mode, struct timespec* mtime ) {
   
   if( write_nonce != NULL ) {
      
      UG_inode_set_write_nonce( inode, *write_nonce );
   }
   if( owner_id != NULL ) {
      
      fskit_entry_set_owner( UG_inode_fskit_entry( inode ), *owner_id );
      SG_manifest_set_owner_id( UG_inode_manifest( inode ), *owner_id );
   }
   if( mode != NULL ) {
      
      fskit_entry_set_mode( UG_inode_fskit_entry( inode ), *mode );
   }
   if( mtime != NULL ) {
      
      fskit_entry_set_mtime( UG_inode_fskit_entry( inode ), mtime );
   }
   
   return 0;
}

// ask the MS to update inode metadata
// NULL data will be ignored.
// return 0 on success 
// return -EINVAL if all data are NULL
// return -ENOMEM on OOM
static int UG_update_local( struct UG_state* state, char const* path, uint64_t* owner_id, mode_t* mode, struct timespec* mtime ) {
   
   int rc = 0;
   struct md_entry inode_data;
   
   struct fskit_core* fs = UG_state_fs( state );
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   int64_t write_nonce = 0;
   int64_t ms_write_nonce = 0;          // write nonce, according to the MS
   
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   
   if( owner_id == NULL && mode == NULL && mtime == NULL ) {
      return -EINVAL;
   }
   
   // keep this around...
   fent = fskit_entry_ref( fs, path, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   write_nonce = UG_inode_write_nonce( inode );
   
   rc = UG_inode_export_fs( fs, path, &inode_data );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, path, fent );
      return rc;
   }
   
   fskit_entry_unlock( fent );
   
   if( mode != NULL ) {
      
      inode_data.mode = *mode;
   }
   
   if( owner_id != NULL ) {
      
      inode_data.owner = *owner_id;
   }
   
   if( mtime != NULL ) {
      
      inode_data.mtime_sec = mtime->tv_sec;
      inode_data.mtime_nsec = mtime->tv_nsec;
   }
   
   rc = ms_client_update( ms, &ms_write_nonce, &inode_data );
   
   md_entry_free( &inode_data );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_update('%s', mode=%o) rc = %d\n", path, *mode, rc );
      
      fskit_entry_unref( fs, path, fent );
      return rc;
   }
   
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   // propagate information back to the inode, if the write nonce hasn't changed
   if( write_nonce == UG_inode_write_nonce( inode ) ) {
      
      UG_update_propagate( inode, NULL, owner_id, mode, mtime );
   }
   else {
      
      // mark stale, so we reload later 
      UG_inode_set_read_stale( inode, true );
   }
   
   fskit_entry_unlock( fent );
   fskit_entry_unref( fs, path, fent );
   
   return 0;
}


// ask a remote gateway to update inode metadata on the MS.
// NULL data will be ignored 
// return 0 on success 
// return -EINVAL if all data are NULL
// return -ENOMEM on OOM 
static int UG_update_remote( struct UG_state* state, char const* fs_path, uint64_t* owner_id, mode_t* mode, struct timespec* mtime ) {
   
   int rc = 0;
   
   struct fskit_core* fs = UG_state_fs( state );
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct UG_inode* inode = NULL;
   
   SG_messages::Request req;
   SG_messages::Reply reply;
   
   int64_t write_nonce = 0;
   uint64_t coordinator_id = 0;
   
   struct fskit_entry* fent = fskit_entry_ref( fs, fs_path, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   // snapshot...
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   // get write nonce, so we can optimistically propogate the metadata back into the inode (i.e. if it doesn't change before/after the update)
   write_nonce = UG_inode_write_nonce( inode );
   coordinator_id = UG_inode_coordinator_id( inode );
   
   fskit_entry_unlock( fent );
   
   if( rc != 0 ) {
      
      // OOM
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   rc = SG_client_request_WRITE_setup( gateway, &req, fs_path, NULL, owner_id, mode, mtime );
   if( rc != 0 ) {
      
      // OOM 
      SG_error("SG_client_request_WRITE_setup('%s') rc = %d\n", fs_path, rc );
      
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   rc = SG_client_request_send( gateway, coordinator_id, &req, NULL, &reply );
   if( rc != 0 ) {
      
      // network error 
      SG_error("SG_client_request_send(WRITE '%s') rc = %d\n", fs_path, rc );
      
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   if( reply.error_code() != 0 ) {
      
      // failed to process
      SG_error("SG_client_request_send(WRITE '%s') reply error = %d\n", fs_path, rc );
      
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   // opportunistically propagate this data locally 
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   if( write_nonce == UG_inode_write_nonce( inode ) ) {
      
      UG_update_propagate( inode, NULL, owner_id, mode, mtime );
   }
   
   fskit_entry_unlock( fent );
   fskit_entry_unref( fs, fs_path, fent );
   
   return rc;
}


// update inode metadata--if local, issue the call to the MS; if remote, issue the call to the coordinator or try to become the coordinator if that fails.
// NULL data will be ignored 
// return 0 on success
// return -EINVAL if all data are NULL
// return -ENOMEM on OOM 
// NOTE: inode->entry must be write-locked!
static int UG_update( struct UG_state* state, char const* path, uint64_t* owner_id, mode_t* mode, struct timespec* mtime ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct fskit_core* fs = UG_state_fs( state );
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   uint64_t coordinator_id = 0;
   
   // ensure fresh first
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   // look up coordinator 
   fent = fskit_entry_ref( fs, path, &rc );
   if( fent == NULL ) {
   
      return rc;  
   }
   
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   coordinator_id = UG_inode_coordinator_id( inode );
   
   fskit_entry_unlock( fent );
   
   UG_try_or_coordinate( gateway, path, coordinator_id, UG_update_local( state, path, owner_id, mode, mtime ), UG_update_remote( state, path, owner_id, mode, mtime ), &rc );
   
   fskit_entry_unref( fs, path, fent );
   
   return rc;
}

// stat(2)
// forward to fskit, which will take care of refreshing the inode metadata
int UG_stat( struct UG_state* state, char const* path, struct stat *statbuf ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   
   // refresh path 
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_stat( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), statbuf );
}

// mkdir(2)
// forward to fskit, which will take care of communicating with the MS
int UG_mkdir( struct UG_state* state, char const* path, mode_t mode ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   
   // refresh path, but expect that the entry doesn't exist
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != -ENOENT ) {
      
      if( rc == 0 ) {
         // already exists 
         rc = -EEXIST;
      }
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_mkdir( UG_state_fs( state ), path, mode, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}

// unlink(2)
// forward to fskit, which will take care of communicating with the MS and garbage-collecting blocks
int UG_unlink( struct UG_state* state, char const* path ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   
   // refresh path 
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_unlink( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}

// rmdir(2)
// forward to fskit, which will take care of communiating with the MS
int UG_rmdir( struct UG_state* state, char const* path ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   
   // refresh path 
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_rmdir( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}


// rename(2)
// forward to fskit, which will take care of communicating with the MS
int UG_rename( struct UG_state* state, char const* path, char const* newpath ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   
   // refresh paths
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   rc = UG_consistency_path_ensure_fresh( gateway, newpath );
   if( rc != 0 && rc != -ENOENT ) {
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_rename( UG_state_fs( state ), path, newpath, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}


// chmod(2)
int UG_chmod( struct UG_state* state, char const* path, mode_t mode ) {
   
   return UG_update( state, path, NULL, &mode, NULL );
}

// chown(2)
int UG_chown( struct UG_state* state, char const* path, uint64_t new_owner ) {
   
   return UG_update( state, path, &new_owner, NULL, NULL );
}


// utime(2)
int UG_utime( struct UG_state* state, char const* path, struct utimbuf *ubuf ) {
   
   struct timespec mtime;
   
   mtime.tv_sec = ubuf->modtime;
   mtime.tv_nsec = 0;
   
   return UG_update( state, path, NULL, NULL, &mtime );
}


// try to change coordinator to the new gateway.
// return 0 on success
// return -EPERM if we do not have the SG_CAP_COORDINATE capability
// return -errno on failure to resolve the path (same errors as path_resolution(7))
// return -ENOMEM on OOM
// return -EACCES if this gateway was not the coordinator
// return -EREMOTEIO on remote MS error
// return -ENODATA if no/partial data was received 
// return -ETIMEDOUT if the request timed out
int UG_chcoord( struct UG_state* state, char const* path, uint64_t* new_coordinator_response ) {
   
   int rc = 0;
   struct md_entry inode_data;
   
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct fskit_core* fs = UG_state_fs( state );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   
   uint64_t ms_new_coordinator = 0;     // coordinator according to the MS
   int64_t ms_write_nonce = 0;          // write nonce, according to the MS
   
   uint64_t caps = ms_client_get_gateway_caps( ms, SG_gateway_id( gateway ) );
   
   // *can* we coordinate?
   if( (caps & SG_CAP_COORDINATE) == 0 ) {
      
      // nope
      return -EPERM;
   }
   
   // ensure we have both fresh data and a fresh manifest
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   // ref fent...
   fent = fskit_entry_ref( fs, path, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   // get latest manifest 
   rc = UG_consistency_manifest_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_manifest_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   rc = UG_inode_export( &inode_data, inode, 0, NULL );
   if( rc != 0 ) {
   
      fskit_entry_unlock( fent );
      return rc;
   }
   
   // set the new coordinator 
   inode_data.coordinator = SG_gateway_id( gateway );
   
   // ask the MS to make us the coordinator
   rc = ms_client_coordinate( ms, &ms_new_coordinator, &ms_write_nonce, &inode_data );
   
   md_entry_free( &inode_data );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_coordinate('%s', %" PRIu64 ") rc = %d\n", path, ms_new_coordinator, rc );
      
      return rc;
   }
   
   *new_coordinator_response = ms_new_coordinator;
   
   // refresh again, to see if we got the coordinatorship
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return rc;
}


// trunc(2)
// forward to fskit
int UG_truncate( struct UG_state* state, char const* path, off_t newsize ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   
   // refresh path 
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_trunc( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), newsize );
}

// open(2)
// forward to fskit
UG_handle_t* UG_open( struct UG_state* state, char const* path, int flags, int* rc ) {
   
   struct fskit_file_handle* fh = NULL;
   UG_handle_t* sh = SG_CALLOC( UG_handle_t, 1 );
   
   if( sh == NULL ) {
      
      *rc = -ENOMEM;
      return NULL;
   }
   
   fh = fskit_open( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), flags, 0644, rc );
   if( fh == NULL ) {
   
      SG_safe_free( sh );
      return NULL;
   }
   
   sh->type = UG_TYPE_FILE;
   sh->fh = fh;
   
   return sh;
}

// read(2)
// forward to fskit
int UG_read( struct UG_state* state, char *buf, size_t size, UG_handle_t *fi ) {
   
   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   int nr = fskit_read( UG_state_fs( state ), fi->fh, buf, size, fi->offset );
   
   if( nr < 0 ) {
      return nr;
   }

   if( nr < (signed)size ) {
      
      // zero-out the remainder of the buffer
      memset( buf + nr, 0, size - nr );
   }
   
   fi->offset += nr;
   return nr;
}

// write(2)
// forward to fskit
int UG_write( struct UG_state* state, char const* buf, size_t size, UG_handle_t *fi ) {

   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   int nw = fskit_write( UG_state_fs( state ), fi->fh, buf, size, fi->offset );
   
   if( nw < 0 ) {
      return nw;
   }
   
   fi->offset += nw;
   return nw;
}

// lseek(2)
off_t UG_seek( UG_handle_t* fi, off_t pos, int whence ) {
   
   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   if( whence == SEEK_SET  ) {
      fi->offset = pos;
   }
   else if( whence == SEEK_CUR  ) {
      fi->offset += pos;
   }
   else if( whence == SEEK_END  ) {
      
      struct fskit_entry* fent = fskit_file_handle_get_entry( fi->fh );
      
      fskit_entry_rlock( fent );
      
      fi->offset = fskit_entry_get_size( fent );
      
      fskit_entry_unlock( fent );
   }
   
   return fi->offset;
}

// close(2)
// forward to fskit
int UG_close( struct UG_state* state, UG_handle_t *fi ) {

   int rc = 0;
   
   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   rc = fskit_close( UG_state_fs( state ), fi->fh );
   
   if( rc == 0 ) {
      free( fi->fh );
      free( fi );
   }
   
   return rc;
}


// fsync(2)
// forward to fskit
int UG_fsync( struct UG_state* state, UG_handle_t *fi ) {
   
   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   return UG_sync_fsync_ex( UG_state_fs( state ), fskit_file_handle_get_path( fi->fh ), fskit_file_handle_get_entry( fi->fh ) );
}

// opendir(3)
// forward to fskit 
UG_handle_t* UG_opendir( struct UG_state* state, char const* path, int* rc ) {

   struct fskit_dir_handle* dh = NULL;
   UG_handle_t* sh = NULL;
   
   sh = SG_CALLOC( UG_handle_t, 1 );
   
   if( sh == NULL ) {
      
      *rc = -ENOMEM;
      return NULL;
   }
   
   dh = fskit_opendir( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), rc );
   if( dh == NULL ) {
      
      SG_safe_free( dh );
      return NULL;
   }
   
   sh->type = UG_TYPE_DIR;
   sh->dh = dh;
   
   return sh;
}

// readdir(3)
int UG_readdir( struct UG_state* state, UG_dir_listing_t* ret_listing, size_t num_children, UG_handle_t *fi ) {

   size_t num_read = 0;
   size_t num_md_read = 0;
   int rc = 0;
   struct fskit_dir_entry** listing = NULL;
   UG_dir_listing_t md_listing = NULL;
   
   // TODO: add fskit getter for this 
   struct fskit_entry* dent = fi->dh->dent;
   char const* path = fi->fh->path;
   
   fskit_entry_rlock( dent );
   
   listing = fskit_readdir( UG_state_fs( state ), fi->dh, fi->offset, num_children, &num_read, &rc );
   
   if( listing != NULL ) {
      
      // convert to list of md_entry 
      md_listing = SG_CALLOC( struct md_entry*, num_read + 1 );
      if( md_listing == NULL ) {
         
         fskit_entry_unlock( dent );
         
         SG_safe_free( md_listing );
         fskit_dir_entry_free_list( listing );
         
         return -ENOMEM;
      }
      
      for( unsigned int i = 0; i < num_read; i++ ) {
       
         md_listing[i] = SG_CALLOC( struct md_entry, 1 );
         if( md_listing[i] == NULL ) {
            
            fskit_entry_unlock( dent );
            
            UG_free_dir_listing( md_listing );
            fskit_dir_entry_free_list( listing );
            
            return -ENOMEM;
         }
      }
      
      for( unsigned int i = 0; i < num_read; i++ ) {
         
         // convert child to md_entry 
         struct UG_inode* inode = NULL;
         struct fskit_entry* child = fskit_dir_find_by_name( dent, listing[i]->name );
         if( child == NULL ) {
            
            // shouldn't happen....
            SG_warn("Child '%s' not found in '%s\n", listing[i]->name, path );
            continue;
         }
         
         fskit_entry_rlock( child );
         
         inode = (struct UG_inode*)fskit_entry_get_user_data( child );
         
         if( inode != NULL ) {
            
            rc = UG_inode_export( md_listing[i], inode, 0, NULL );
         }
         
         fskit_entry_unlock( child );
         
         if( rc != 0 ) {
            
            // OOM?
            break;
         }
         
         num_md_read ++;
      }
      
      fskit_entry_unlock( dent );
      
      fskit_dir_entry_free_list( listing );
      fi->offset += num_md_read;
   }
   
   *ret_listing = md_listing;
   
   return rc;
}


// rewindidir(3)
int UG_rewinddir( UG_handle_t* fi ) {
   
   fi->offset = 0;
   return 0;
}

// telldir(3)
off_t UG_telldir( UG_handle_t* fi ) {
   
   return fi->offset;
}

// seekdir(3)
int UG_seekdir( UG_handle_t* fi, off_t loc ) {
   
   fi->offset = loc;
   return 0;
}

// closedir(3)
int UG_closedir( struct UG_state* state, UG_handle_t *fi ) {
   
   int rc = 0;
   
   rc = fskit_closedir( UG_state_fs( state ), fi->dh );
   if( rc == 0 ) {
      
      free( fi->dh );
      free( fi );
   }
   
   return rc;
}

// free a dir listing 
// always succeeds
void UG_free_dir_listing( UG_dir_listing_t listing ) {
   
   for( int i = 0; listing[i] != NULL; i++ ) {
      
      md_entry_free( listing[i] );
      SG_safe_free( listing[i] );
   }
   
   SG_safe_free( listing );
}


// access(2)
// forward to fskit
int UG_access( struct UG_state* state, char const* path, int mask ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct fskit_core* fs = UG_state_fs( state );
   
   // ensure fresh first
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_access( fs, path, UG_state_owner_id( state ), UG_state_volume_id( state ), mask );
}

// creat(2)
// forward to fskit
UG_handle_t* UG_create( struct UG_state* state, char const* path, mode_t mode, int* ret_rc ) {
   
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   UG_handle_t* sh = NULL;
   struct fskit_file_handle* fh = NULL;
   
   // refresh path, but expect -ENOENT (since we don't want a collision)
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != -ENOENT ) {
      
      if( rc == 0 ) {
         rc = -EEXIST;
      }
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      *ret_rc = rc;
      return NULL;
   }
   
   sh = SG_CALLOC( UG_handle_t, 1 );
   if( sh == NULL ) {
      
      *ret_rc = -ENOMEM;
      return NULL;
   }
   
   fh = fskit_create( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), mode, ret_rc );
   
   if( fh == NULL ) {
      
      SG_safe_free( sh );
      return NULL;
   }
   
   sh->fh = fh;
   sh->type = UG_TYPE_FILE;
   
   return sh;
}

// ftruncate(2)
// forward to fskit
int UG_ftruncate( struct UG_state* state, off_t length, UG_handle_t *fi ) {
   
   return fskit_ftrunc( UG_state_fs( state ), fi->fh, length );
}

// fstat(2)
// forward to fskit
int UG_fstat( struct UG_state* state, struct stat *statbuf, UG_handle_t *fi ) {
   
   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   return fskit_fstat( UG_state_fs( state ), fskit_file_handle_get_path( fi->fh ), fskit_file_handle_get_entry( fi->fh ), statbuf );
}

// setxattr(2)
// forward to xattr
int UG_setxattr( struct UG_state* state, char const* path, char const* name, char const* value, size_t size, int flags ) {
   return UG_xattr_setxattr( UG_state_gateway( state ), path, name, value, size, flags, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}


// getxattr(2)
// forward to xattr
int UG_getxattr( struct UG_state* state, char const* path, char const* name, char *value, size_t size ) {
   return UG_xattr_getxattr( UG_state_gateway( state ), path, name, value, size, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}


// listxattr(2)
// forward to xattr 
int UG_listxattr( struct UG_state* state, char const* path, char *list, size_t size ) {
   return UG_xattr_listxattr( UG_state_gateway( state ), path, list, size, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}


// removexattr(2)
// forward to xattr 
int UG_removexattr( struct UG_state* state, char const* path, char const* name ) {
   return UG_xattr_removexattr( UG_state_gateway( state ), path, name, UG_state_owner_id( state ), UG_state_volume_id( state ) );
}

// chownxattr 
// forward to xattr 
int UG_chownxattr( struct UG_state* state, char const* path, char const* name, uint64_t new_owner ) {
   return UG_xattr_chownxattr( UG_state_gateway( state ), path, name, new_owner );
}

// chmodxattr 
// forward to xattr 
int UG_chmodxattr( struct UG_state* state, char const* path, char const* name, mode_t mode ) {
   return UG_xattr_chmodxattr( UG_state_gateway( state ), path, name, mode );
}

// get-or-set xattr 
// forward to xattr 
int UG_getsetxattr( struct UG_state* state, char const* path, char const* name, char const* new_value, size_t new_value_len, char** value, size_t* value_len, mode_t mode ) {
   
   int rc = 0;
   struct fskit_entry* fent = NULL;
   
   fent = fskit_entry_resolve_path( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), true, &rc );
   if( fent == NULL ) {
      return rc;
   }
   
   rc = UG_xattr_get_or_set_xattr( UG_state_gateway( state ), fent, name, new_value, new_value_len, value, value_len, mode );
   
   fskit_entry_unlock( fent );
   
   return rc;
}
