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


// generate and send a WRITE message to another UG.
// write_data should be prepopuldated with the manifest, owner, mode, mtime, etc.--everything *but* the routing info (which will get overwritten)
// return 0 on success, get back the latest inode data via *inode_out and sy
// return -EINVAL if all data are NULL
// return -ENOMEM on OOM 
// return -EAGAIN if the request should be retried (i.e. it timed out, or the remote gateway told us)
// return -EREMOTEIO if there was a network-level error 
int UG_send_WRITE( struct UG_state* state, char const* fs_path, struct SG_client_WRITE_data* write_data, struct md_entry* inode_out ) {
    
   int rc = 0;
   
   struct fskit_core* fs = UG_state_fs( state );
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct UG_inode* inode = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t coordinator_id = 0;
   uint64_t file_id = 0;
   int64_t file_version = 0;
   int64_t write_nonce;
   
   SG_messages::Request req;
   SG_messages::Reply reply;
   
   struct fskit_entry* fent = fskit_entry_ref( fs, fs_path, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   // who are we sending to?
   coordinator_id = UG_inode_coordinator_id( inode );
   file_id = UG_inode_file_id( inode );
   file_version = UG_inode_file_version( inode );
   write_nonce = UG_inode_write_nonce( inode );
   
   fskit_entry_unlock( fent );
   
   if( rc != 0 ) {
      
      // OOM
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   // make write data 
   SG_client_WRITE_data_set_routing_info( write_data, volume_id, coordinator_id, file_id, file_version );
   
   // NOTE: update metadata only; use UG_write() to update manifest blocks
   rc = SG_client_request_WRITE_setup( gateway, &req, fs_path, write_data );
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
      
      // timed out? retry 
      if( rc == -ETIMEDOUT ) {
         rc = -EAGAIN;
      }
      
      // propagate retries; everything else is remote I/O error 
      if( rc != -EAGAIN ) {
         rc = -EREMOTEIO;
      }
      
      return rc;
   }
   
   if( reply.error_code() != 0 ) {
      
      // failed to process
      SG_error("SG_client_request_send(WRITE '%s') reply error = %d\n", fs_path, rc );
      
      fskit_entry_unref( fs, fs_path, fent );
      return reply.error_code();
   }
   
   // recover write nonce
   if( reply.has_ent_out() ) {
      
      // verify response
      rc = ms_entry_verify( SG_gateway_ms( gateway ), reply.mutable_ent_out() );
      if( rc != 0 ) {
          
          SG_error("Unable to verify response %" PRIX64 " (%s) from %" PRIu64 ", rc = %d\n", file_id, fs_path, coordinator_id, rc );
          fskit_entry_unref( fs, fs_path, fent );
          return rc;
      }
      
      // deserialize
      memset( inode_out, 0, sizeof(struct md_entry) );
      rc = ms_entry_to_md_entry( reply.ent_out(), inode_out );
      if( rc != 0 ) {
          
          fskit_entry_unref( fs, fs_path, fent );
          return rc;
      }
      
      fskit_entry_wlock( fent );
    
      inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
      
      // reload, if we haven't written in the mean time
      if( file_version == UG_inode_file_version( inode ) && write_nonce == UG_inode_write_nonce( inode ) ) {
         rc = UG_inode_import( inode, inode_out );
      }
      else {
         rc = 0;
      }
      
      fskit_entry_unlock( fent );
      
      if( rc != 0 ) {
          
          // will need to refresh
          SG_error("UG_inode_import(%" PRIX64 " (%s)) rc = %d\n", file_id, fs_path, rc );
          UG_inode_set_read_stale( inode, true );
          
          rc = 0;
      }
   }
   
   fskit_entry_unref( fs, fs_path, fent );
   
   return rc;
}

// propagate locally-updated inode metadata
// always succeeds
// NOTE: inode->entry must be write-locked
static int UG_update_propagate_local( struct UG_inode* inode, struct md_entry* inode_ms ) {
   
   if( inode_ms != NULL ) {
       
      UG_inode_set_write_nonce( inode, inode_ms->write_nonce );
      
      fskit_entry_set_owner( UG_inode_fskit_entry( inode ), inode_ms->owner );
      SG_manifest_set_owner_id( UG_inode_manifest( inode ), inode_ms->owner );
      
      fskit_entry_set_mode( UG_inode_fskit_entry( inode ), inode_ms->mode );
      
      struct timespec mtime;
      mtime.tv_sec = inode_ms->mtime_sec;
      mtime.tv_nsec = inode_ms->mtime_nsec;
      
      fskit_entry_set_mtime( UG_inode_fskit_entry( inode ), &mtime );
   }
   
   return 0;
}


// ask the MS to update inode metadata
// NULL data will be ignored.
// the associated inode must be unlocked or read-locked
// return 0 on success 
// return -EINVAL if all data are NULL
// return -ENOMEM on OOM
static int UG_update_local( struct UG_state* state, char const* path, struct SG_client_WRITE_data* write_data ) {
   
   int rc = 0;
   struct md_entry inode_data;
   
   struct fskit_core* fs = UG_state_fs( state );
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   int64_t write_nonce = 0;
   struct md_entry inode_data_out;
   memset( &inode_data_out, 0, sizeof(struct md_entry) );
   
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   
   unsigned char xattr_hash[SHA256_DIGEST_LENGTH];
   memset( xattr_hash, 0, SHA256_DIGEST_LENGTH );
   
   // keep this around...
   fent = fskit_entry_ref( fs, path, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   write_nonce = UG_inode_write_nonce( inode );
   
   rc = UG_inode_export( &inode_data, inode, 0 );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, path, fent );
      return rc;
   }
   
   rc = UG_inode_export_xattr_hash( fs, SG_gateway_id( gateway ), inode, xattr_hash );
   if( rc != 0 ) {
       
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, path, fent );
      return rc;
   }
   
   fskit_entry_unlock( fent );
   
   // apply changes to the inode we'll send
   SG_client_WRITE_data_merge( write_data, &inode_data );
   inode_data.xattr_hash = xattr_hash;
   
   // send the update along
   rc = ms_client_update( ms, &inode_data_out, &inode_data );
   
   inode_data.xattr_hash = NULL;
   md_entry_free( &inode_data );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_update('%s') rc = %d\n", path, rc );
      
      fskit_entry_unref( fs, path, fent );
      md_entry_free( &inode_data_out );
      return rc;
   }
   
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   // propagate information back to the inode
   if( write_nonce == UG_inode_write_nonce( inode ) ) {
       
       // haven't written in the mean time, so apply changes to local copy as well
       // to keep it coherent with the MS
       UG_update_propagate_local( inode, &inode_data_out );
   }
   else {
       
       // data has since changed; will need to pull latest 
       UG_inode_set_read_stale( inode, true );
   }
  
   fskit_entry_unlock( fent );
   fskit_entry_unref( fs, path, fent );
   
   md_entry_free( &inode_data_out ); 
   return 0;
}


// ask a remote gateway to update inode metadata on the MS.
// NULL data will be ignored 
// the associated inode must be unlocked or read-locked
// return 0 on success 
// return -EINVAL if all data are NULL
// return -ENOMEM on OOM 
// return -EAGAIN if the request should be retried (i.e. it timed out, or the remote gateway told us)
// return -EREMOTEIO if there was a network-level error 
// return non-zero error if the write was processed remotely, but failed remotely
static int UG_update_remote( struct UG_state* state, char const* fs_path, struct SG_client_WRITE_data* write_data ) {
   
   int rc = 0;
   struct md_entry inode_out;
   struct fskit_core* fs = UG_state_fs( state );
   
   struct fskit_entry* fent = fskit_entry_ref( fs, fs_path, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   // send the write off, and sync the inode
   rc = UG_send_WRITE( state, fs_path, write_data, &inode_out );
   if( rc != 0 ) {
       
       SG_error("UG_send_WRITE('%s') rc = %d\n", fs_path, rc );
       fskit_entry_unref( fs, fs_path, fent );
       return rc;
   }
   
   fskit_entry_unref( fs, fs_path, fent );
   return rc;
}


// update inode metadata--if local, issue the call to the MS; if remote, issue the call to the coordinator or try to become the coordinator if that fails.
// NULL data will be ignored 
// return 0 on success
// return -EINVAL if all data are NULL
// return -ENOMEM on OOM 
// NOTE: inode->entry must be unlocked!
int UG_update( struct UG_state* state, char const* path, struct SG_client_WRITE_data* write_data ) {
   
   int rc = 0;
   int ref_rc = 0;
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
   
   UG_try_or_coordinate( gateway, path, coordinator_id, UG_update_local( state, path, write_data ), UG_update_remote( state, path, write_data ), &rc );
   
   ref_rc = fskit_entry_unref( fs, path, fent );
   if( ref_rc != 0 ) {
      SG_warn("fskit_entry_unref('%s') rc = %d\n", path, ref_rc );
   }
   
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


// stat raw entry
// get the md_entry itself
// return 0 on success
// return -errno on error
int UG_stat_raw( struct UG_state* state, char const* path, struct md_entry* ent ) {
    
   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct fskit_core* fs_core = UG_state_fs( state ); 

   // refresh path 
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {

      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }

   rc = UG_inode_export_fs( fs_core, path, ent );
   return rc;
}


// mkdir(2)
// forward to fskit, which will take care of communicating with the MS
int UG_mkdir( struct UG_state* state, char const* path, mode_t mode ) {
   
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
   
   int rc = 0;
   struct SG_client_WRITE_data* write_data = NULL;
   
   write_data = SG_client_WRITE_data_new();
   if( write_data == NULL ) {
       return -ENOMEM;
   }
   
   // prepare to write 
   SG_client_WRITE_data_init( write_data );
   SG_client_WRITE_data_set_mode( write_data, mode );
   
   rc = UG_update( state, path, write_data );
   
   SG_safe_free( write_data );
   
   return rc;
}

// chown(2)
int UG_chown( struct UG_state* state, char const* path, uint64_t new_owner ) {
   
   int rc = 0;
   struct SG_client_WRITE_data* write_data = NULL;
   
   write_data = SG_client_WRITE_data_new();
   if( write_data == NULL ) {
       return -ENOMEM;
   }
   
   SG_client_WRITE_data_init( write_data );
   
   // prepare to write 
   SG_client_WRITE_data_init( write_data );
   SG_client_WRITE_data_set_owner_id( write_data, new_owner );
   
   rc = UG_update( state, path, write_data );
   
   SG_safe_free( write_data );
   
   return rc;
}


// utime(2)
int UG_utime( struct UG_state* state, char const* path, struct utimbuf *ubuf ) {
   
   int rc = 0;
   struct SG_client_WRITE_data* write_data = NULL;
   struct timespec mtime;
   
   write_data = SG_client_WRITE_data_new();
   if( write_data == NULL ) {
       return -ENOMEM;
   }
   
   mtime.tv_sec = ubuf->modtime;
   mtime.tv_nsec = 0;
   
   SG_client_WRITE_data_init( write_data );
   SG_client_WRITE_data_set_mtime( write_data, &mtime );
   
   rc = UG_update( state, path, write_data );
   
   SG_safe_free( write_data );
   
   return rc;
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
// return -EAGAIN if we need to try again--i.e. the information we had about the inode was out-of-date 
int UG_chcoord( struct UG_state* state, char const* path, uint64_t* new_coordinator_response ) {
   
   int rc = 0;
   struct md_entry inode_data;
   
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct fskit_core* fs = UG_state_fs( state );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   
   uint64_t file_id = 0;
   int64_t xattr_nonce = 0;
   int64_t write_nonce = 0;
   
   fskit_xattr_set* xattrs = NULL;      // xattrs to fetch
   fskit_xattr_set* old_xattrs = NULL;
   
   struct md_entry inode_data_out;
   memset( &inode_data_out, 0, sizeof(struct md_entry) );
   
   unsigned char xattr_hash[ SHA256_DIGEST_LENGTH ];
   unsigned char ms_xattr_hash[ SHA256_DIGEST_LENGTH ];
   unsigned char ms_xattr_hash2[ SHA256_DIGEST_LENGTH ];
   
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
      fskit_entry_unref( fs, path, fent );
      return rc;
   }
   
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   // MS-given info
   file_id = UG_inode_file_id( inode );
   xattr_nonce = UG_inode_xattr_nonce( inode );
   write_nonce = UG_inode_write_nonce( inode );
   UG_inode_ms_xattr_hash( inode, ms_xattr_hash );
   
   fskit_entry_unlock( fent );
   
   // go get the xattrs, and verify that they match this hash 
   rc = UG_consistency_fetchxattrs( gateway, file_id, xattr_nonce, ms_xattr_hash, &xattrs );
   if( rc != 0 ) {
       
       SG_error("UG_consistency_fetchxattrs('%s') rc = %d\n", path, rc );
       fskit_entry_unref( fs, path, fent );
       return rc;
   }
   
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   UG_inode_ms_xattr_hash( inode, ms_xattr_hash2 );
   
   // verify no changes in the mean time (otherwise retry)
   if( sha256_cmp( ms_xattr_hash, ms_xattr_hash2 ) == 0 ) {
       
       SG_error("xattr hash changed for %" PRIX64 "; retrying...\n", file_id );
       fskit_entry_unlock( fent );
       fskit_entry_unref( fs, path, fent );
       return -EAGAIN;
   }
   
   // good to go! install xattrs 
   old_xattrs = fskit_entry_swap_xattrs( fent, xattrs );
   if( old_xattrs != NULL ) {
       
       fskit_xattr_set_free( old_xattrs );
       old_xattrs = NULL;
   }
   
   // get inode info
   rc = UG_inode_export( &inode_data, inode, 0 );
   if( rc != 0 ) {
   
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, path, fent );
      return rc;
   }
   
   // get new xattr hash 
   rc = UG_inode_export_xattr_hash( fs, SG_gateway_id( gateway ), inode, xattr_hash );
   if( rc != 0 ) {
      
      md_entry_free( &inode_data );
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, path, fent );
      return rc;
   }
   
   // propagate new xattr hash
   inode_data.xattr_hash = xattr_hash;
   
   // set the new coordinator to ourselves, and increment the version number 
   inode_data.coordinator = SG_gateway_id( gateway );
   inode_data.version += 1;
   fskit_entry_unlock( fent );
   
   // ask the MS to make us the coordinator
   rc = ms_client_coordinate( ms, &inode_data_out, &inode_data, xattr_hash );
   
   inode_data.xattr_hash = NULL;        // don't free this...
   md_entry_free( &inode_data );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_coordinate('%s', %" PRIu64 ") rc = %d\n", path, inode_data_out.coordinator, rc );
      
      fskit_entry_unref( fs, path, fent );
      md_entry_free( &inode_data_out );
      return rc;
   }
       
   // pass back current coordinator
   *new_coordinator_response = inode_data_out.coordinator;
   
   // did we succeed?
   if( SG_gateway_id( gateway ) != inode_data_out.coordinator || inode_data_out.version <= inode_data.version ) {
       
       // nope 
       fskit_entry_unref( fs, path, fent );
       md_entry_free( &inode_data_out );
       return -EAGAIN;
   }
   
   // can we load this data?
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   if( write_nonce == UG_inode_write_nonce( inode ) ) {
       
       // MS data is fresh 
       rc = UG_inode_import( inode, &inode_data_out );
       if( rc != 0 ) {
           
           // failed to load. mark stale.
           UG_inode_set_read_stale( inode, true );
           rc = 0;
       }
   }
   else {
       
       // local changes.  make sure we reload before trying again.
       UG_inode_set_read_stale( inode, true );
   }
   
   fskit_entry_unlock( fent );
   fskit_entry_unref( fs, path, fent );
   
   md_entry_free( &inode_data_out );
   return rc;
}


// invalidate a cached metadata entry 
// return 0 on success
// return -ENOMEM on OOM
// return -ENOENT if there is no such entry 
int UG_invalidate( struct UG_state* state, char const* path ) {

   int rc = 0;
   struct fskit_core* fs = UG_state_fs( state );
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;

   fent = fskit_entry_resolve_path( fs, path, 0, 0, true, &rc );
   if( fent == NULL ) {
      return rc;
   }

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   UG_inode_set_read_stale( inode, true ); 

   fskit_entry_unlock( fent );
   return 0;
}


// refresh a cached metadata entry
// return 0 on success
// return -ENOMEM on OOM 
// return -ENOENT if the entry does not exist 
// return -EREMOTEIO on failure to talk to the MS 
int UG_refresh( struct UG_state* state, char const* path ) {

   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );

   rc = UG_consistency_path_ensure_fresh( gateway, path );
   return rc;
}


// start vacuuming a file inode's old data (used to recover after an unclean shutdown)
// set up and return *vctx to be a waitable vacuum context 
// return 0 on success
// return -ENOMEM on OOM
// return -ENOENT if there so such path
// return -EACCES if we can't write to the file 
// return -EISDIR if the path refers to a directory
// return -ENOTCONN if we're quiescing requests
int UG_vacuum_begin( struct UG_state* state, char const* path, struct UG_vacuum_context** ret_vctx ) {

   int rc = 0;
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct fskit_core* fs = UG_state_fs( state );
   struct UG_vacuum_context* vctx = NULL;
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;

   // refresh path 
   rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( rc != 0 ) {

      SG_error("UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }

   // refresh manifest 
   rc = UG_consistency_manifest_ensure_fresh( gateway, path );
   if( rc != 0 ) {

      SG_error("UG_consistency_manifest_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }

   fent = fskit_entry_resolve_path( fs, path, 0, 0, true, &rc );
   if( rc != 0 ) {

      SG_error("fskit_entry_resolve_path('%s') rc = %d\n", path, rc );
      return rc;
   }

   if( fskit_entry_get_type( fent ) != FSKIT_ENTRY_TYPE_FILE ) {
      SG_error("'%s' is not a file\n", path );
      fskit_entry_unlock( fent );
      return -EISDIR;
   }

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );

   vctx = UG_vacuum_context_new();
   if( vctx == NULL ) {
      fskit_entry_unlock( fent );
      return -ENOMEM;
   }

   rc = UG_vacuum_context_init( vctx, state, path, inode, NULL );
   if( rc != 0 ) {
      SG_error("UG_vacuum_context_init rc = %d\n", rc );
      SG_safe_free( vctx );
      fskit_entry_unlock( fent );
      return rc;
   }

   rc = UG_vacuumer_enqueue_wait( UG_state_vacuumer( state ), vctx );
   if( rc != 0 ) {
      SG_error("UG_vacuumer_enqueue_wait rc = %d\n", rc );
      UG_vacuum_context_free( vctx );
      fskit_entry_unlock( fent );
      return rc;
   }

   fskit_entry_unlock( fent );

   *ret_vctx = vctx;
   return 0;
}


// wait for an ongoing vacuum request to finish 
// always succeeds (if it returns at all)
int UG_vacuum_wait( struct UG_vacuum_context* vctx ) {
   UG_vacuum_context_wait( vctx );
   UG_vacuum_context_free( vctx );
   return 0;
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

   // refresh manifest 
   rc = UG_consistency_manifest_ensure_fresh( gateway, path );
   if( rc != 0 ) {

      SG_error( "UG_consistency_manifest_ensure_fresh('%s') rc = %d\n", path, rc );
      return rc;
   }
   
   return fskit_trunc( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), newsize );
}

// open(2)
// forward to fskit
UG_handle_t* UG_open( struct UG_state* state, char const* path, int flags, int* rc ) {
   
   struct fskit_file_handle* fh = NULL;
   struct SG_gateway* gateway = UG_state_gateway( state );
   UG_handle_t* sh = SG_CALLOC( UG_handle_t, 1 );
   
   if( sh == NULL ) {
      
      *rc = -ENOMEM;
      return NULL;
   }
    
   // refresh path 
   *rc = UG_consistency_path_ensure_fresh( gateway, path );
   if( *rc != 0 ) {
      
      SG_error( "UG_consistency_path_ensure_fresh('%s') rc = %d\n", path, *rc );
      SG_safe_free( sh );
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
   
   if( fi == NULL ) {
      return -EBADF;
   }

   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   int nr = fskit_read( UG_state_fs( state ), fi->fh, buf, size, fi->offset );
   
   if( nr < 0 ) {
      return nr;
   }

   if( (unsigned)nr < size ) {
      
      // zero-out the remainder of the buffer
      memset( buf + nr, 0, size - nr );
   }
   
   fi->offset += nr;
   return nr;
}


// write(2)
// forward to fskit
int UG_write( struct UG_state* state, char const* buf, size_t size, UG_handle_t *fi ) {

   if( fi == NULL ) {
      return -EBADF;
   }

   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }

   int rc = fskit_write( UG_state_fs( state ), fi->fh, buf, size, fi->offset );
   
   if( rc < 0 ) {
      return rc;
   }
   
   fi->offset += size;
   return size;
}


// getblockinfo 
// get a block's metadata directly from the manifest.
// useful for redirecting remote requests on blocks.
// if non-null, hash must have SG_BLOCK_HASH_LEN bytes.
// return 0 on success.
// return -ENOMEM on OOM
// return -EBADF if fi is NULL or refers to a directory 
int UG_getblockinfo( struct UG_state* state, uint64_t block_id, int64_t* ret_block_version, unsigned char* ret_hash, UG_handle_t* fi ) {

   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   struct SG_manifest* manifest = NULL;
   struct SG_gateway* gateway = UG_state_gateway( state );
   int rc = 0;
   uint64_t file_id = 0;
   int64_t block_version = 0;
   unsigned char* hash = NULL;
   size_t hash_len = 0;

   if( fi == NULL ) {
      return -EBADF;
   }

   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
 
   fskit_file_handle_rlock( fi->fh );

   fent = fskit_file_handle_get_entry( fi->fh );
   if( fent == NULL ) {
      SG_error("BUG: file handle %p's entry is NULL\n", fi->fh);
      exit(1);
   }

   fskit_entry_rlock( fent );

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   if( inode == NULL ) {
      SG_error("BUG: inode for entry %p (handle %p) is NULL\n", fent, fi );
      exit(1);
   }

   file_id = UG_inode_file_id( inode );

   fskit_entry_unlock( fent );

   // ensure fresh 
   rc = UG_consistency_inode_ensure_fresh( gateway, fskit_file_handle_get_path( fi->fh ), inode );
   if( rc != 0 ) {
      SG_error("UG_consistency_inode_ensure_fresh('%s' (%" PRIX64 ")) rc = %d\n", fskit_file_handle_get_path( fi->fh ), file_id, rc );
      fskit_file_handle_unlock( fi->fh );
      goto UG_getblock_out;
   }

   // query block
   fskit_entry_rlock( fent );

   manifest = UG_inode_manifest( inode );
   if( manifest == NULL ) {
      SG_error("BUG: Manifest for %" PRIX64 " (%s) not set\n", file_id, fskit_file_handle_get_path( fi->fh ) );
      exit(1);
   }

   rc = SG_manifest_get_block_version( manifest, block_id, &block_version );
   if( rc != 0 ) {

      // not found, or write hole 
      SG_error("SG_manifest_get_block_version(%" PRIX64 "[%" PRIu64 "]) rc = %d\n", file_id, block_id, rc );
      fskit_entry_unlock( fent );
      fskit_file_handle_unlock( fi->fh );
      return rc;
   }

   if( ret_hash != NULL ) {

       rc = SG_manifest_get_block_hash( manifest, block_id, &hash, &hash_len );
       if( rc != 0 ) {
          
          SG_error("SG_manifest_get_block_hash(%" PRIX64 "[%" PRIu64 ".%" PRId64 "]) rc = %d\n", file_id, block_id, block_version, rc );
          fskit_entry_unlock( fent );
          fskit_file_handle_unlock( fi->fh );
          return rc;
       }

       memcpy( ret_hash, hash, hash_len );
       SG_safe_free( hash );
   }

   if( ret_block_version != NULL ) {
      *ret_block_version = block_version;
   }

   fskit_entry_unlock( fent );
   fskit_file_handle_unlock( fi->fh );

UG_getblock_out:

   return rc;
}


// putblockinfo
// put a block's metadata directly into the manifest.
// useful for when the driver knows how to serve data directly.
// hash must be a SHA256 (or must have SG_BLOCK_HASH_LEN bytes), or NULL.
// return 0 on success
// return -ENOMEM on OOM 
// return -EBADF if fi is NULL or refers to a directory
int UG_putblockinfo( struct UG_state* state, uint64_t block_id, int64_t block_version, unsigned char* hash, UG_handle_t* fi ) {

   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   struct SG_manifest* manifest = NULL;
   struct SG_gateway* gateway = UG_state_gateway( state );
   int rc = 0;
   uint64_t file_id = 0;
   struct SG_manifest_block binfo;
   int hash_len = SG_BLOCK_HASH_LEN;
   
   if( fi == NULL ) {
      return -EBADF;
   }

   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
 
   if( hash == NULL ) {
      hash_len = 0;
   }

   fskit_file_handle_rlock( fi->fh );

   fent = fskit_file_handle_get_entry( fi->fh );
   if( fent == NULL ) {
      SG_error("BUG: file handle %p's entry is NULL\n", fi->fh);
      exit(1);
   }

   fskit_entry_rlock( fent );

   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   if( inode == NULL ) {
      SG_error("BUG: inode for entry %p (handle %p) is NULL\n", fent, fi );
      exit(1);
   }

   file_id = UG_inode_file_id( inode );

   fskit_entry_unlock( fent );

   // ensure fresh 
   rc = UG_consistency_inode_ensure_fresh( gateway, fskit_file_handle_get_path( fi->fh ), inode );
   if( rc != 0 ) {
      SG_error("UG_consistency_inode_ensure_fresh('%s' (%" PRIX64 ")) rc = %d\n", fskit_file_handle_get_path( fi->fh ), file_id, rc );
      fskit_file_handle_unlock( fi->fh );
      goto UG_putblock_out;
   }

   // set up block 
   rc = SG_manifest_block_init( &binfo, block_id, block_version, hash, hash_len );
   if( rc != 0 ) {
      fskit_file_handle_unlock( fi->fh );
      goto UG_putblock_out;
   }

   fskit_entry_wlock( fent );

   manifest = UG_inode_manifest( inode );
   if( manifest == NULL ) {
      SG_error("BUG: Manifest for %" PRIX64 " (%s) not set\n", file_id, fskit_file_handle_get_path( fi->fh ) );
      exit(1);
   }

   rc = SG_manifest_put_block( manifest, &binfo, true );
   if( rc != 0 ) {
      SG_error("SG_manifest_put_block(%" PRIX64 "[%" PRIu64 ".%" PRId64 "] (%s)) rc = %d\n", file_id, block_id, block_version, fskit_file_handle_get_path(fi->fh), rc );
      fskit_entry_unlock( fent );
      fskit_file_handle_unlock( fi->fh );
      return rc;
   }

   fskit_entry_unlock( fent );
   fskit_file_handle_unlock( fi->fh );

UG_putblock_out:

   return rc;
}


// lseek(2)
off_t UG_seek( UG_handle_t* fi, off_t pos, int whence ) {
   
   if( fi == NULL ) {
      return -EBADF;
   }

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

   if( fi == NULL ) {
      return -EBADF;
   }
   
   if( fi->type != UG_TYPE_FILE ) {
      return -EBADF;
   }
   
   rc = fskit_close( UG_state_fs( state ), fi->fh );
   
   if( rc == 0 ) {
      free( fi );
   }
   
   return rc;
}


// fsync(2)
// forward to fskit
int UG_fsync( struct UG_state* state, UG_handle_t *fi ) {
   
   if( fi == NULL ) {
      return -EBADF;
   }

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
int UG_readdir( struct UG_state* state, struct md_entry*** ret_listing, size_t num_children, UG_handle_t *fi ) {

   size_t num_read = 0;
   size_t num_md_read = 0;
   int rc = 0;
   struct fskit_dir_entry** listing = NULL;
   struct md_entry** md_listing = NULL;
   
   if( fi == NULL ) {
      return -EBADF;
   }

   struct fskit_entry* dent = fskit_dir_handle_get_entry( fi->dh );
   char const* path = fskit_dir_handle_get_path( fi->dh );
   
   fskit_entry_rlock( dent );
   
   listing = fskit_readdir( UG_state_fs( state ), fi->dh, num_children, &num_read, &rc );
   
   if( listing != NULL && num_read > 0 ) {
      
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
            
            rc = UG_inode_export( md_listing[i], inode, 0 );
         }
         
         fskit_entry_unlock( child );
         
         if( rc != 0 ) {
            
            // OOM?
            break;
         }
         
         num_md_read ++;
      }
   }
   
   if( listing != NULL ) {
       
      fskit_dir_entry_free_list( listing );
   }
   
   fskit_entry_unlock( dent );
   
   *ret_listing = md_listing;
   
   return rc;
}


// rewindidir(3)
int UG_rewinddir( UG_handle_t* fi ) {
   
   if( fi == NULL ) {
      return -EBADF;
   }

   fskit_rewinddir( fi->dh );
   return 0;
}

// telldir(3)
off_t UG_telldir( UG_handle_t* fi ) {
   
   if( fi == NULL ) {
      return -EBADF;
   }

   return fskit_telldir( fi->dh );
}

// seekdir(3)
int UG_seekdir( UG_handle_t* fi, off_t loc ) {
   
   if( fi == NULL ) {
      return -EBADF;
   }

   fskit_seekdir( fi->dh, loc );
   return 0;
}

// closedir(3)
int UG_closedir( struct UG_state* state, UG_handle_t *fi ) {
   
   int rc = 0;
   
   if( fi == NULL ) {
      return -EBADF;
   }

   rc = fskit_closedir( UG_state_fs( state ), fi->dh );
   if( rc == 0 ) {
      
      free( fi );
   }
   
   return rc;
}

// free a dir listing 
// always succeeds
void UG_free_dir_listing( struct md_entry** listing ) {
   
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


// publish a file with the given metadata.
// forward to fskit
UG_handle_t* UG_publish( struct UG_state* state, char const* path, struct md_entry* ent_data, int* ret_rc ) {
   
   UG_handle_t* sh = NULL;
   struct fskit_file_handle* fh = NULL;
   
   // sanity check 
   if( ent_data->type != MD_ENTRY_FILE ) {
      *ret_rc = -EINVAL;
      return NULL;
   }

   sh = SG_CALLOC( UG_handle_t, 1 );
   if( sh == NULL ) {
      
      *ret_rc = -ENOMEM;
      return NULL;
   }
  
   fh = fskit_create_ex( UG_state_fs( state ), path, UG_state_owner_id( state ), UG_state_volume_id( state ), ent_data->mode, (void*)ent_data, ret_rc );
   
   if( fh == NULL ) {
      
      SG_safe_free( sh );
      return NULL;
   }
   
   sh->fh = fh;
   sh->type = UG_TYPE_FILE;
   
   return sh;
}


// POSIX-y creat(2):  make an empty file
// forward to fskit 
UG_handle_t* UG_create( struct UG_state* state, char const* fs_path, mode_t mode, int* ret_rc ) {

   struct md_entry ent_data;
   struct timespec ts;
   UG_handle_t* sh = NULL;
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct ms_client* ms = SG_gateway_ms( gateway );
   char* name = NULL;

   memset( &ent_data, 0, sizeof(struct md_entry) );

   name = md_basename( fs_path, NULL );
   if( name == NULL ) {
      *ret_rc = -ENOMEM;
      return NULL;
   }

   clock_gettime( CLOCK_REALTIME, &ts );

   // NOTE: file_id, write_nonce, xattr_nonce, num_children, capacity, and generation are all set 
   // by libsyndicate internally, on ms_client_create()
   ent_data.type = MD_ENTRY_FILE;
   ent_data.name = name;
   ent_data.volume = ms_client_get_volume_id( ms );
   ent_data.owner = SG_gateway_user_id( gateway );
   ent_data.coordinator = SG_gateway_id( gateway );
   ent_data.size = 0;
   ent_data.mode = mode;
   ent_data.mtime_sec = ts.tv_sec;
   ent_data.mtime_nsec = ts.tv_nsec;
   ent_data.manifest_mtime_sec = ts.tv_sec;
   ent_data.manifest_mtime_nsec = ts.tv_nsec;
   ent_data.ctime_sec = ts.tv_sec;
   ent_data.ctime_nsec = ts.tv_nsec;
   
   sh = UG_publish( state, fs_path, &ent_data, ret_rc );
   md_entry_free( &ent_data );

   return sh;
}


// ftruncate(2)
// forward to fskit
int UG_ftruncate( struct UG_state* state, off_t length, UG_handle_t *fi ) {
   
   if( fi == NULL ) {
      return -EBADF;
   }

   return fskit_ftrunc( UG_state_fs( state ), fi->fh, length );
}

// fstat(2)
// forward to fskit
int UG_fstat( struct UG_state* state, struct stat *statbuf, UG_handle_t *fi ) {
   
   if( fi == NULL ) { 
      return -EBADF;
   }

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
