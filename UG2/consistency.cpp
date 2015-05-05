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

#include "consistency.h"
#include "read.h"

// download a manifest, synchronously.  Try from each gateway in gateway_ids, in order.
// return 0 on success, and populate *manifest 
// return -ENOMEM on OOM 
// return -EINVAL if reqdat doesn't refer to a manifest
// return -ENODATA if a manifest could not be fetched (i.e. no gateways online, all manifests obtained were invalid, etc.)
// NOTE: does *not* check if the manifest came from a different gateway than the one contacted
int UG_consistency_manifest_download( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t* gateway_ids, size_t num_gateway_ids, struct SG_manifest* manifest ) {
   
   int rc = 0;
   SG_messages::Manifest mmsg;
   
   if( !SG_request_is_manifest( reqdat ) ) {
      return -EINVAL;
   }
   
   for( size_t i = 0; i < num_gateway_ids; i++ ) {
      
      rc = SG_client_get_manifest( gateway, reqdat, gateway_ids[i], manifest );
      if( rc != 0 ) {
         
         // not from this one 
         SG_warn("SG_client_get_manifest( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) from %" PRIu64 " rc = %d\n", 
                  reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, gateway_ids[i], rc );
         
         rc = -ENODATA;
         continue;
      }      
   }
   
   return rc;
}


// Verify that a manifest is fresh.  Download and merge the latest manifest data for the referred inode if not.
// local dirty blocks that were overwritten will be dropped and evicted.
// return 0 on success 
// return -ENOMEM on OOM 
// return -ENODATA if we could not fetch a manifest, but needed to
int UG_consistency_manifest_ensure_fresh( struct SG_gateway* gateway, char const* fs_path ) {
   
   int rc = 0;
   struct SG_manifest new_manifest;
   struct SG_request_data reqdat;
   
   uint64_t* gateway_ids_buf = NULL;
   uint64_t* gateway_ids = NULL;        // either points to gateway_ids_buf, or gateway_ids_buf + 1 (i.e. to skip the coordinator)
   size_t num_gateway_ids = 0;
   
   int64_t manifest_mtime_sec;
   int32_t manifest_mtime_nsec;
   
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct timespec now;
   int64_t max_read_freshness = 0;
   struct timespec manifest_refresh_mtime;
   
   uint64_t file_id = 0;
   int64_t file_version = 0;
   uint64_t coordinator_id = 0;
   off_t file_size = 0;
   
   bool local_coordinator = false;
   
   // ref...
   fent = fskit_entry_ref( fs, fs_path, &rc );
   if( rc != 0 ) {
      
      return rc;
   }
   
   fskit_entry_rlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   manifest_refresh_mtime = UG_inode_manifest_refresh_time( *inode );
   file_id = UG_inode_file_id( *inode );
   file_version = UG_inode_file_version( *inode );
   coordinator_id = UG_inode_coordinator_id( *inode );
   file_size = fskit_entry_get_size( fent );
   max_read_freshness = UG_inode_max_read_freshness( *inode );
   
   SG_manifest_get_modtime( UG_inode_manifest( *inode ), &manifest_mtime_sec, &manifest_mtime_nsec );
   
   // are we the coordinator?
   if( SG_gateway_id( gateway ) == SG_manifest_get_coordinator( UG_inode_manifest( *inode ) ) ) {
      
      local_coordinator = true;
   }
   
   // if we're the coordinator and we didn't explicitly mark the manifest as stale, then it's fresh
   if( !SG_manifest_is_stale( UG_inode_manifest( *inode ) ) && local_coordinator ) {
      
      // we're the coordinator--we already have the freshest version 
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return 0;
   }
   
   rc = clock_gettime( CLOCK_REALTIME, &now );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("clock_gettime rc = %d\n", rc );
      
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   // is the manifest stale?
   if( !SG_manifest_is_stale( UG_inode_manifest( *inode ) ) && md_timespec_diff_ms( &now, &manifest_refresh_mtime ) <= max_read_freshness ) {
      
      // still fresh
      fskit_entry_unlock( fent );
      fskit_entry_unref( fs, fs_path, fent );
      return 0;
   }
   
   // manifest is stale--must refresh.
   fskit_entry_unlock( fent );
   
   // get list of gateways to try
   rc = UG_read_download_gateway_list( gateway, coordinator_id, &gateway_ids_buf, &num_gateway_ids );
   if( rc != 0 ) {
      
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   // set up a request 
   rc = SG_request_data_init_manifest( gateway, fs_path, file_id, file_version, manifest_mtime_sec, manifest_mtime_nsec, &reqdat );
   if( rc != 0 ) {
   
      SG_safe_free( gateway_ids_buf );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   // if we were the coordinator, skip ourselves
   if( local_coordinator ) {
      
      gateway_ids = gateway_ids_buf + 1;
      num_gateway_ids --;
   }
   
   // get the manifest 
   rc = UG_consistency_manifest_download( gateway, &reqdat, gateway_ids, num_gateway_ids, &new_manifest );
   
   SG_safe_free( gateway_ids_buf );
   
   if( rc != 0 ) {
      
      SG_error("UG_consistency_manifest_download( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n", 
               reqdat.file_id, reqdat.file_version, reqdat.manifest_timestamp.tv_sec, reqdat.manifest_timestamp.tv_nsec, rc );
      
      SG_request_data_free( &reqdat );
      fskit_entry_unref( fs, fs_path, fent );
      return rc;
   }
   
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   // merge in new blocks (but keep locally-dirty ones)
   // NOTE: this works without keeping the inode locked because UG_inode_manifest_merge_blocks is a commutative, associative operation!
   // other writes to it may have occurred intermittently, but that's okay to merge the new manifest since we'll arrive at the same manifest regardless of the merge order.
   rc = UG_inode_manifest_merge_blocks( gateway, inode, &new_manifest );
   
   if( rc == 0 ) {
      
      // if were the local coordinator, we need to fix up the manifest from the one we got back from the RGs
      if( local_coordinator ) {
         
         // if the size shrank, then truncate 
         if( (unsigned)SG_manifest_get_file_size( &new_manifest ) < (unsigned)file_size ) {
            
            UG_inode_truncate( gateway, inode, SG_manifest_get_file_size( &new_manifest ), SG_manifest_get_file_version( &new_manifest ) );
            fskit_entry_set_size( fent, SG_manifest_get_file_size( &new_manifest ) );
         }
         
         // restore modtime 
         SG_manifest_set_modtime( UG_inode_manifest( *inode ), SG_manifest_get_modtime_sec( &new_manifest ), SG_manifest_get_modtime_nsec( &new_manifest ) );
      }
         
      // update refresh time 
      rc = clock_gettime( CLOCK_REALTIME, &now );
      if( rc != 0 ) {
         
         rc = -errno;
         SG_error("clock_gettime rc = %d\n", rc );
         
         // mask--the worst that'll happen is we refresh too much
         rc = 0;
      }
      else {
         
         // advance refresh time
         UG_inode_set_manifest_refresh_time( inode, &now );
      }
   }
   
   fskit_entry_unlock( fent );
   
   SG_manifest_free( &new_manifest );
   
   if( rc != 0 ) {
      
      SG_error("UG_inode_manifest_merge_blocks( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n", 
                reqdat.file_id, reqdat.file_version, reqdat.manifest_timestamp.tv_sec, reqdat.manifest_timestamp.tv_nsec, rc );
   }
   
   SG_request_data_free( &reqdat );
   
   return rc;
}


// common fskit entry initialization from an exported inode
// return 0 on success (always succeeds)
static int UG_consistency_fskit_common_init( struct fskit_entry* fent, struct md_entry* inode_data ) {
   
   struct timespec ts;
   
   // set ctime, mtime
   ts.tv_sec = inode_data->mtime_sec;
   ts.tv_nsec = inode_data->mtime_nsec;
   fskit_entry_set_mtime( fent, &ts );
   
   ts.tv_sec = inode_data->ctime_sec;
   ts.tv_nsec = inode_data->ctime_nsec;
   fskit_entry_set_ctime( fent, &ts );
   
   // set size
   fskit_entry_set_size( fent, inode_data->size );
   
   return 0;
}

// generate a new fskit entry for a directory 
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if inode_data doesn't represent a file
static int UG_consistency_fskit_dir_init( struct fskit_entry* fent, struct fskit_entry* parent, struct md_entry* inode_data ) {
   
   int rc = 0;
   
   // sanity check 
   if( inode_data->type != MD_ENTRY_DIR ) {
      return -EINVAL;
   }
   
   rc = fskit_entry_init_dir( fent, parent, inode_data->file_id, inode_data->name, inode_data->owner, inode_data->volume, inode_data->mode );
   if( rc != 0 ) {
      
      return rc;
   }
   
   UG_consistency_fskit_common_init( fent, inode_data );
   
   return 0;
}

// generate a new fskit entry for a regular file 
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if inode_data doesn't represent a file
static int UG_consistency_fskit_file_init( struct fskit_entry* fent, struct md_entry* inode_data ) {
   
   int rc = 0;
   
   // sanity 
   if( inode_data->type != MD_ENTRY_FILE ) {
      return -EINVAL;
   }
   
   rc = fskit_entry_init_file( fent, inode_data->file_id, inode_data->name, inode_data->owner, inode_data->volume, inode_data->mode );
   if( rc != 0 ) {
      
      return rc;
   }
   
   UG_consistency_fskit_common_init( fent, inode_data );
   
   return 0;
}


// build an fskit entry from an exported inode and a manifest 
// return 0 on success
// return -ENOMEM on OOM 
static int UG_consistency_fskit_entry_init( struct fskit_core* fs, struct fskit_entry* fent, struct fskit_entry* parent, struct md_entry* inode_data, struct SG_manifest* manifest ) {
   
   int rc = 0;
   struct UG_inode* inode = NULL;
   bool manifest_alloced = false;
   
   // going from file to directory?
   if( inode_data->type == MD_ENTRY_FILE ) {
      
      // turn the inode into a directory, and blow away the file's data
      // set up the file 
      rc = UG_consistency_fskit_file_init( fent, inode_data );
      if( rc != 0 ) {
         
         return rc;
      }
   }
   else {
      
      // going to make a directory, and replace this file.
      // set up the directory 
      rc = UG_consistency_fskit_dir_init( fent, parent, inode_data );
      if( rc != 0 ) {
         
         return rc;
      }
   }
   
   // build the inode
   inode = SG_CALLOC( struct UG_inode, 1 );
   if( inode == NULL ) {
      
      fskit_entry_destroy( fs, fent, false );
      return -ENOMEM;
   }
   
   if( manifest == NULL ) {
      
      // build a manifest
      manifest = SG_CALLOC( struct SG_manifest, 1 );
      if( manifest == NULL ) {
         
         SG_safe_free( inode );
         fskit_entry_destroy( fs, fent, false );
         return -ENOMEM;
      }
      
      rc = SG_manifest_init( manifest, inode_data->volume, inode_data->coordinator, inode_data->file_id, inode_data->version );
      if( rc != 0 ) {
         
         SG_safe_free( inode );
         SG_safe_free( manifest );
         fskit_entry_destroy( fs, fent, false );
         return rc;
      }
      
      manifest_alloced = true;
   }
   
   // set up the inode 
   rc = UG_inode_init_from_export( inode, inode_data, manifest, fent );
   if( rc != 0 ) {
      
      if( manifest_alloced ) {
         
         SG_manifest_free( manifest );
         SG_safe_free( manifest );
      }
      
      fskit_entry_destroy( fs, fent, false );
      
      return rc;
   }
   
   // put the inode into the fent 
   fskit_entry_set_user_data( fent, inode );
   
   return 0;
}     


// replace one fskit_entry with another.
// deferred-delete the old fent.
// return 0 on success
// return -errno on failure 
// return EAGAIN if we successfully attached, but failed to remove the old fent
// NOTE: fent must be write-locked 
static int UG_consistency_fskit_entry_replace( struct SG_gateway* gateway, char const* fs_path, struct fskit_entry* parent, struct fskit_entry* fent, struct fskit_entry* new_fent ) {
   
   int rc = 0;

   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );
   
   // blow away this file/directory and replace it with the file/directory
   rc = fskit_entry_detach_lowlevel_force( parent, fent );
   if( rc != 0 ) {
      
      SG_error("fskit_entry_detach_lowlevel_force( '%s' ) rc = %d\n", fs_path, rc );
      
      fskit_entry_destroy( fs, new_fent, false );
      SG_safe_free( new_fent );
      
      return rc;
   }
   
   // put the new one in place
   rc = fskit_entry_attach_lowlevel( parent, new_fent );
   if( rc != 0 ) {
      
      SG_error("fskit_entry_attach_lowlevel( '%s' ) rc = %d\n", fs_path, rc );
      
      // NOTE: don't try to reinsert--the old one was gone either way
      fskit_entry_destroy( fs, new_fent, false );
      SG_safe_free( new_fent );
      
      return rc;
   }
   
   // attached
   // remember the parent 
   // new_fent->entry_parent = parent;
   
   // blow away the old fskit entry
   if( fskit_entry_get_type( fent ) == FSKIT_ENTRY_TYPE_DIR ) {
      
      rc = fskit_deferred_remove_all( fs, fs_path, fent );
   }
   else {
      
      rc = fskit_deferred_remove( fs, fs_path, fent );
   }
   
   // blow away the inode and all its cached data
   // (NOTE: don't care if this fails--it'll get reaped eventually)
   md_cache_evict_file( cache, fskit_entry_get_file_id( fent ), UG_inode_file_version( *inode ) );
   
   UG_inode_free( inode );
   inode = NULL;
   
   if( rc != 0 ) {
      
      SG_error("fskit_deferred_remove(_all)( '%s' ) rc = %d\n", fs_path, rc );
   }
   
   return rc;
}


// reload a single inode's metadata.
// * if the types don't match, the inode (and its children) will be dropped and a new inode with the new type will be created in its place.
// * if the versions don't match, then the inode will be reversioned 
// * for regular files, if the sizes don't match, then the inode will be truncated (i.e. evicting blocks if need be)
// * if the names don't match, the name will be changed.
// NOTE: fent must be write-locked
// NOTE: parent must be write-locked
// NOTE: fent might be replaced--don't access it after calling this method.
// return 0 on success
// return 1 if fent got replaced 
// return -ENOMEM on OOM
// return -errno on error
static int UG_consistency_inode_reload( struct SG_gateway* gateway, char const* fs_path, struct fskit_entry* parent, struct fskit_entry* fent, struct md_entry* inode_data ) {
   
   int rc = 0;
   struct fskit_entry* new_fent = NULL;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   // types don't match?
   if( !UG_inode_export_match_type( inode, inode_data ) ) {
      
      // make a new fskit entry for this
      new_fent = SG_CALLOC( struct fskit_entry, 1 );
      if( new_fent == NULL ) {
         return -ENOMEM;
      }
      
      // build the new fent
      rc = UG_consistency_fskit_entry_init( fs, new_fent, parent, inode_data, NULL );
      if( rc != 0 ) {
         
         // OOM 
         SG_safe_free( new_fent );
         return rc;
      }
      
      // replace in parent
      rc = UG_consistency_fskit_entry_replace( gateway, fs_path, parent, fent, new_fent );
      if( rc != 0 ) {
         
         SG_error("UG_consistency_fskit_entry_replace( '%s' ) rc = %d\n", fs_path, rc );
         
         if( rc < 0 ) {
            
            // failed to attach 
            struct UG_inode* new_inode = (struct UG_inode*)fskit_entry_get_user_data( new_fent );
            
            fskit_entry_destroy( fs, new_fent, false );
            SG_safe_free( new_fent );
            
            UG_inode_free( new_inode );
            SG_safe_free( new_inode );
            
            return rc;
         }
         else {
            
            // failed to garbage-collect 
            SG_error("LEAK: failed to garbage-collect old inode for '%s'.  Consider filing a bug report!\n", fs_path);
            rc = 0;
         }
      }
      
      // if this is a file, it's manifest is stale--we'll want to reload the block information as well
      if( fskit_entry_get_type( new_fent ) == FSKIT_ENTRY_TYPE_FILE ) {
         
         SG_manifest_set_stale( UG_inode_manifest( *inode ), true );
      }
      
      // replaced!
      // nothing more to do--the new inode has the right version, name, and size
      return 1;
   }
   
   // versions don't match?
   if( !UG_inode_export_match_version( inode, inode_data ) ) {
      
      // reversion--both metadata, and cached data 
      // NOTE: don't really care if cache reversioning fails--it'll get reaped eventually
      md_cache_reversion_file( cache, inode_data->file_id, UG_inode_file_version( *inode ), inode_data->version );
      SG_manifest_set_file_version( &inode->manifest, inode_data->version );
   }
   
   // file sizes don't match?
   if( fskit_entry_get_type( fent ) == FSKIT_ENTRY_TYPE_FILE && !UG_inode_export_match_size( inode, inode_data ) ) {
      
      // need to expand/truncate inode
      off_t size = fskit_entry_get_size( UG_inode_fskit_entry( *inode ) );
      off_t new_size = inode_data->size;
      
      if( size > new_size ) {
         
         // shrunk
         uint64_t max_block_id = (new_size / block_size);
         
         for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( UG_inode_manifest( *inode ) ); itr != SG_manifest_block_iterator_end( UG_inode_manifest( *inode ) ); itr++ ) {
            
            if( SG_manifest_block_iterator_id( itr ) <= max_block_id ) {
               continue;
            }
            
            // NOTE: don't really care if these fail; they'll get reaped eventually 
            md_cache_evict_block_async( cache, UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), SG_manifest_block_iterator_id( itr ), (SG_manifest_block_iterator_block( itr ))->block_version );
         }
         
         SG_manifest_truncate( UG_inode_manifest( *inode ), max_block_id );
      }
      
      SG_manifest_set_size( UG_inode_manifest( *inode ), new_size );
   }
   
   // names don't match?
   if( UG_inode_export_match_name( inode, inode_data ) <= 0 ) {
      
      // inode got renamed 
      rc = fskit_entry_rename_in_directory( parent, fent, inode_data->name );
      if( rc != 0 ) {
         
         // OOM 
         return rc;
      }
   }
   
   // manifest timestamps don't match, and we don't coordinate this file?
   if( fskit_entry_get_type( fent ) == FSKIT_ENTRY_TYPE_FILE && UG_inode_coordinator_id( *inode ) != SG_gateway_id( gateway ) &&
      (inode_data->manifest_mtime_sec != SG_manifest_get_modtime_sec( UG_inode_manifest( *inode ) ) || inode_data->manifest_mtime_nsec != SG_manifest_get_modtime_nsec( UG_inode_manifest( *inode ) ) ) ) {
      
      SG_manifest_set_stale( UG_inode_manifest( *inode ), true );
   }
   
   // xattr nonces don't match?
   if( inode_data->xattr_nonce != UG_inode_xattr_nonce( *inode ) ) {
      
      // clear them out 
      fskit_fremovexattr_all( fs, fent );
   }
   
   // reload everything else 
   rc = UG_inode_import( inode, inode_data );
   
   if( rc == 0 ) {
      
      // reloaded!
      // no longer stale
      inode->read_stale = false;
      clock_gettime( CLOCK_REALTIME, &inode->refresh_time );
      
      // only update the manifest refresh time if we're NOT the coordinator
      if( UG_inode_coordinator_id( *inode ) != SG_gateway_id( gateway ) ) { 
         
         SG_manifest_set_modtime( UG_inode_manifest( *inode ), inode_data->manifest_mtime_sec, inode_data->manifest_mtime_nsec );
      }
   }
   
   return rc;
}


// free a graft--a chain of fskit_entry structures built from UG_consistency_fskit_path_graft_build.
// do not detach the inodes--we don't want to run the unlink routes.
// alwyas succeeds
static int UG_consistency_fskit_path_graft_free( struct fskit_core* fs, struct fskit_entry* graft_parent, struct md_entry* path_data, size_t path_len ) {
   
   if( graft_parent == NULL ) {
      return 0;
   }
   
   struct fskit_entry* graft_child = NULL;
   int i = 0;
   
   while( (unsigned)i < path_len ) {
      
      // search graft parent 
      graft_child = fskit_dir_find_by_name( graft_parent, path_data[i].name );
      if( graft_child == NULL ) {
         
         // done 
         break;
      }
      
      // destroy graft parent 
      fskit_entry_destroy( fs, graft_parent, false );
      SG_safe_free( graft_parent );
      
      graft_parent = graft_child;
      
      i++;
   }
   
   return 0;
}


// construct a graft--a chain of fskit_entry structures--from an ordered list of inode metadata.
// do not attach it to fskit; just build it up 
// return 0 on success, and set *graft_root to be the root of the graft.  graft_root will have no parent (i.e. a NULL parent for "..")
// return -EINVAL on invalid data (i.e. the path_data contains a non-leaf directory, etc.)
// return -ENOMEM on OOM
static int UG_consistency_fskit_path_graft_build( struct SG_gateway* gateway, struct md_entry* path_data, size_t path_len, struct fskit_entry** graft_root ) {
   
   int rc = 0;
   
   struct fskit_entry* graft_parent = NULL;
   struct fskit_entry* graft_child = NULL;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   // sanity check--all path_data elements except the leaf must be directories
   for( size_t i = 0; i < path_len - 1; i++ ) {
      
      if( path_data[i].type != MD_ENTRY_DIR ) {
         
         return -EINVAL;
      }
   }
   
   for( size_t i = 0; i < path_len; i++ ) {
      
      // next child 
      graft_child = SG_CALLOC( struct fskit_entry, 1 );
      if( graft_child == NULL ) {
         
         rc = -ENOMEM;
         UG_consistency_fskit_path_graft_free( fs, *graft_root, path_data, path_len );
         
         return rc;
      }
      
      // build the inode
      rc = UG_consistency_fskit_entry_init( fs, graft_child, graft_parent, &path_data[i], NULL );
      if( rc != 0 ) {
         
         SG_error("UG_consistency_fskit_entry_init( %" PRIX64 " (%s) ) rc = %d\n", path_data[i].file_id, path_data[i].name, rc );
         
         UG_consistency_fskit_path_graft_free( fs, *graft_root, path_data, path_len );
         return rc;
      }
      
      // insert the inode into its parent (except for the root, which we'll do later)
      if( graft_parent != NULL ) {
            
         rc = fskit_entry_attach_lowlevel( graft_parent, graft_child );
         if( rc != 0 ) {
            
            SG_error("fskit_entry_attach_lowlevel( %" PRIX64 " --> %" PRIX64 " (%s) ) rc = %d\n", fskit_entry_get_file_id( graft_parent ), path_data[i].file_id, path_data[i].name, rc );
            
            UG_consistency_fskit_path_graft_free( fs, *graft_root, path_data, path_len );
            return rc;
         }
      }
      
      // set *graft_root if this is the first 
      if( i == 0 ) {
         
         *graft_root = graft_child;
      }
      
      // next entry
      graft_parent = graft_child;
      graft_child = NULL;
   }
   
   // success!
   return 0;
}


// attach a graft to an fskit_entry, based on its parent's ID and the path that the graft was generated from.
// return 0 on success
// return -ENOENT if the parent could not be found 
// return -EEXIST if there is an existing entry with graft_root's name 
// return -ENOTDIR if the parent is not a directory
// return -ENOMEM on OOM 
static int UG_consistency_fskit_path_graft_attach( struct SG_gateway* gateway, char const* fs_path, uint64_t parent_id, struct fskit_entry* graft_root ) {
   
   int rc = 0;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   // struct UG_inode* graft_root_inode = NULL;
   
   char* graft_root_name = NULL;
   bool attached = false;
   
   struct fskit_path_iterator itr;
   
   graft_root_name = fskit_entry_get_name( graft_root );
   if( graft_root_name == NULL ) {
      
      return -ENOMEM;
   }
   
   // find the attachment point
   for( itr = fskit_path_begin( fs, fs_path, true ); !fskit_path_end( &itr ); fskit_path_next( &itr ) ) {
      
      // current entry 
      struct fskit_entry* cur = fskit_path_iterator_entry( &itr );
      struct fskit_entry* collision = NULL;
      
      if( fskit_entry_get_file_id( cur ) == parent_id ) {
         
         // has to be a dir 
         if( fskit_entry_get_type( cur ) != FSKIT_ENTRY_TYPE_DIR ) {
            
            rc = -ENOTDIR;
            break;
         }
         
         // graft point exists already?
         collision = fskit_dir_find_by_name( cur, graft_root_name );
         if( collision != NULL ) {
            
            // exists 
            rc = -EEXIST;
            break;
         }
         
         // attach!
         rc = fskit_entry_attach_lowlevel( cur, graft_root );
         
         if( rc == 0 ) {
            attached = true;
            
            // remember the parent
            // graft_root_inode = (struct UG_inode*)fskit_entry_get_user_data( graft_root );
            // graft_root_inode->entry_parent = cur;
         }
         
         break;
      }
   }
   
   // done with this iterator
   fskit_path_iterator_release( &itr );
   
   SG_safe_free( graft_root_name );
   
   if( rc == 0 && !attached ) {
      
      // that's odd--no point to attach to 
      rc = -ENOENT;
   }
   
   return rc;
}


// build up an ms_path_t of locally-cached but stale fskit entries 
// return 0 on success 
// return -ENOMEM on OOM 
static int UG_consistency_path_local_stale( struct SG_gateway* gateway, char const* fs_path, struct timespec* refresh_begin, ms_path_t* path_local ) {
   
   int rc = 0;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct fskit_path_iterator itr;
   
   for( itr = fskit_path_begin( fs, fs_path, false ); !fskit_path_end( &itr ); fskit_path_next( &itr ) ) {
      
      struct fskit_entry* cur = fskit_path_iterator_entry( &itr );
      
      struct ms_path_ent path_ent;
      
      struct UG_inode* inode = (struct UG_inode*)fskit_entry_get_user_data( cur );
      
      // is this inode stale?  skip if not
      if( !UG_inode_is_read_stale( *inode, refresh_begin ) ) {
         continue;
      }
      
      rc = ms_client_getattr_request( &path_ent, UG_inode_volume_id( *inode ), UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), UG_inode_write_nonce( *inode ), NULL );
      if( rc != 0 ) {
         
         // OOM 
         break;
      }
      
      try {
         
         path_local->push_back( path_ent );
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         break;
      }
   }
   
   // done with this iterator
   fskit_path_iterator_release( &itr );
   
   return rc;
}


// reload cached stale metadata entries from inode data.
// if the MS indicates that an inode got removed remotely, then delete the cached inode locally and all of its children (if it has any) and terminate.
// NOTE: inode_data must be in the same order as the inodes that appear in fskit.
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if the order of inode_data is out-of-whack with fskit
static int UG_consistency_path_stale_reload( struct SG_gateway* gateway, char const* fs_path, struct md_entry* inode_data, size_t num_inodes ) {
   
   int rc = 0;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   char* name = NULL;           // inode name in fskit
   size_t inode_i = 0;          // indexes inode_data
   
   if( num_inodes == 0 ) {
      return 0;
   }
   
   struct fskit_path_iterator itr;
   
   // reload each stale inode 
   for( itr = fskit_path_begin( fs, fs_path, true ); !fskit_path_end( &itr ); fskit_path_next( &itr ) ) {
      
      struct fskit_entry* cur = fskit_path_iterator_entry( &itr );
      struct fskit_entry* parent = fskit_path_iterator_entry_parent( &itr );
      
      // next datum 
      struct md_entry* inode_datum = &inode_data[ inode_i ];
      
      // is this the fskit entry to reload?
      if( fskit_entry_get_file_id( cur ) != inode_datum->file_id ) {
         
         // nope--this one's fresh 
         continue;
      }
      
      // is there any change to reload?
      if( inode_datum->error == MS_LISTING_NOCHANGE ) {
         
         // nope--nothing to do 
         inode_i++;
         continue;
      }
      
      // does this inode exist on the MS?
      if( inode_datum->error == MS_LISTING_NONE ) {
         
         // nope--this inode and everything beneath it got unlinked remotely
         // blow them all away locally
         char const* method = NULL;
         
         char* path_stump = fskit_path_iterator_path( &itr );
         if( path_stump == NULL ) {
            
            rc = -ENOMEM;
            break;
         }
         
         if( fskit_entry_get_type( cur ) == FSKIT_ENTRY_TYPE_FILE ) {
            
            method = "fskit_deferred_remove";
            rc = fskit_deferred_remove( fs, path_stump, cur );
         }
         else {
            
            method = "fskit_deferred_remove_all";
            rc = fskit_deferred_remove_all( fs, path_stump, cur );
         }
         
         if( rc != 0 ) {
            
            SG_error( "%s('%s') rc = %d\n", method, path_stump, rc );
         }
         
         SG_safe_free( path_stump );
         
         // done iterating
         fskit_path_iterator_release( &itr );
         return rc;
      }
      
      // name of this inode, in case it gets blown away?
      name = fskit_entry_get_name( cur );
      if( name == NULL ) {
         
         rc = -ENOMEM;
         break;
      }
         
      // reload 
      rc = UG_consistency_inode_reload( gateway, fs_path, parent, cur, inode_datum );
   
      if( rc < 0 ) {
         
         SG_error("UG_consistency_inode_reload( '%s' (at %" PRIX64 " (%s))) rc = %d\n", fs_path, fskit_entry_get_file_id( cur ), name, rc );
         
         SG_safe_free( name );
         
         break;
      }
      
      if( rc > 0 ) {
      
         // cur got replaced.
         // reload it 
         cur = fskit_dir_find_by_name( parent, name );
         
         if( cur == NULL ) {
            
            // not found--this and all inodes beneath us are gone
            SG_safe_free( name );
            
            rc = -ENOENT;
            break;
         }
      }
      
      SG_safe_free( name );
      
      // success!  next entry
      inode_i++;
   }
   
   // done iterating
   fskit_path_iterator_release( &itr );
   
   return rc;
}


// build up a path of download requests for remote entries
// return 0 on success, and fill in *path_remote with remote paths
// return -ENOMEM on OOM 
static int UG_consistency_path_remote( struct SG_gateway* gateway, char const* fs_path, ms_path_t* path_remote ) {

   int rc = 0;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   char* path_dup = NULL;
   
   struct UG_inode* inode = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct fskit_path_iterator itr;
   
   struct ms_path_ent deepest_ent;
   uint64_t deepest_ent_parent_id = 0;
   uint64_t deepest_ent_file_id = 0;
   char* deepest_ent_name = NULL;
   
   int depth = 0;
   char** names = NULL;

   // in order to build up the contents of path_remote, we need
   // the first entry of path_remote to have information known 
   // to the deepest known fskit entry (volume_id, file_id, name, parent_id).
   // the remaining entries only need names and volume_id.
   // find this parent, and populate it.
   // This means, find the entry at the end of the locally-cached path, but keep track of how deep it is too.
   for( itr = fskit_path_begin( fs, fs_path, false ); !fskit_path_end( &itr ); fskit_path_next( &itr ) ) {
      
      struct fskit_entry* cur = fskit_path_iterator_entry( &itr );
      
      char* name = fskit_entry_get_name( cur );
      if( name == NULL ) {
         
         // OOM!
         rc = -ENOMEM;
         break;
      }
      
      SG_safe_free( deepest_ent_name );
      
      deepest_ent_parent_id = deepest_ent_file_id;
      deepest_ent_file_id = UG_inode_file_id( *inode );
      deepest_ent_name = name;
      
      depth++;
   }
   
   // done iterating
   fskit_path_iterator_release( &itr );
   
   // failed?
   if( rc != 0 ) {
      
      SG_safe_free( deepest_ent_name );
      return rc;
   }
   
   // should have hit ENOENT...
   if( fskit_path_iterator_error( &itr ) == 0 ) {
      
      // nothing to do!
      SG_safe_free( deepest_ent_name );
      return 0;
   }
   else if( fskit_path_iterator_error( &itr ) != -ENOENT ) {
      
      // some other error...
      SG_error("fskit_path_iterator_error('%s') rc = %d\n", fs_path, fskit_path_iterator_error( &itr ) );
      SG_safe_free( deepest_ent_name );
      return fskit_path_iterator_error( &itr );
   }
   
   // build the head of the remote path 
   rc = ms_client_path_download_ent_head( &deepest_ent, volume_id, deepest_ent_file_id, deepest_ent_parent_id, deepest_ent_name, NULL );
   SG_safe_free( deepest_ent_name );
   
   if( rc != 0 ) {
      
      // OOM 
      return rc;
   }
   
   try {
      
      path_remote->push_back( deepest_ent );
   }
   catch( bad_alloc& ba ) {
      
      ms_client_free_path_ent( &deepest_ent, NULL );
      return -ENOMEM;
   }
   
   // get tail names
   path_dup = SG_strdup_or_null( fs_path );
   if( path_dup == NULL ) {
      
      ms_client_free_path( path_remote, NULL );
      return -ENOMEM;
   }
   
   rc = fskit_path_split( path_dup, &names );
   if( rc != 0 ) {
      
      SG_safe_free( path_dup );
      ms_client_free_path( path_remote, NULL );
      return -ENOMEM;
   }
   
   // build the tail
   for( size_t i = depth; names[i] != NULL; i++ ) {
      
      struct ms_path_ent ms_ent;
      
      rc = ms_client_path_download_ent_tail( &ms_ent, volume_id, names[i], NULL );
      if( rc != 0 ) {
         
         ms_client_free_path( path_remote, NULL );
         SG_safe_free( path_dup );
         return rc;
      }
      
      try {
         
         path_remote->push_back( ms_ent );
      }
      catch( bad_alloc& ba ) {
         
         ms_client_free_path( path_remote, NULL );
         SG_safe_free( path_dup );
         return -ENOMEM;
      }
   }
   
   SG_safe_free( path_dup );
   
   // built!
   return 0;
}


// reload a path of metadata 
// cached path entries will be revalidated--reloaded, or dropped if they are no longer present upstream 
// un-cached path entries will be downloaded and grafted into the fskit filesystem 
// return 0 on success 
// return -ENOMEM on OOM 
// return -errno on failure to connect
int UG_consistency_path_ensure_fresh( struct SG_gateway* gateway, char const* fs_path ) {
   
   int rc = 0;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   ms_path_t path_local;
   ms_path_t path_remote;
   
   struct timespec refresh_start;
   
   struct ms_client_multi_result remote_inodes;
   
   struct fskit_entry* graft_root = NULL;
   
   clock_gettime( CLOCK_REALTIME, &refresh_start );
   
   // find all local stale nodes 
   rc = UG_consistency_path_local_stale( gateway, fs_path, &refresh_start, &path_local );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_local_stale( '%s' ) rc = %d\n", fs_path, rc );
      return rc;
   }
   
   // refresh stale data 
   rc = ms_client_getattr_multi( ms, &path_local, &remote_inodes );
   ms_client_free_path( &path_local, NULL );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_getattr_multi('%s') rc = %d\n", fs_path, rc );
      return rc;
   }
   
   // load downloaded inodes into the fskit filesystem tree 
   rc = UG_consistency_path_stale_reload( gateway, fs_path, remote_inodes.ents, remote_inodes.num_ents );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_stale_reload('%s') rc = %d\n", fs_path, rc );
        
      return rc;
   }
   
   ms_client_multi_result_free( &remote_inodes );
   
   // which inodes are remote?
   rc = UG_consistency_path_remote( gateway, fs_path, &path_remote );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_path_remote('%s') rc = %d\n", fs_path, rc );
      
      return rc;
   }
   
   // are any remote?
   if( path_remote.size() == 0 ) {
      
      // done!
      return 0;
   }
   
   // fetch remote inodes
   rc = ms_client_path_download( ms, &path_remote, &remote_inodes );
   if( rc != 0 ) {
      
      SG_error("ms_client_download_path('%s') rc = %d\n", fs_path, rc );
      
      return rc;
   }
   
   // build a graft from them 
   rc = UG_consistency_fskit_path_graft_build( gateway, remote_inodes.ents, remote_inodes.num_ents, &graft_root );
   if( rc != 0 ) {
      
      ms_client_free_path( &path_remote, NULL );
      
      SG_error("UG_consistency_fskit_path_graft_build('%s') rc = %d\n", fs_path, rc );
      return rc;
   }
   
   // graft absent inodes into fskit 
   rc = UG_consistency_fskit_path_graft_attach( gateway, fs_path, path_remote.at(0).parent_id, graft_root );
   if( rc != 0 ) {
      
      UG_consistency_fskit_path_graft_free( fs, graft_root, remote_inodes.ents, remote_inodes.num_ents );
      
      char* tmp = fskit_entry_get_name( graft_root );
      SG_error("UG_consistency_fskit_path_graft_attach('%s' (at %" PRIX64 " (%s)) ) rc = %d\n", fs_path, fskit_entry_get_file_id( graft_root ), tmp, rc );
      
      SG_safe_free( tmp );
      ms_client_free_path( &path_remote, NULL );
   
      return rc;
   }
   
   // finished!
   ms_client_free_path( &path_remote, NULL );
   
   return 0;
}


// merge a list of md_entrys into an fskit_entry directory.
// for conflicts, if a local entry is newer than the given cut-off, keep it.  Otherwise replace it.
// return 0 on success
// return -ENOMEM on OOM 
// NOTE: dent must be write-locked!
static int UG_consistency_dir_merge( struct SG_gateway* gateway, char const* fs_path_dir, struct fskit_entry* dent, struct md_entry* ents, size_t num_ents, struct timespec* keep_cutoff ) {
   
   int rc = 0;
   char* fs_path = NULL;
   size_t max_name_len = 0;
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   // set up the fs_path buffer 
   for( size_t i = 0; i < num_ents; i++ ) {
      
      size_t len = strlen( ents[i].name );
      if( len > max_name_len ) {
         max_name_len = len;
      }
   }
   
   fs_path = SG_CALLOC( char, strlen(fs_path_dir) + 1 + max_name_len + 1 );
   if( fs_path == NULL ) {
      
      return -ENOMEM;
   }
   
   for( size_t i = 0; i < num_ents; i++ ) {
      
      struct md_entry* ent = &ents[i];
      
      if( ent->name == NULL ) {
         continue;
      }
      
      struct fskit_entry* fent = fskit_dir_find_by_name( dent, ent->name );
      
      int64_t ctime_sec = 0;
      int32_t ctime_nsec = 0;
      
      struct timespec ctime;
      
      if( fent != NULL ) {
         
         fskit_fullpath( fs_path_dir, ent->name, fs_path );
         
         fskit_entry_wlock( fent );
         
         // do we replace?
         // when was this entry created?
         fskit_entry_get_ctime( fent, &ctime_sec, &ctime_nsec );
         
         ctime.tv_sec = ctime_sec;
         ctime.tv_nsec = ctime_nsec;
         
         if( md_timespec_diff_ms( &ctime, keep_cutoff ) < 0 ) {
            
            // fent was created before the reload, and is in conflict.  reload
            rc = UG_consistency_inode_reload( gateway, fs_path, dent, fent, ent );
            if( rc < 0 ) {
               
               SG_error("UG_consistency_inode_reload('%s') rc = %d\n", fs_path, rc );
               
               // try to soldier on...
               rc = 0;
               
               fskit_entry_unlock( fent );
            }
            else if( rc == 0 ) {
               
               // reloaded, but not replaced
               fskit_entry_unlock( fent );
            }
         }
         
         else {
            
            // preserve this entry
            fskit_entry_unlock( fent );
         }
      }
      else {
         
         // insert this entry 
         fent = SG_CALLOC( struct fskit_entry, 1 );
         if( fent == NULL ) {
            
            rc = -ENOMEM;
            break;
         }
         
         rc = UG_consistency_fskit_entry_init( fs, fent, dent, ent, NULL );
         if( rc != 0 ) {
            
            SG_error("UG_consistency_fskit_entry_init('%s') rc = %d\n", fs_path, rc );
            
            SG_safe_free( fent );
            break;
         }
         
         rc = fskit_entry_attach_lowlevel( dent, fent );
         if( rc != 0 ) {
            
            SG_error("fskit_entry_attach_lowlevel('%s', '%s') rc = %d\n", fs_path_dir, ent->name, rc );
            
            fskit_entry_destroy( fs, fent, false );
            SG_safe_free( fent );
            break;
         }
      }
   }
   
   SG_safe_free( fs_path );
   return rc;
}


// ensure that a directory has a fresh listing of children
// if not, fetch the immediate children the named directory, and attach them all
// return 0 on success
// return -ENOMEM on OOM
int UG_consistency_dir_ensure_fresh( struct SG_gateway* gateway, char const* fs_path ) {
   
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   struct UG_inode* inode = NULL;
   
   uint64_t file_id = 0;
   int64_t num_children = 0;
   int64_t least_unknown_generation = 0;
   int64_t max_read_freshness = 0;
   int64_t capacity = 0;
   
   struct timespec now;
   struct timespec dir_refresh_time;
   
   struct ms_client_multi_result results;
   
   char const* method = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   struct fskit_entry* dent = fskit_entry_resolve_path( fs, fs_path, 0, 0, true, &rc );
   if( dent == NULL ) {
      
      return rc;
   }
   
   rc = clock_gettime( CLOCK_REALTIME, &now );
   if( rc != 0 ) {
      
      rc = -errno;
      fskit_entry_unlock( dent );
      
      SG_error("clock_gettime rc = %d\n", rc );
      return rc;
   }
   
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( dent );
   
   dir_refresh_time = UG_inode_refresh_time( *inode );
   max_read_freshness = UG_inode_max_read_freshness( *inode );
   
   // is the inode's directory listing still fresh?
   if( md_timespec_diff_ms( &now, &dir_refresh_time ) <= max_read_freshness ) {
      
      // still fresh 
      fskit_entry_unlock( dent );
      return 0;
   }
   
   // stale--redownload
   file_id = fskit_entry_get_file_id( dent );
   num_children = UG_inode_ms_num_children( *inode );
   least_unknown_generation = UG_inode_generation( *inode );
   capacity = UG_inode_ms_capacity( *inode );
   
   // reference dent--it must stick around 
   fskit_entry_ref_entry( dent );
   
   fskit_entry_unlock( dent );

   // have we listed before?
   if( least_unknown_generation == 0 ) {
      
      // nope--full download
      method = "ms_client_listdir";
      rc = ms_client_listdir( ms, file_id, num_children, capacity, &results );
   }
   else {
      
      method = "ms_client_diffdir";
      rc = ms_client_diffdir( ms, file_id, num_children, least_unknown_generation + 1, &results );
   }
   
   if( rc < 0 ) {
      
      SG_error("%s('%s') rc = %d\n", method, fs_path, rc );
      fskit_entry_unref( fs, fs_path, dent );
      
      return rc;
   }
   
   if( results.reply_error != 0 ) {
      
      SG_error("%s('%s') reply_error = %d\n", method, fs_path, rc );
      fskit_entry_unref( fs, fs_path, dent );
      
      return rc;
   }
   
   // re-acquire 
   fskit_entry_wlock( dent );
   
   // load them in 
   rc = UG_consistency_dir_merge( gateway, fs_path, dent, results.ents, results.num_ents, &now );
   
   fskit_entry_unlock( dent );
   
   if( rc != 0 ) {
      
      SG_error("UG_consistency_dir_merge('%s') rc = %d\n", fs_path, rc );
   }
   
   fskit_entry_unref( fs, fs_path, dent );
   return rc;
}

