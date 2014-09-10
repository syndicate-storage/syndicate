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

#include "vacuumer.h"
#include "replication.h"
#include "network.h"
#include "sync.h"
#include "libsyndicate/ms/vacuum.h"

static void* vacuumer_main( void* arg );

int fs_entry_vacuumer_rlock( struct fs_vacuumer* vac ) {
   return pthread_rwlock_rdlock( &vac->vacuum_set_lock );
}

int fs_entry_vacuumer_wlock( struct fs_vacuumer* vac ) {
   return pthread_rwlock_wrlock( &vac->vacuum_set_lock );
}

int fs_entry_vacuumer_unlock( struct fs_vacuumer* vac ) {
   return pthread_rwlock_unlock( &vac->vacuum_set_lock );
}

int fs_entry_vacuumer_pending_rlock( struct fs_vacuumer* vac ) {
   return pthread_rwlock_rdlock( &vac->vacuum_pending_lock );
}

int fs_entry_vacuumer_pending_wlock( struct fs_vacuumer* vac ) {
   return pthread_rwlock_wrlock( &vac->vacuum_pending_lock );
}

int fs_entry_vacuumer_pending_unlock( struct fs_vacuumer* vac ) {
   return pthread_rwlock_unlock( &vac->vacuum_pending_lock );
}


// initialize the vacuumer 
int fs_entry_vacuumer_init( struct fs_vacuumer* vac, struct fs_core* core ) {
   
   memset( vac, 0, sizeof(struct fs_vacuumer) );
   
   vac->vacuum_set = new vacuum_set_t();
   
   vac->vacuum_pending_1 = new vacuum_set_t();
   vac->vacuum_pending_2 = new vacuum_set_t();
   vac->vacuum_pending = vac->vacuum_pending_1;
   
   vac->core = core;
   
   pthread_rwlock_init( &vac->vacuum_set_lock, NULL );
   pthread_rwlock_init( &vac->vacuum_pending_lock, NULL );
   
   vac->running = false;
   return 0;
}

// shut down the vacuumer 
int fs_entry_vacuumer_shutdown( struct fs_vacuumer* vac ) {
   if( vac->running )
      return -EINVAL;
   
   delete vac->vacuum_set;
   vac->vacuum_set = NULL;
   
   delete vac->vacuum_pending_1;
   delete vac->vacuum_pending_2;
   
   vac->vacuum_pending = NULL;
   vac->vacuum_pending_1 = NULL;
   vac->vacuum_pending_2 = NULL;
   
   pthread_rwlock_destroy( &vac->vacuum_set_lock );
   pthread_rwlock_destroy( &vac->vacuum_pending_lock );
   
   return 0;
}

// start the vacuumer 
int fs_entry_vacuumer_start( struct fs_vacuumer* vac ) {
   
   vac->running = true;
   
   vac->thread = md_start_thread( vacuumer_main, vac, false );
   if( vac->thread < 0 ) {
      vac->running = false;
      errorf("failed to start vacuumer, rc = %d\n", (int)vac->thread );
      return vac->thread;
   }
   
   return 0;
}

// stop the vacuumer 
int fs_entry_vacuumer_stop( struct fs_vacuumer* vac ) {
   
   vac->running = false;
   
   pthread_cancel( vac->thread );
   pthread_join( vac->thread, NULL );
   
   return 0;
}

// free a vacuumer request 
static int fs_entry_vacuumer_request_free( struct fs_vacuumer_request* vreq ) {
   if( vreq->fs_path ) {
      free( vreq->fs_path );
      vreq->fs_path = NULL;
   }
   
   return 0;
}

// move pending to the queue 
// vac must be wlock'ed, and not pending wlock'ed
static int fs_entry_vacuumer_add_pending( struct fs_vacuumer* vac ) {
   
   vacuum_set_t* pending = NULL;
   
   // detach pending 
   fs_entry_vacuumer_pending_wlock( vac );
   
   pending = vac->vacuum_pending;
   
   if( vac->vacuum_pending == vac->vacuum_pending_1 ) {
      vac->vacuum_pending = vac->vacuum_pending_2;
   }
   else {
      vac->vacuum_pending = vac->vacuum_pending_1;
   }
   
   fs_entry_vacuumer_pending_unlock( vac );
   
   // now, vac->pending points to a different vacuum list than before, and no writes will occur to pending outside this thread.
   vac->vacuum_set->insert( pending->begin(), pending->end() );
   
   pending->clear();
   
   return 0;
}


// add a vaccum_write request to the vacuumer
int fs_entry_vacuumer_write_bg( struct fs_vacuumer* vac, char const* fs_path, struct replica_snapshot* snapshot ) {
   
   if( !vac->running )
      return -ENOTCONN;
   
   struct fs_vacuumer_request vreq;
   memset( &vreq, 0, sizeof(struct fs_vacuumer_request ) );
   
   // set up the request 
   vreq.type = VACUUM_TYPE_WRITE;
   vreq.fs_path = strdup(fs_path);
   
   memcpy( &vreq.fent_snapshot, snapshot, sizeof(struct replica_snapshot) );
   
   fs_entry_vacuumer_pending_wlock( vac );
   
   vac->vacuum_pending->insert( vreq );
   
   fs_entry_vacuumer_pending_unlock( vac );
   
   return 0;
}

// add a vacuum_write request to the vacuumer, and mark the fent as being vacuumed 
// fent must be write-locked 
int fs_entry_vacuumer_write_bg_fent( struct fs_vacuumer* vac, char const* fs_path, struct fs_entry* fent ) {
   
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( vac->core, fent, 0, 0, &fent_snapshot );
   
   int rc = fs_entry_vacuumer_write_bg( vac, fs_path, &fent_snapshot );
   
   if( rc == 0 ) {
      // mark as vacuuming 
      fent->vacuuming = true;
   }
   
   return rc;
}
   

// add a vacuum-log request to the vacuumer 
int fs_entry_vacuumer_log_entry_bg( struct fs_vacuumer* vac, char const* fs_path, struct replica_snapshot* snapshot ) {
   
   if( !vac->running )
      return -ENOTCONN;
   
   struct fs_vacuumer_request vreq;
   memset( &vreq, 0, sizeof(struct fs_vacuumer_request ) );
   
   // set up the request 
   vreq.type = VACUUM_TYPE_LOG;
   vreq.fs_path = strdup( fs_path );
   
   memcpy( &vreq.fent_snapshot, snapshot, sizeof(struct replica_snapshot) );
   
   fs_entry_vacuumer_pending_wlock( vac );
   
   vac->vacuum_pending->insert( vreq );
   
   fs_entry_vacuumer_pending_unlock( vac );
   
   return 0;
}

// build a garbage modification map from a manifest and a list of write-affected blocks.
// return -EINVAL if the affected blocks aren't in the manifest
static int fs_entry_vacuumer_get_garbage_block_info( Serialization::ManifestMsg* manifest_msg, uint64_t* affected_blocks, size_t num_affected_blocks, modification_map* garbage ) {
   
   // verify that the affected blocks are present 
   for( size_t k = 0; k < num_affected_blocks; k++ ) {
      
      uint64_t affected_block_id = affected_blocks[k];
         
      for( int i = 0; i < manifest_msg->block_url_set_size(); i++ ) {
         Serialization::BlockURLSetMsg busmsg = manifest_msg->block_url_set( i );
         
         // make sure version and hash lengths match up
         if( busmsg.block_versions_size() != busmsg.block_hashes_size() ) {
            errorf("Manifest message len(block_versions) == %u differs from len(block_hashes) == %u\n", busmsg.block_versions_size(), busmsg.block_hashes_size() );
            
            fs_entry_free_modification_map( garbage );
            
            return -EINVAL;
         }
         
         // is this block in this block url set?
         if( affected_block_id < busmsg.start_id() || affected_block_id >= busmsg.end_id() )
            continue;
         
         // find the version of this block 
         for( int j = 0; j < busmsg.block_versions_size(); j++ ) {
            
            // validate length
            if( busmsg.block_hashes(j).size() != BLOCK_HASH_LEN() ) {
               errorf("Block URL set hash length for block %" PRIu64 " is %zu, which differs from expected %zu\n", (uint64_t)(busmsg.start_id() + j), busmsg.block_hashes(j).size(), BLOCK_HASH_LEN() );
               
               fs_entry_free_modification_map( garbage );
               
               return -EINVAL;
            }
            
            // make the garbage block info
            struct fs_entry_block_info binfo;
            memset( &binfo, 0, sizeof(struct fs_entry_block_info) );
            
            unsigned char* hash = CALLOC_LIST( unsigned char, BLOCK_HASH_LEN() );
            memcpy( hash, busmsg.block_hashes(j).data(), BLOCK_HASH_LEN() );
            
            fs_entry_block_info_garbage_init( &binfo, busmsg.block_versions(j), hash, BLOCK_HASH_LEN(), busmsg.gateway_id() );
            
            (*garbage)[ affected_block_id ] = binfo;
         }
      }
   }
   
   return 0;
}


// get a request's manifest
// if fent is NULL, it will be resolved
static int fs_entry_vacuumer_get_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, Serialization::ManifestMsg* manifest_msg ) {
   
   int err = 0;
   int rc = 0;
   bool resolved = false;
   
   if( fent == NULL ) {
      // resolve 
      fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
      if( fent == NULL || err != 0 ) {
         if( err == 0 )
            err = -ENOMEM;
         
         return err;
      }
      
      resolved = true;
   }
   
   // get the manifest from an RG
   rc = fs_entry_download_manifest_replica( core, fs_path, fent, manifest_mtime_sec, manifest_mtime_nsec, manifest_msg, NULL );
   if( rc != 0 ) {
      errorf("fs_entry_download_manifest_replica( %s %" PRIX64 " ) rc = %d\n", fs_path, fent->file_id, rc);
      
      if( resolved )
         fs_entry_unlock( fent );
      
      return rc;
   }
   
   if( resolved )
      fs_entry_unlock( fent );
   
   return 0;
}


// vacuum a specific write's data, in the background
static int fs_entry_vacuumer_vacuum_data_bg( struct fs_core* core, char const* fs_path, struct replica_snapshot* fent_snapshot,
                                             Serialization::ManifestMsg* manifest_msg, uint64_t* affected_blocks, size_t num_affected_blocks ) {
   
   int rc = 0;
   modification_map garbage;
   
   int64_t file_version = manifest_msg->file_version();
   int64_t manifest_mtime_sec = manifest_msg->mtime_sec();
   int32_t manifest_mtime_nsec = manifest_msg->mtime_nsec();
   
   // duplicate the snapshot, using the manifest modtime and version 
   struct replica_snapshot fent_gc_snapshot;
   memcpy( &fent_gc_snapshot, fent_snapshot, sizeof(struct replica_snapshot) );
   
   fent_gc_snapshot.file_version = file_version;
   fent_gc_snapshot.manifest_mtime_sec = manifest_mtime_sec;
   fent_gc_snapshot.manifest_mtime_nsec = manifest_mtime_nsec;
   
   // build up a modification_map for the affected blocks 
   rc = fs_entry_vacuumer_get_garbage_block_info( manifest_msg, affected_blocks, num_affected_blocks, &garbage );
   if( rc != 0 ) {
      errorf("fs_entry_vacuumer_get_garbage_block_info(%" PRIX64 "%" PRId64 "/manifest.%" PRId64 ".%d) rc = %d\n", fent_gc_snapshot.file_id, file_version, manifest_mtime_sec, manifest_mtime_nsec, rc );
      return -EINVAL;
   }
   
   // erase it, using the garbage collecter thread 
   rc = fs_entry_garbage_collect_kickoff( core, fs_path, &fent_gc_snapshot, &garbage, true );
   
   fs_entry_free_modification_map( &garbage );
   
   if( rc != 0 ) {
      errorf("fs_entry_garbage_collect_kickoff( %" PRIX64 ".%" PRId64 " ) rc = %d\n", fent_gc_snapshot.file_id, fent_gc_snapshot.file_version, rc );
      
      return rc;
   }
   
   // enqueued in the garbage collector
   return 0;
}


// get the next write log entry to vacuum
static int fs_entry_vacuumer_get_next_write( struct fs_core* core, uint64_t volume_id, uint64_t file_id, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, struct ms_vacuum_entry* ve ) {
   
   memset( ve, 0, sizeof(struct ms_vacuum_entry) );
   
   // get the head of the vacuum log 
   int rc = ms_client_peek_vacuum_log( core->ms, volume_id, file_id, ve );
   if( rc != 0 ) {
      if( rc == -ENOENT ) {
         dbprintf("Nothing to vacuum for %" PRIX64 "\n", file_id );
         return VACUUM_DONE;
      }
      else {
         errorf("ms_client_peek_vacuum_log(%" PRIX64 ") rc = %d\n", file_id, rc );
         return rc;
      }
   }
   
   // if this refers to the current data, then don't vacuum.  Just delete this log entry
   if( ve->manifest_mtime_sec == manifest_mtime_sec && ve->manifest_mtime_nsec == manifest_mtime_nsec ) {
      dbprintf("Nothing left to vacuum for %" PRIX64 "\n", file_id );
      
      return VACUUM_HEAD;
   }
   
   return VACUUM_AGAIN;
}
   

// vacuum a write, synchronously
// return VACUUM_AGAIN on success
// return negative on error 
// fent can be NULL; it will be resolved by fs_path read-locked to get the manifest if so
static int fs_entry_vacuumer_vacuum_write( struct fs_core* core, char const* fs_path, struct fs_entry* fent, struct replica_snapshot* fent_snapshot, struct ms_vacuum_entry* ve ) {
   
   int rc = 0;
   Serialization::ManifestMsg manifest_msg;
   
   // get the manifest 
   rc = fs_entry_vacuumer_get_manifest( core, fs_path, fent, ve->manifest_mtime_sec, ve->manifest_mtime_nsec, &manifest_msg );
   if( rc != 0 ) {
      
      if( rc == -ENOENT ) {
         // no manifest to be had
         dbprintf("WARN: manifest %" PRIX64 "/manifest.%" PRId64 ".%d not found\n", fent_snapshot->file_id, ve->manifest_mtime_sec, ve->manifest_mtime_nsec );
         rc = VACUUM_AGAIN;
      }
      else {
         errorf("fs_entry_vacuumer_get_manifest(%s %" PRIX64 ") rc = %d\n", fs_path, fent_snapshot->file_id, rc );
      }
      return rc;
   }
   
   // vacuum the data 
   rc = fs_entry_vacuumer_vacuum_data_bg( core, fs_path, fent_snapshot, &manifest_msg, ve->affected_blocks, ve->num_affected_blocks );
   if( rc != 0 ) {
      errorf("fs_entry_vacuumer_vacuum_data(%s %" PRIX64 ") rc = %d\n", fs_path, fent_snapshot->file_id, rc );
      
      return rc;  
   }
   
   // vacuumed!  Get the next one
   return VACUUM_AGAIN;
}


// vacuum a specific entry of the write log, synchronously 
static int fs_entry_vacuumer_vacuum_write_log( struct ms_client* ms, struct ms_vacuum_entry* ve ) {
   
   int rc = 0;
   
   // vacuum this log entry
   rc = ms_client_remove_vacuum_log_entry( ms, ve->volume_id, ve->file_id, ve->file_version, ve->manifest_mtime_sec, ve->manifest_mtime_nsec );
   if( rc != 0 ) {
      if( rc == -ENOENT ) {
         return VACUUM_DONE;
      }
      else {
         errorf("ms_client_remove_vacuum_log_entry(%" PRIX64 ".%" PRId64 ") rc = %d\n", ve->file_id, ve->file_version, rc );
         return rc;
      }
   }
   else {
      return VACUUM_AGAIN;
   }
}


// vacuum all writes for a file, synchronously.
// fent must be read-locked
// return 0 on success; negative on error 
int fs_entry_vacuumer_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent ) {
   
   int rc = 0;
   
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );
   
   dbprintf("Vacuuming %s %" PRIX64 "\n", fs_path, fent->file_id);
   
   while( true ) {
      
      // peek the log 
      struct ms_vacuum_entry ve;
      bool delete_data = true;
      
      rc = fs_entry_vacuumer_get_next_write( core, fent_snapshot.volume_id, fent_snapshot.file_id, fent_snapshot.manifest_mtime_sec, fent_snapshot.manifest_mtime_nsec, &ve );
      
      if( rc < 0 ) {
         errorf("fs_entry_vacuumer_get_next_write( %s %" PRIX64 " ) rc = %d\n", fs_path, fent_snapshot.file_id, rc );
         return rc;
      }
      else if( rc == VACUUM_HEAD ) {
         // at the head--just delete the log entry 
         delete_data = false;
      }
      else if( rc == VACUUM_DONE ) {
         // nothing left to do
         break;
      }
         
      // collect the data, if we have to 
      if( delete_data ) { 
         
         rc = fs_entry_vacuumer_vacuum_write( core, fs_path, fent, &fent_snapshot, &ve );
         
         if( rc < 0 ) {
            errorf("fs_entry_vacuumer_vacuum_write(%s %" PRIX64 ") rc = %d\n", fs_path, fent->file_id, rc );
            return rc;
         }
      }
      
      // collect the log entry 
      rc = fs_entry_vacuumer_vacuum_write_log( core->ms, &ve );
      
      if( rc < 0 ) {
         errorf("fs_entry_vacuumer_vacuum_write_log(%s %" PRIX64 ") rc = %d\n", fs_path, fent->file_id, rc );
         return rc;
      }
      else if( rc == VACUUM_DONE ) {
         // done!
         break;
      }
   }
   
   // garbage-collect current file state 
   fs_entry_garbage_collect_file( core, fs_path, fent );
   
   dbprintf("Vacuumed %s %" PRIX64 " successfully\n", fs_path, fent->file_id);
   
   return 0;
}


// mark a file as being vacuumed or not, if we're still the coordinatr
static int fs_entry_vacuumer_set_vacuum_status( struct fs_core* core, char const* fs_path, bool set_vacuuming, bool vacuuming, bool set_vacuumed, bool vacuumed ) {
   
   struct fs_entry* fent = NULL;
   int err = 0;
   
   // resolve 
   fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, true, &err );
   if( fent == NULL || err != 0 ) {
      if( err == 0 )
         err = -ENOMEM;
      
      return err;
   }
   
   // verify that we're still the coordinator
   if( !FS_ENTRY_LOCAL( core, fent ) ) {
      // nothing to do 
      fs_entry_unlock( fent );
      return 0;
   }
   
   if( set_vacuuming )
      fent->vacuuming = vacuuming;
   
   if( set_vacuumed )
      fent->vacuumed = vacuumed;
   
   fs_entry_unlock( fent );
   return 0;
}


// is a file being vacuumed?
bool fs_entry_vacuumer_is_vacuuming( struct fs_entry* fent ) {
   return fent->vacuuming;
}


// is a file vacuumed?
bool fs_entry_vacuumer_is_vacuumed( struct fs_entry* fent ) {
   return fent->vacuumed;
}


// main vacuum method 
static void* vacuumer_main( void* arg ) {
   
   struct fs_vacuumer* vac = (struct fs_vacuumer*)arg;
   
   dbprintf("%s", "Started vacuumer thread\n");
   
   while( vac->running ) {
      
      fs_entry_vacuumer_wlock( vac );
      
      // splice in the pending vacuum requests 
      fs_entry_vacuumer_add_pending( vac );
      
      if( vac->vacuum_set->size() > 0 ) {
         
         // process pending requests 
         for( vacuum_set_t::iterator itr = vac->vacuum_set->begin(); itr != vac->vacuum_set->end(); itr++ ) {
            
            struct fs_vacuumer_request vreq = *itr;
            int rc = 0;
            char const* method = NULL;
            
            // peek the log 
            struct ms_vacuum_entry ve;
            memset( &ve, 0, sizeof(struct ms_vacuum_entry) );
            
            rc = fs_entry_vacuumer_get_next_write( vac->core, vreq.fent_snapshot.volume_id, vreq.fent_snapshot.file_id, vreq.fent_snapshot.manifest_mtime_sec, vreq.fent_snapshot.manifest_mtime_nsec, &ve );
            
            if( rc < 0 ) {
               errorf("fs_entry_vacuumer_get_next_write( %s %" PRIX64 " ) rc = %d\n", vreq.fs_path, vreq.fent_snapshot.file_id, rc );
            }
            else if( rc == VACUUM_HEAD ) {
               
               // just vacuum the log head 
               method = "fs_entry_vacuumer_vacuum_write_log (HEAD)";
               rc = fs_entry_vacuumer_vacuum_write_log( vac->core->ms, &ve );
            }
            else if( rc != VACUUM_DONE ) {
               
               // proceed with the request to vacuum data
               switch( vreq.type ) {
                  case VACUUM_TYPE_WRITE: {
                     
                     method = "fs_entry_vacuumer_vacuum_write";
                     rc = fs_entry_vacuumer_vacuum_write( vac->core, vreq.fs_path, NULL, &vreq.fent_snapshot, &ve );
                     
                     if( rc >= 0 ) {
                        // do the log entry as well 
                        method = "fs_entry_vacuumer_vacuum_write; fs_entry_vacuumer_write_log";
                        rc = fs_entry_vacuumer_vacuum_write_log( vac->core->ms, &ve );
                     }
                     
                     break;
                  }
                  
                  case VACUUM_TYPE_LOG: {
                     
                     method = "fs_entry_vacuumer_vacuum_write_log";
                     rc = fs_entry_vacuumer_vacuum_write_log( vac->core->ms, &ve );
                     break;
                  }
                  
                  default: {
                     
                     errorf("unrecognized request type %d\n", vreq.type );
                     rc = -EINVAL;
                     break;
                  }
               }
            }
            
            // result?
            if( rc == VACUUM_AGAIN ) {
               // re-enqueue 
               dbprintf("Re-enqueue result of %s( %" PRIX64 " type %d )\n", method, vreq.fent_snapshot.file_id, vreq.type );
               
               fs_entry_vacuumer_pending_wlock( vac );
               vac->vacuum_pending->insert( vreq );
               fs_entry_vacuumer_pending_unlock( vac );
            }
            else if( rc == VACUUM_DONE ) {
               // done!
               dbprintf("Finished request type %d on %" PRIX64 "\n", vreq.type, vreq.fent_snapshot.file_id );
               
               fs_entry_vacuumer_set_vacuum_status( vac->core, vreq.fs_path, true, false, true, true );
               
               fs_entry_vacuumer_request_free( &vreq );
            }
            else {
               // error 
               errorf("%s( %" PRIX64 " type %d ) rc = %d\n", method, vreq.fent_snapshot.file_id, vreq.type, rc );
               
               fs_entry_vacuumer_set_vacuum_status( vac->core, vreq.fs_path, true, false, true, false );
               
               fs_entry_vacuumer_request_free( &vreq );
            }
            
            ms_client_vacuum_entry_free( &ve );
         }
         
         vac->vacuum_set->clear();
         
         fs_entry_vacuumer_unlock( vac );
      }
      else {
         
         fs_entry_vacuumer_unlock( vac );
         
         // do nothing--wait for requests to accumulate
         sleep(1);
      }
   }
   
   dbprintf("%s", "Vacuumer thread exit\n");
   return NULL;
}
