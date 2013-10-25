/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "write.h"
#include "replication.h"
#include "read.h"

// expand a file (e.g. if we write to it beyond the end of the file).
// record the blocks added.
// fent must be write-locked
int fs_entry_expand_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t new_size, modification_map* modified_blocks ) {
   off_t old_size = fent->size;
   int rc = 0;
   fent->size = new_size;
   
   if( new_size <= old_size ) {
      return 0;
   }

   int err = 0;
   uint64_t start_id = fs_entry_block_id( core, old_size );
   uint64_t end_id = fs_entry_block_id( core, new_size );
   
   if( start_id == end_id ) {
      // nothing to do here
      return 0;
   }
   
   bool local = FS_ENTRY_LOCAL( core, fent );
   char* block = CALLOC_LIST( char, core->blocking_factor );

   // preserve the last block, if there is one
   if( old_size > 0 && (old_size % core->blocking_factor) > 0 ) {
      
      uint64_t block_version = fent->manifest->get_block_version( start_id );
      
      int block_fd = fs_entry_open_block( core, fent, start_id, block_version, !local, false );
      if( block_fd < 0 ) {
         errorf("fs_entry_open_block( /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRId64 ".%" PRIu64 ") rc = %d\n", core->volume, core->gateway, fent->file_id, fent->version, start_id, block_version, block_fd );
         free( block );
         return block_fd;
      }
      
      // read the tail of this block in
      ssize_t nr = fs_entry_fill_block( core, fent, block, NULL, block_fd, old_size - start_id * core->blocking_factor );
      
      close( block_fd );
      
      if( nr < 0 ) {
         errorf("fs_entry_fill_block( /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRId64 ".%" PRIu64 ") rc = %zd\n", core->volume, core->gateway, fent->file_id, fent->version, start_id, block_version, nr );
         free( block );
         return (int)nr;
      }
   }

   bool cleared = false;
   
   for( uint64_t id = start_id; id <= end_id; id++ ) {

      rc = fs_entry_put_block_data( core, fent, id, block, 0, core->blocking_factor, !local );
      if( rc < 0 ) {
         errorf("fs_entry_put_block(/%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "[%" PRIu64 "]) rc = %d\n", core->volume, core->gateway, fent->file_id, fent->version, id, rc );
         err = rc;
         break;
      }

      // record that we have written this
      struct fs_entry_block_info binfo;
      memset( &binfo, 0, sizeof(binfo) );

      binfo.version = fent->manifest->get_block_version( id );
      binfo.hash = sha256_hash_data( block, core->blocking_factor );
      binfo.hash_len = sha256_len();
      
      (*modified_blocks)[ id ] = binfo;

      if( !cleared )
         memset( block, 0, core->blocking_factor );
      
      cleared = true;
   }

   return err;
}


// fill in a block of data (used by fs_entry_write_real)
ssize_t fs_entry_fill_block( struct fs_core* core, struct fs_entry* fent, char* block, char const* buf, int source_fd, size_t count ) {
   ssize_t rc = count;
   
   if( buf != NULL ) {
      // source is the buffer
      memcpy( block, buf, count );
      return rc;
   }
   
   else if( source_fd > 0 ) {
      // source is the fd.  Read the block
      ssize_t fd_read = 0;
      while( (unsigned)fd_read < count ) {
         ssize_t fd_nr = read( source_fd, block + fd_read, count - fd_read);
         if( fd_nr < 0 ) {
            fd_nr = -errno;
            errorf( "read(/%" PRIu64 "/%" PRIu64 "/%" PRIX64 ") errno = %zd\n", core->volume, core->gateway, fent->file_id, fd_nr );
            rc = -errno;
            break;
         }

         if( fd_nr == 0 )
            break;

         fd_read += fd_nr;
      }
   }

   return rc;
}


static bool fs_entry_is_write_unaligned( off_t byte_offset, uint64_t block_size ) {
   return (byte_offset % block_size) != 0;
}


static int fs_entry_hash_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len, struct fs_entry_block_info* binfo ) {
   ssize_t sz = fs_entry_read_local_block( core, fent, block_id, block_bits, block_len );
   if( sz >= 0 ) {
      binfo->hash = sha256_hash_data( block_bits, block_len );
      binfo->hash_len = sha256_len();
      sz = 0;
   }
   else {
      errorf("fs_entry_read_local_block(%" PRIX64 ") rc = %d\n", block_id, (int)sz );
   }
   
   return (int)sz;
}


// can a block be garbage-collected?
static bool fs_entry_is_garbage_collectable_block( struct fs_core* core, struct replica_snapshot* snapshot_fent, off_t fent_old_size, uint64_t block_id, modification_map* no_garbage_collect ) {
   
   // don't collect this one 
   if( no_garbage_collect->find( block_id ) != no_garbage_collect->end() )
      return false;
   
   // no blocks exist, so this is guaranteed new
   if( fent_old_size == 0 )
      return false;
   
   // block is beyond the last block in the file, so guaranteed new
   if( block_id > ((uint64_t)fent_old_size / core->blocking_factor) && fent_old_size > 0 )
      return false;
   
   // has an older copy to be removed
   return true;
}


// write data to a file, either from a buffer or a file descriptor
ssize_t fs_entry_write_real( struct fs_core* core, struct fs_file_handle* fh, char const* buf, int source_fd, size_t count, off_t offset ) {
   fs_file_handle_rlock( fh );
   if( fh->fent == NULL || fh->open_count <= 0 ) {
      // invalid
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   struct timespec ts, ts2;
   struct timespec latency_ts, write_ts, replicate_ts, replicate_ts_total, garbage_collect_ts, remote_write_ts, update_ts;

   BEGIN_TIMING_DATA( ts );
   
   int rc = fs_entry_revalidate_path( core, core->volume, fh->path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate(%s) rc = %d\n", fh->path, rc );
      fs_file_handle_unlock( fh );
      return -EREMOTEIO;
   }
   
   fs_entry_wlock( fh->fent );
   
   bool local = FS_ENTRY_LOCAL( core, fh->fent );
   off_t old_size = fh->fent->size;

   rc = fs_entry_revalidate_manifest( core, fh->path, fh->fent );

   if( rc != 0 ) {
      errorf("fs_entry_revalidate_manifest(%s) rc = %d\n", fh->path, rc );
      fs_entry_unlock( fh->fent );
      fs_file_handle_unlock( fh );
      return -EREMOTEIO;
   }

   END_TIMING_DATA( ts, latency_ts, "metadata latency" );

   BEGIN_TIMING_DATA( write_ts );

   ssize_t ret = 0;
   ssize_t num_written = 0;

   // record of blocks NOT to garbage-collect
   modification_map no_garbage_collect;
   
   // record which blocks we've modified
   modification_map modified_blocks;
   
   // record of which blocks we've overwritten and can safely garbage-collect
   modification_map overwritten_blocks;
   
   // snapshot fent before we do anything
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fh->fent, 0, 0, &fent_snapshot );
   
   // do we first need to expand this file?
   if( offset > old_size ) {
      rc = fs_entry_expand_file( core, fh->path, fh->fent, offset, &modified_blocks );
      if( rc != 0 ) {
         // can't proceed
         errorf( "fs_entry_expand_file(%s) to size %" PRId64 " rc = %d\n", fh->path, offset, rc );
         fs_entry_unlock( fh->fent );
   
         fs_file_handle_unlock( fh );


         if( modified_blocks.size() > 0 ) {
            // free memory
            for( modification_map::iterator itr = modified_blocks.begin(); itr != modified_blocks.end(); itr++ ) {
               free( itr->second.hash );
               itr->second.hash = NULL;
            }
         }

         return rc;
      }
      
      // dont garbage-collect these---remember their versions
      for( modification_map::iterator itr = modified_blocks.begin(); itr != modified_blocks.end(); itr++ ) {
         struct fs_entry_block_info new_binfo;
         memset( &new_binfo, 0, sizeof(new_binfo) );
         
         new_binfo.version = itr->second.version;
         
         no_garbage_collect[ itr->first ] = new_binfo;
      }
   }

   fs_entry_unlock( fh->fent );
   
   char* block = CALLOC_LIST( char, core->blocking_factor );
   
   uint64_t start_id = fs_entry_block_id( core, offset );
   uint64_t end_id = fs_entry_block_id( core, offset + count );
   
   // do we need to get the hashes of the last blocks from the entire block?
   bool need_end_id_hash = (start_id != end_id && fs_entry_is_write_unaligned( offset + count, core->blocking_factor ));
   
   while( (size_t)num_written < count ) {
      // which block are we about to write?
      uint64_t block_id = fs_entry_block_id( core, offset + num_written );
      
      // what is the write offset into the block?
      off_t block_write_offset = (offset + num_written) % core->blocking_factor;
      off_t block_put_offset = block_write_offset;
      off_t block_fill_offset = 0;
      
      // how much data are we going to write into this block?
      size_t block_write_len = MIN( core->blocking_factor - block_write_offset, count - num_written );
      size_t block_put_len = block_write_len;
      
      if( block_write_offset != 0 ) {
         // need to fill this block with the contents of the current block first, since we're writing unaligned
         
         ssize_t read_rc = fs_entry_do_read_block( core, fh->path, fh->fent, block_id, block, core->blocking_factor );
         if( read_rc < 0 ) {
            errorf("fs_entry_do_read_block( %s ) rc = %d\n", fh->path, (int)read_rc );
            rc = (int)read_rc;
            break;
         }
         
         // fill the rest of the block at the unaligned offset
         block_fill_offset = block_write_offset;
         
         // write aligned--put the whole block
         block_put_offset = 0;
         block_put_len = core->blocking_factor;
      }
      
      // get the data...
      ssize_t read_len = fs_entry_fill_block( core, fh->fent, block + block_fill_offset, buf + num_written, source_fd, block_write_len );
      if( (unsigned)read_len != block_write_len ) {
         errorf("fs_entry_fill_block(%s/%" PRId64 ", offset=%" PRId64 ", len=%" PRId64 ") rc = %zd\n", fh->path, block_id, block_write_offset, block_write_len, read_len );
         rc = read_len;
         break;
      }
      
      fs_entry_wlock( fh->fent );
      
      uint64_t old_version = fh->fent->manifest->get_block_version( block_id );
      
      // write the data...
      ssize_t write_size = fs_entry_put_block_data( core, fh->fent, block_id, block, block_put_offset, block_put_len, !local );
      
      if( (unsigned)write_size != block_put_len ) {
         errorf("fs_entry_put_block_data(%s/%" PRId64 ", offset=%" PRId64 ", len=%" PRId64 ") rc = %zd\n", fh->path, block_id, block_put_offset, block_put_len, write_size );
         rc = write_size;
         fs_entry_unlock( fh->fent );
         break;
      }
      
      uint64_t new_version = fh->fent->manifest->get_block_version( block_id );
      
      fs_entry_unlock( fh->fent );
      
      // is this a block to garbage collect?
      if( fs_entry_is_garbage_collectable_block( core, &fent_snapshot, old_size, block_id, &no_garbage_collect ) ) {
         
         // mark the old version of the block that we've overwritten to be garbage-collected
         struct fs_entry_block_info binfo_overwritten;
         memset( &binfo_overwritten, 0, sizeof(binfo_overwritten) );

         binfo_overwritten.version = old_version;
         
         overwritten_blocks[ block_id ] = binfo_overwritten;
      }
      
      // record that we've written this block
      struct fs_entry_block_info binfo;
      memset( &binfo, 0, sizeof(binfo) );
      
      binfo.version = new_version;
      
      // only store the hash if it covers the whole block
      if( (block_id == end_id && !need_end_id_hash) || (block_id != start_id && block_id != end_id) ) {
         binfo.hash = sha256_hash_data( block, core->blocking_factor );
         binfo.hash_len = sha256_len();
      }
      
      modified_blocks[ block_id ] = binfo;

      num_written += block_put_len;     // not write_size, since physically we may have written a whole block, while logically we've written less
      
      memset( block, 0, core->blocking_factor );
   }
   
   int hash_rc = 0;
   
   // hash the last block?
   if( rc == 0 && hash_rc == 0 && need_end_id_hash ) {
      memset( block, 0, core->blocking_factor );
      struct fs_entry_block_info* binfo = &modified_blocks[ end_id ];
      
      if( binfo->hash != NULL ) {
         free( binfo->hash);
      }
      
      hash_rc = fs_entry_hash_block( core, fh->fent, end_id, block, core->blocking_factor, binfo );
   }
   
   free( block );

   if( hash_rc != 0 )
      ret = hash_rc;
   
   else if( rc != 0 )
      ret = rc;
   
   else
      ret = count;

   END_TIMING_DATA( write_ts, ts2, "write data" );

   fs_entry_wlock( fh->fent );

   BEGIN_TIMING_DATA( replicate_ts_total );
   
   // if we wrote data, replicate the manifest and blocks.
   if( ret > 0 && modified_blocks.size() > 0 ) {
      
      if( local ) {
         BEGIN_TIMING_DATA( replicate_ts );
   
         // replicate the new manifest
         int rc = fs_entry_replicate_manifest( core, fh->fent, false, fh );
         if( rc != 0 ) {
            errorf("fs_entry_replicate_manifest(%s) rc = %d\n", fh->path, rc );
            ret = -EIO;
         }
         
         
         END_TIMING_DATA( replicate_ts, ts2, "replicate manifest" );
      }

      if( ret >= 0 ) {
         // replicate written blocks
         BEGIN_TIMING_DATA( replicate_ts );
         
         uint64_t start_id = modified_blocks.begin()->first;
         uint64_t end_id = modified_blocks.rbegin()->first + 1;      // exclusive

         int rc = fs_entry_replicate_blocks( core, fh->fent, &modified_blocks, false, fh );
         if( rc != 0 ) {
            errorf("fs_entry_replicate_write(%s[%" PRId64 "-%" PRId64 "]) rc = %d\n", fh->path, start_id, end_id, rc );
            ret = -EIO;
         }
         
         END_TIMING_DATA( replicate_ts, ts2, "replicate block data" );
      }
      
      if( fh->flags & O_SYNC ) {
         // wait for all replicas to finish
         fs_entry_replicate_wait( fh );
      }
   }

   END_TIMING_DATA( replicate_ts_total, ts2, "replicate data" );
   
   BEGIN_TIMING_DATA( garbage_collect_ts );
   
   // if we modified data, then garbage-collect old data
   if( ret > 0 && overwritten_blocks.size() > 0 ) {
      
      int rc = 0;
      
      if( local ) {
         // garbage collect the old manifest
         
         rc = fs_entry_garbage_collect_manifest( core, &fent_snapshot );
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fh->path, rc );
            rc = -EIO;
         }
      }
      
      if( ret >= 0 ) {
         // garbage-collect written blocks
         
         rc = fs_entry_garbage_collect_blocks( core, &fent_snapshot, &overwritten_blocks );
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_blocks(%s) rc = %d\n", fh->path, rc );
            rc = -EIO;
         }
      }
   }

   END_TIMING_DATA( garbage_collect_ts, ts2, "garbage collect data" );
   
   if( ret > 0 && count > 0 ) {

      // SUCCESS!

      uint64_t start_id = modified_blocks.begin()->first;
      uint64_t end_id = modified_blocks.rbegin()->first + 1;      // exclusive

      // NOTE: size may have changed due to expansion, but it shouldn't affect this computation
      fh->fent->size = MAX( fh->fent->size, (unsigned)(offset + count) );
      
      if( !local ) {
         BEGIN_TIMING_DATA( remote_write_ts );
      
         // tell the remote owner about our write
         Serialization::WriteMsg *write_msg = new Serialization::WriteMsg();

         // send a prepare message
         int64_t* versions = fh->fent->manifest->get_block_versions( start_id, end_id );
         fs_entry_prepare_write_message( write_msg, core, fh->path, fh->fent, start_id, end_id, versions );
         free( versions );

         Serialization::WriteMsg *write_ack = new Serialization::WriteMsg();
         int rc = fs_entry_post_write( write_ack, core, fh->fent->coordinator, write_msg );

         // process the write message--hopefully it's a PROMISE
         if( rc != 0 ) {
            // failed to post
            errorf( "fs_entry_post_write(%s) rc = %d\n", fh->path, rc );
            ret = -EIO;
         }
         else if( write_ack->type() != Serialization::WriteMsg::PROMISE ) {
            if( write_ack->type() == Serialization::WriteMsg::ERROR ) {
               if( write_ack->errorcode() == -EINVAL ) {
                  // file version mismatch--the file got reversioned while we were writing (e.g. due to a truncate)
                  // the write is said to have happened before the truncate in this case, so clear it
                  dbprintf("file version mismatch; can't write to old version of %s\n", fh->path );

                  fs_entry_mark_read_stale( fh->fent );
               }
               else {
                  errorf( "remote write error = %d (%s)\n", write_ack->errorcode(), write_ack->errortxt().c_str() );
                  ret = -abs( write_ack->errorcode() );
               }
            }
            else {
               errorf( "remote write invalid message %d\n", write_ack->type() );
               ret = -EIO;
            }
         }

         delete write_ack;
         delete write_msg;
         
         
         END_TIMING_DATA( remote_write_ts, ts2, "send remote write" );
      }
   }
   
   if( ret > 0 ) {
      // update mtime
      struct timespec new_mtime;
      clock_gettime( CLOCK_REALTIME, &new_mtime );
      
      fh->fent->mtime_sec = new_mtime.tv_sec;
      fh->fent->mtime_nsec = new_mtime.tv_nsec;
   }

   if( ret > 0 && local ) {

      BEGIN_TIMING_DATA( update_ts );
      
      // synchronize the new modifications with the MS
      struct md_entry ent;
      fs_entry_to_md_entry( core, &ent, fh->fent, fh->parent_id, fh->parent_name );

      int up_rc = 0;
      char const* errstr = NULL;
      
      if( fh->fent->max_write_freshness > 0 && !(fh->flags & O_SYNC) ) {
         up_rc = ms_client_queue_update( core->ms, &ent, currentTimeMillis() + fh->fent->max_write_freshness, 0 );
         errstr = "ms_client_queue_update";
      }
      else {
         up_rc = ms_client_update( core->ms, &ent );
         errstr = "ms_client_update";
      }
      
      md_entry_free( &ent );

      if( up_rc != 0 ) {
         errorf("%s(%s) rc = %d\n", errstr, fh->path, rc );
         ret = -EREMOTEIO;
      }

      END_TIMING_DATA( update_ts, ts2, "MS update" );
   }
   
   fs_entry_unlock( fh->fent );
   fs_file_handle_unlock( fh );

   if( modified_blocks.size() > 0 ) {
      // free memory
      for( modification_map::iterator itr = modified_blocks.begin(); itr != modified_blocks.end(); itr++ ) {
         free( itr->second.hash );
      }
   }

   END_TIMING_DATA( ts, ts2, "write" );

   return ret;
}


ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, char const* buf, size_t count, off_t offset ) {
   return fs_entry_write_real( core, fh, buf, -1, count, offset );
}

ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, int source_fd, size_t count, off_t offset ) {
   return fs_entry_write_real( core, fh, NULL, source_fd, count, offset );
}


// Handle a remote write.  Update the affected blocks in the manifest, and republish.
int fs_entry_remote_write( struct fs_core* core, char const* fs_path, uint64_t file_id, uint64_t coordinator_id, Serialization::WriteMsg* write_msg ) {
   int err = 0;
   uint64_t parent_id = 0;
   char* parent_name = NULL;
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, fs_path, write_msg->user_id(), write_msg->volume_id(), true, &err, &parent_id, &parent_name );
   if( err != 0 || fent == NULL ) {
      return err;
   }
   
   // validate
   if( fent->file_id != file_id ) {
      errorf("Remote write to file %s ID %" PRIX64 ", expected %" PRIX64 "\n", fs_path, file_id, fent->file_id );
      fs_entry_unlock( fent );
      return -ESTALE;
   }
   
   if( fent->coordinator != coordinator_id ) {
      errorf("Remote write to file %s coordinator %" PRIu64 ", expected %" PRIu64 "\n", fs_path, coordinator_id, fent->coordinator );
      fs_entry_unlock( fent );
      return -ESTALE;
   }
   
   // snapshot the fent so we can garbage-collect the manifest 
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );
   
   uint64_t gateway_id = write_msg->gateway_id();

   struct timespec ts, ts2, replicate_ts, garbage_collect_ts, update_ts;
   struct timespec mts;

   BEGIN_TIMING_DATA( ts );

   modification_map old_block_info;
   
   // update the blocks
   for( unsigned int i = 0; i < write_msg->blocks().end_id() - write_msg->blocks().start_id(); i++ ) {
      uint64_t block_id = i + write_msg->blocks().start_id();
      int64_t new_version = write_msg->blocks().version(i);

      // back up old version and gateway, in case we have to restore it
      int64_t old_version = fent->manifest->get_block_version( block_id );
      uint64_t old_gateway_id = fent->manifest->get_block_gateway( block_id );
      bool old_staging_status = fent->manifest->is_block_staging( block_id );
      
      struct fs_entry_block_info binfo;
      memset( &binfo, 0, sizeof(struct fs_entry_block_info) );
      
      binfo.version = old_version;
      binfo.gateway_id = old_gateway_id;
      binfo.staging = old_staging_status;
      
      old_block_info[ block_id ] = binfo;
      
      // put the new version into the manifest
      fs_entry_manifest_put_block( core, gateway_id, fent, block_id, new_version, false );
   }
   
   off_t old_size = fent->size;
   fent->size = write_msg->metadata().size();

   clock_gettime( CLOCK_REALTIME, &mts );

   fent->mtime_sec = mts.tv_sec;
   fent->mtime_nsec = mts.tv_nsec;
   
      
   // replicate the manifest, synchronously
   BEGIN_TIMING_DATA( replicate_ts );

   err = fs_entry_replicate_manifest( core, fent, true, NULL );
   if( err != 0 ) {
      errorf("fs_entry_replicate_manifest(%s) rc = %d\n", fs_path, err );
      err = -EIO;
   }
   
   END_TIMING_DATA( replicate_ts, ts2, "replicate manifest" );
   
   if( err == 0 ) {
   
      BEGIN_TIMING_DATA( update_ts );
      
      // replicated!
      // propagate the update to the MS
      struct md_entry data;
      fs_entry_to_md_entry( core, &data, fent, parent_id, parent_name );
      
      uint64_t max_write_freshness = fent->max_write_freshness;
      fs_entry_unlock( fent );
      
      // NOTE: this will send the update immediately if max_write_freshness == 0
      err = ms_client_queue_update( core->ms, &data, currentTimeMillis() + max_write_freshness, 0 );
      if( err != 0 ) {
         errorf("%ms_client_queue_update(%s) rc = %d\n", fs_path, err );
         err = -EREMOTEIO;
      }
      
      md_entry_free( &data );      
      
      END_TIMING_DATA( update_ts, ts2, "MS update" );
   }

   free( parent_name );
   
   // garbage-collect the old manifest
   if( err == 0 ) {
      
      BEGIN_TIMING_DATA( garbage_collect_ts );
      
      int rc = fs_entry_garbage_collect_manifest( core, &fent_snapshot );
      if( rc != 0 ) {
         errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fs_path, rc );
         rc = -EIO;
      }
      
      END_TIMING_DATA( garbage_collect_ts, ts2, "garbage collect manifest" );
   }
   
   else {
      errorf("roll back manifest of %s\n", fs_path );
      
      // had an error along the way.  Restore the old fs_entry's manifest
      uint64_t proposed_size = fent->size;
      
      fent->size = old_size;
      fent->mtime_sec = fent_snapshot.mtime_sec;
      fent->mtime_nsec = fent_snapshot.mtime_nsec;
      
      uint64_t old_end_block = old_size / core->blocking_factor;
      uint64_t proposed_end_block = proposed_size / core->blocking_factor;
      
      if( old_end_block < proposed_end_block ) {
         // truncate the manifest back to its original size
         fent->manifest->truncate( old_end_block );
      }
      
      // restore gateway ownership and versions
      for( modification_map::iterator itr = old_block_info.begin(); itr != old_block_info.end(); itr++ ) {
         uint64_t block_id = itr->first;
         
         // skip blocks written beyond the end of the original manifest
         if( block_id > old_end_block )
            continue;
         
         struct fs_entry_block_info* old_binfo = &itr->second;
         
         fs_entry_manifest_put_block( core, old_binfo->gateway_id, fent, block_id, old_binfo->version, old_binfo->staging );
      }
      
      fs_entry_unlock( fent );
   }
   
   END_TIMING_DATA( ts, ts2, "write, remote" );
   return err;
}
