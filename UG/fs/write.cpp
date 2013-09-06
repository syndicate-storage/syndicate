/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "write.h"
#include "replication.h"

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
   
   char* block = CALLOC_LIST( char, core->blocking_factor );

   // preserve the last block, if there is one
   if( fent->size > 0 ) {
      rc = fs_entry_prepare_write_block( core, fs_path, fent, block, 0, fent->size, 0 );
      if( rc != 0 ) {
         errorf("fs_entry_prepare_write_block(/%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "[%" PRId64 "]) rc = %d\n", core->volume, core->gateway, fent->file_id, fent->version, fs_entry_block_id( core, fent->size ), rc );
         return rc;
      }
   }

   bool cleared = false;
   bool local = FS_ENTRY_LOCAL( core, fent );
   
   for( uint64_t id = start_id; id <= end_id; id++ ) {

      rc = fs_entry_put_block( core, fent, id, block, !local );
      if( rc != 0 ) {
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


// read a block in preparation for overwriting part of it.
// Only reads the block if there exists the possibility that some of its data will need to be preserved
// fent must be write-locked
int fs_entry_prepare_write_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, char* block, size_t count, off_t offset, ssize_t num_written ) {
   int rc = 0;
   uint64_t block_id = fs_entry_block_id( core, offset + num_written );
   
   ssize_t block_write_offset = (offset + num_written) % core->blocking_factor;
   
   if( fent->size > 0 && (block_write_offset != 0 || (offset + num_written < fent->size && count - (size_t)num_written < core->blocking_factor ))) {

      // get the block data, since we'll need to preserve part of it
      // NOTE: use the offset at the block boundary; otherwise we can get EOF
      ssize_t blk_size = fs_entry_do_read_block( core, fs_path, fent, block_id, block, core->blocking_factor );

      if( blk_size < 0 ) {
         errorf( "fs_entry_read_block(/%" PRIu64 "/%" PRIu64 "/%" PRIX64 "[%" PRId64 "]) rc = %zd\n", core->volume, core->gateway, fent->file_id, block_id, blk_size );
         rc = -EIO;
      }
      else {
         dbprintf("read %zd bytes, block_write_offset = %zd\n", blk_size, block_write_offset);
      }
   }

   return rc;
}


// fill in a block of data (used by fs_entry_write_real)
ssize_t fs_entry_fill_block( struct fs_core* core, struct fs_entry* fent, char* block, char const* buf, int source_fd, size_t count, off_t offset, ssize_t num_written ) {
   ssize_t block_write_offset = (offset + num_written) % core->blocking_factor;
   
   // fill in the block
   ssize_t write_size = MIN( (uint64_t)(count - num_written), core->blocking_factor - block_write_offset );

   if( buf != NULL ) {
      // source is the buffer
      memcpy( block + block_write_offset, buf + num_written, write_size );
   }
   else if( source_fd > 0 ) {
      // source is the fd.  Read the block
      char* fd_buf = CALLOC_LIST( char, core->blocking_factor );

      memset( fd_buf, 0, core->blocking_factor );

      ssize_t fd_read = 0;
      while( (unsigned)fd_read < core->blocking_factor - block_write_offset ) {
         ssize_t fd_nr = read( source_fd, fd_buf + fd_read, core->blocking_factor - block_write_offset - fd_read );
         if( fd_nr < 0 ) {
            fd_nr = -errno;
            errorf( "read(/%" PRIu64 "/%" PRIu64 "/%" PRIX64 ") errno = %zd\n", core->volume, core->gateway, fent->file_id, fd_nr );
            write_size = -errno;
            break;
         }

         if( fd_nr == 0 )
            break;

         fd_read += fd_nr;
      }

      memcpy( block + block_write_offset, fd_buf, write_size );
      free( fd_buf );
   }

   return write_size;
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
   struct timespec latency_ts, write_ts, replicate_ts, remote_write_ts, update_ts;

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

   // record which blocks we've written
   modification_map modified_blocks;
   
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
            }
         }

         return rc;
      }
   }

   fs_entry_unlock( fh->fent );
   
   char* block = CALLOC_LIST( char, core->blocking_factor );

   while( (size_t)num_written < count ) {

      uint64_t block_id = fs_entry_block_id( core, offset + num_written );
      
      // we only need to read the block first if we're merging changes;
      // no need to do so if we're overwriting entirely.
      // That is, read the first block modified, and read the last block modified
      fs_entry_wlock( fh->fent );

      rc = fs_entry_prepare_write_block( core, fh->path, fh->fent, block, count, offset, num_written );
      if( rc != 0 ) {
         // failed to prepare the block for writing
         errorf("fs_entry_prepare_write_block(%s[%" PRId64 "]) rc = %d\n", fh->path, block_id, rc );
      }
      
      ssize_t write_size = fs_entry_fill_block( core, fh->fent, block, buf, source_fd, count, offset, num_written );
      if( write_size < 0 ) {
         errorf("fs_entry_fill_block(%s[%" PRId64 "]) rc = %zd\n", fh->path, block_id, write_size );
         fs_entry_unlock( fh->fent );
         break;
      }
      
      rc = fs_entry_put_block( core, fh->fent, block_id, block, !local );
      if( rc != 0 ) {
         errorf("fs_entry_put_block(%s[%" PRId64 "]) rc = %d\n", fh->path, block_id, rc );
         fs_entry_unlock( fh->fent );
         break;
      }

      // record that we've written this block
      struct fs_entry_block_info binfo;
      memset( &binfo, 0, sizeof(binfo) );

      binfo.version = fh->fent->manifest->get_block_version( block_id );
      binfo.hash = sha256_hash_data( block, core->blocking_factor );
      binfo.hash_len = sha256_len();
      
      modified_blocks[ block_id ] = binfo;

      num_written += write_size;

      fs_entry_unlock( fh->fent );

      // next block
      memset( block, 0, core->blocking_factor );
   }

   free( block );

   if( rc != 0 )
      ret = rc;
   else
      ret = count;

   END_TIMING_DATA( write_ts, ts2, "write data" );

   fs_entry_wlock( fh->fent );

   BEGIN_TIMING_DATA( replicate_ts );
   
   // if we wrote data, replicate the manifest and blocks.
   if( modified_blocks.size() > 0 ) {

      uint64_t start_id = modified_blocks.begin()->first;
      uint64_t end_id = modified_blocks.rbegin()->first + 1;      // exclusive

      int rc = fs_entry_replicate_write( core, fh->path, fh->fent, &modified_blocks, fh->flags & O_SYNC );
      if( rc != 0 ) {
         errorf("fs_entry_replicate_write(%s[%" PRId64 "-%" PRId64 "]) rc = %d\n", fh->path, start_id, end_id, rc );
         ret = -EIO;
      }
   }

   END_TIMING_DATA( replicate_ts, ts2, "replicate data" );

   if( ret > 0 && count > 0 ) {

      BEGIN_TIMING_DATA( remote_write_ts );
      
      // SUCCESS!

      uint64_t start_id = modified_blocks.begin()->first;
      uint64_t end_id = modified_blocks.rbegin()->first + 1;      // exclusive

      // NOTE: size may have changed due to expansion, but it shouldn't affect this computation
      fh->fent->size = MAX( fh->fent->size, (unsigned)(offset + count) );

      if( !local ) {
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
      }

      END_TIMING_DATA( remote_write_ts, ts2, "send remote write" );
   }

   if( ret > 0 && local ) {

      BEGIN_TIMING_DATA( update_ts );
      
      // synchronize the new modifications with the MS
      struct md_entry ent;
      fs_entry_to_md_entry( core, &ent, fh->fent, fh->parent_id, fh->parent_name );

      int up_rc = 0;
      char const* errstr = NULL;
      
      if( fh->fent->max_write_freshness > 0 || !(fh->flags & O_SYNC) ) {
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
int fs_entry_remote_write( struct fs_core* core, char const* fs_path, Serialization::WriteMsg* write_msg ) {
   int err = 0;
   uint64_t parent_id = 0;
   char* parent_name = NULL;
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, fs_path, write_msg->user_id(), write_msg->volume_id(), true, &err, &parent_id, &parent_name );
   if( err != 0 || fent == NULL ) {
      return err;
   }
   
   uint64_t gateway_id = write_msg->gateway_id();

   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );

   // update the blocks
   for( unsigned int i = 0; i < write_msg->blocks().end_id() - write_msg->blocks().start_id(); i++ ) {
      uint64_t block_id = i + write_msg->blocks().start_id();
      int64_t new_version = write_msg->blocks().version(i);

      fs_entry_manifest_put_block( core, gateway_id, fent, block_id, new_version, false );
   }

   fent->size = write_msg->metadata().size();

   clock_gettime( CLOCK_REALTIME, &ts );

   fent->mtime_sec = ts.tv_sec;
   fent->mtime_nsec = ts.tv_nsec;

   // propagate the update to the MS
   struct md_entry data;
   fs_entry_to_md_entry( core, &data, fent, parent_id, parent_name );
   
   char const* errstr = NULL;
   
   uint64_t max_write_freshness = fent->max_write_freshness;
   fs_entry_unlock( fent );
   
   if( max_write_freshness > 0 ) {
      err = ms_client_queue_update( core->ms, &data, currentTimeMillis() + max_write_freshness, 0 );
      errstr = "ms_client_queue_update";
   }
   else {
      err = ms_client_update( core->ms, &data );
      errstr = "ms_client_update";
   }
   if( err != 0 ) {
      errorf("%s(%s) rc = %d\n", errstr, fs_path, err );
      err = -EREMOTEIO;
   }

   md_entry_free( &data );
   free( parent_name );
   
   END_TIMING_DATA( ts, ts2, "write, remote" );
   return err;
}
