/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "collator.h"
#include "read.h"
#include "manifest.h"
#include "storage.h"
#include "network.h"
#include "url.h"
#include "fs_entry.h"

// verify the integrity of a block, given the fent (and its manifest).
// fent must be at least read-locked
int fs_entry_verify_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len ) {
   unsigned char* block_hash = BLOCK_HASH_DATA( block_bits, block_len );
   
   int rc = fent->manifest->hash_cmp( block_id, block_hash );
   
   free( block_hash );
   
   if( rc != 0 ) {
      errorf("Hash mismatch (rc = %d)\n", rc );
      return -EINVAL;
   }
   else {
      return 0;
   }
}

// read a block known to be local
ssize_t fs_entry_read_local_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len ) {

   if( block_id * block_len >= (unsigned)fent->size ) {
      return 0;      // EOF
   }

   // get the URL (must be local)
   char* block_url = fent->manifest->get_block_url( core, NULL, fent, block_id );

   if( block_url == NULL ) {
      // something's wrong
      errorf( "no URL for data at %" PRId64 "\n", block_id );
      return -ENODATA;
   }

   // this is a locally-hosted block--get its bits
   int fd = open( GET_PATH( block_url ), O_RDONLY );
   if( fd < 0 ) {
      fd = -errno;
      errorf( "could not open %s, errno = %d\n", GET_PATH( block_url ), fd );
      free( block_url );
      return fd;
   }

   ssize_t nr = fs_entry_get_block_local( core, fd, block_bits, block_len );
   if( nr < 0 ) {
      errorf("fs_entry_get_block_local(%d) rc = %zd\n", fd, nr );
      free( block_url );
      close( fd );
      return nr;
   }
   
   close( fd );
   free( block_url );
   
   // verify
   int rc = fs_entry_verify_block( core, fent, block_id, block_bits, block_len );
   if( rc != 0 )
      nr = rc;

   else
      dbprintf("read %zd bytes locally\n", nr );
   
   return nr;
}


// read a block known to be remote
// fent must be read-locked
ssize_t fs_entry_read_remote_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len ) {

   if( block_id * block_len >= (unsigned)fent->size ) {
      return 0;      // EOF
   }

   if( fs_path == NULL ) {
      return -EINVAL;
   }

   // this is a remotely-hosted block--get its bits
   uint64_t block_version = fent->manifest->get_block_version( block_id );
   char* block_url = NULL;
   
   int gateway_type = ms_client_get_gateway_type( core->ms, fent->coordinator );
   
   if( gateway_type < 0 ) {
      // unknown gateway---maybe try reloading the certs?
      errorf("Unknown gateway %" PRIu64 "\n", fent->coordinator );
      ms_client_sched_volume_reload( core->ms );
      return -EAGAIN;
   }
      
   if( gateway_type == SYNDICATE_UG )
      block_url = fs_entry_remote_block_url( core, fent->coordinator, fs_path, fent->version, block_id, block_version );
   else if( gateway_type == SYNDICATE_RG )
      block_url = fs_entry_RG_block_url( core, fent->coordinator, fent->volume, fent->file_id, fent->version, block_id, block_version );
   else if( gateway_type == SYNDICATE_AG )
      block_url = fs_entry_AG_block_url( core, fent->coordinator, fs_path, fent->version, block_id, block_version );
   
   if( block_url == NULL ) {
      errorf("Failed to compute block URL for Gateway %" PRIu64 "\n", fent->coordinator);
      return -ENODATA;
   }

   char* block_buf = NULL;
   ssize_t nr = fs_entry_download_block( core, block_url, &block_buf, block_len );
   
   if( nr <= 0 ) {
      errorf("fs_entry_download_block(%s) rc = %zd\n", block_url, nr );
   }
   
   if( nr <= 0 && gateway_type != SYNDICATE_AG ) {
      // try from an RG
      uint64_t rg_id = 0;
      
      int rc = fs_entry_download_block_replica( core, fent->volume, fent->file_id, fent->version, block_id, block_version, &block_buf, block_len, &rg_id );
      
      if( rc != 0 ) {
         // error
         errorf("Failed to read /%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64 " from RGs\n", fent->volume, fent->file_id, fent->version, block_id, block_version );
         nr = -ENODATA;
      }
      else {
         // success!
         dbprintf("Read /%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64 " from RG %" PRIu64 "\n", fent->volume, fent->file_id, fent->version, block_id, block_version, rg_id );
         nr = block_len;
      }
   }
   if( nr <= 0 ) {
      nr = -ENODATA;
   }
   else {
      // verify the block
      int rc = fs_entry_verify_block( core, fent, block_id, block_buf, block_len );
      if( rc != 0 ) {
         nr = rc;
      }
      else {
         memcpy( block_bits, block_buf, nr );
         dbprintf("read %ld bytes remotely\n", (long)nr);
      }
      
      free( block_buf );
   }

   free( block_url );
   return nr;
}


// Given an offset, get the corresponding block's data
// fent must be at least read-locked first
ssize_t fs_entry_do_read_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len ) {

   if( block_id * block_len >= (unsigned)fent->size ) {
      return 0;      // EOF
   }
   
   int loc = fent->manifest->is_block_local( core, block_id );
   if( loc > 0 ) {
      dbprintf("%s.%" PRId64 "/%" PRIu64 " is local\n", fs_path, fent->version, block_id );
      return fs_entry_read_local_block( core, fent, block_id, block_bits, block_len );
   }

   else if( loc == 0 ) {
      dbprintf("%s.%" PRId64 "/%" PRIu64 " is remote\n", fs_path, fent->version, block_id );
      return fs_entry_read_remote_block( core, fs_path, fent, block_id, block_bits, block_len );
   }
   
   else {
      // likely due to a truncate
      errorf("Block %s.%" PRId64 "/%" PRIu64 " does not exist\n", fs_path, fent->version, block_id);
      return -ENOENT;
   }
}

// read a block, given a path and block ID
ssize_t fs_entry_read_block( struct fs_core* core, char const* fs_path, uint64_t block_id, char* block_bits, size_t block_len ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      return err;
   }

   ssize_t ret = fs_entry_do_read_block( core, fs_path, fent, block_id, block_bits, block_len );

   fs_entry_unlock( fent );

   return ret;
}


// read data from a file
ssize_t fs_entry_read( struct fs_core* core, struct fs_file_handle* fh, char* buf, size_t count, off_t offset ) {
   fs_file_handle_rlock( fh );
   
   if( fh->open_count <= 0 ) {
      // invalid
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   struct timespec ts, ts2, read_ts;
   
   BEGIN_TIMING_DATA( ts );
   
   int rc = fs_entry_revalidate_metadata( core, fh->path, fh->fent, NULL );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_metadata(%s) rc = %d\n", fh->path, rc );
      fs_file_handle_unlock( fh );
      return -EREMOTEIO;
   }
   
   fs_entry_rlock( fh->fent );
   
   if( !IS_STREAM_FILE( *(fh->fent) ) && fh->fent->size < offset ) {
      // eof
      fs_entry_unlock( fh->fent );
      fs_file_handle_unlock( fh );
      return 0;
   }

   off_t file_size = fh->fent->size;
   
   fs_entry_unlock( fh->fent );
   
   BEGIN_TIMING_DATA( read_ts );
   
   ssize_t total_read = 0;

   // if we're reading from an AG, then the blocksize is different...
   size_t block_len = 0;
   if( fh->is_AG )
      block_len = fh->AG_blocksize;
   else
      block_len = core->blocking_factor;
   
   ssize_t block_offset = offset % block_len;
   ssize_t ret = 0;
   

   char* block = CALLOC_LIST( char, block_len );

   vector<uint64_t> collated_blocks;

   bool done = false;
   while( (size_t)total_read < count && ret >= 0 && !done ) {
      // read the next block
      fs_entry_rlock( fh->fent );
      bool eof = (unsigned)(offset + total_read) >= fh->fent->size;

      if( eof ) {
         ret = 0;
         fs_entry_unlock( fh->fent );
         break;
      }

      // TODO: unlock fent somehow while we're reading/downloading
      uint64_t block_id = fs_entry_block_id( block_len, offset + total_read );
      ssize_t tmp = fs_entry_do_read_block( core, fh->path, fh->fent, block_id, block, block_len );
      
      if( tmp > 0 ) {
         size_t read_if_not_eof = (unsigned)MIN( (size_t)(tmp - block_offset), count - total_read );
         size_t read_if_eof = file_size - (total_read + offset);
         
         ssize_t total_copy = IS_STREAM_FILE( *(fh->fent) ) ? read_if_not_eof : MIN( read_if_eof, read_if_not_eof );
         
         if( total_copy == 0 ) {
            // EOF
            ret = total_read;
            done = true;
         }
         
         else {
            dbprintf("file_size = %zd, total_read = %zd, tmp = %zd, block_offset = %jd, count = %zu, total_copy = %zd\n", file_size, total_read, tmp, (intmax_t)block_offset, count, total_copy);
            
            memcpy( buf + total_read, block + block_offset, total_copy );

            if( !IS_STREAM_FILE( *(fh->fent) ) ) {
               // did we re-integrate this block?
               // if so, store it
               int64_t block_id = fs_entry_block_id( block_len, offset + total_read );
               if( FS_ENTRY_LOCAL( core, fh->fent ) && !fh->fent->manifest->is_block_local( core, block_id ) ) {
                  int rc = fs_entry_collate( core, fh->fent, block_id, fh->fent->manifest->get_block_version( block_id ), block, block_len, fh->parent_id, fh->parent_name );
                  if( rc != 0 ) {
                     errorf("WARN: fs_entry_collate_block(%s, %" PRId64 ") rc = %d\n", fh->path, block_id, rc );
                  }
                  else {
                     collated_blocks.push_back( block_id );
                  }
               }
            }
            
            total_read += total_copy;
         }
      }
      else if( tmp >= 0 && tmp != (signed)block_len ) {
         // EOF
         dbprintf("EOF after %zd bytes read\n", total_read );
         ret = total_read;
         done = true;
      }
      
      else {
         errorf( "could not read %s, rc = %zd\n", fh->path, tmp );
         ret = tmp;
         done = true;
      }

      fs_entry_unlock( fh->fent );
      block_offset = 0;
   }

   free( block );

   if( ret >= 0 ) {
      ret = total_read;

      // release
      if( collated_blocks.size() > 0 ) {
         uint64_t start_id = collated_blocks[0];
         for( unsigned int i = 0; i < collated_blocks.size(); i++ ) {
            if( start_id + i < collated_blocks[i] ) {
               // new range...
               fs_entry_release_remote_blocks( core, fh->path, fh->fent, start_id, start_id + i + 1 );
               start_id = collated_blocks[i];
            }
         }
      }
   }
   
   fs_file_handle_unlock( fh );

   END_TIMING_DATA( read_ts, ts2, "read data" );
   
   END_TIMING_DATA( ts, ts2, "read" );

   return ret;
}
