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

#include "cache.h"
#include "read.h"
#include "manifest.h"
#include "network.h"
#include "url.h"
#include "fs_entry.h"
#include "driver.h"

// verify the integrity of a block, given the fent (and its manifest).
// fent must be at least read-locked
int fs_entry_verify_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len ) {
   unsigned char* block_hash = BLOCK_HASH_DATA( block_bits, block_len );
   
   int rc = fent->manifest->hash_cmp( block_id, block_hash );
   
   free( block_hash );
   
   if( rc != 0 ) {
      errorf("Hash mismatch (rc = %d, len = %zu)\n", rc, block_len );
      return -EPROTO;
   }
   else {
      return 0;
   }
}


// download a block from a remote host.
// fent must be read-locked
ssize_t fs_entry_read_remote_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, uint64_t block_version, char** block_bits ) {

   if( fs_path == NULL ) {
      return -EINVAL;
   }

   // this is a remotely-hosted block--get its bits
   char* block_url = NULL;
   
   int gateway_type = ms_client_get_gateway_type( core->ms, fent->coordinator );
   
   if( gateway_type < 0 ) {
      // unknown gateway---maybe try reloading the certs?
      errorf("Unknown gateway %" PRIu64 "\n", fent->coordinator );
      ms_client_sched_volume_reload( core->ms );
      return -EAGAIN;
   }
      
   char* block_buf = NULL;
   ssize_t nr = 0;
   
   // this file may be locally coordinated, so don't download from ourselves.
   if( !FS_ENTRY_LOCAL( core, fent ) ) {
      
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

      nr = fs_entry_download_block( core, fs_path, fent, block_id, block_version, block_url, &block_buf );
      
      if( nr <= 0 ) {
         errorf("fs_entry_download_block(%s) rc = %zd\n", block_url, nr );
      }
   }
   
   if( FS_ENTRY_LOCAL( core, fent ) || (nr <= 0 && gateway_type != SYNDICATE_AG) ) {
      // try from an RG
      uint64_t rg_id = 0;
      
      nr = fs_entry_download_block_replica( core, fs_path, fent, block_id, block_version, &block_buf, &rg_id );
      
      if( nr < 0 ) {
         // error
         errorf("Failed to read /%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64 " from RGs, rc = %zd\n", fent->volume, fent->file_id, fent->version, block_id, block_version, nr );
      }
      else {
         // success!
         dbprintf("Fetched %zd bytes of /%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64 " from RG %" PRIu64 "\n", nr, fent->volume, fent->file_id, fent->version, block_id, block_version, rg_id );
      }
   }
   if( nr < 0 ) {
      nr = -ENODATA;
   }
   else {
      // verify the block
      // TODO: do this for AGs as well, once AGs hash blocks
      if( gateway_type != SYNDICATE_AG ) {
         int rc = fs_entry_verify_block( core, fent, block_id, block_buf, nr );
         if( rc != 0 ) {
            nr = rc;
         }
      }
      if( nr >= 0 ) {
         *block_bits = block_buf;
         dbprintf("read %ld bytes remotely\n", (long)nr);
      }
   }

   if( block_url )
      free( block_url );
   return nr;
}


// Given an offset, get the corresponding block's data.
// return the number of bytes actually read (up to block_len, but can be less)
// fent must be at least read-locked first
ssize_t fs_entry_read_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len ) {

   if( block_id * block_len >= (size_t)fent->size ) {
      dbprintf("EOF found! : block_id = %ld, block_len = %ld, file_size = %ld\n", block_id, block_len, fent->size);
      return 0;      // EOF
   }
   
   bool hit_cache = false;
   
   char* block_buf = NULL;
   ssize_t read_len = 0;
   
   uint64_t block_version = fent->manifest->get_block_version( block_id );
   
   // local?
   int block_fd = fs_entry_cache_open_block( core, core->cache, fent->file_id, fent->version, block_id, block_version, O_RDONLY );

   dbprintf("file_id = %ld, version = %ld, block_id = %ld, block_fd = %d\n", fent->file_id, fent->version, block_id, block_fd);

   if( block_fd < 0 ) {
      if( block_fd != -ENOENT ) {
         errorf("WARN: fs_entry_cache_open_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n", fent->file_id, fent->version, block_id, block_version, fs_path, block_fd );
      }
   }
   else {
      read_len = fs_entry_cache_read_block( core, core->cache, fent->file_id, fent->version, block_id, block_version, block_fd, &block_buf );
      if( read_len < 0 ) {
         errorf("fs_entry_cache_read_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n", fent->file_id, fent->version, block_id, block_version, fs_path, (int)read_len );
      }
      else {
         // done!
         hit_cache = true;
         
         // promote!
         fs_entry_cache_promote_block( core, core->cache, fent->file_id, fent->version, block_id, block_version );
      }
      
      close( block_fd );
      
      dbprintf("Cache HIT on %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", fent->file_id, fent->version, block_id, block_version );
   }
   
   bool cache_result = false;
   
   if( !hit_cache ) {
      // get it remotely
      read_len = fs_entry_read_remote_block( core, fs_path, fent, block_id, block_version, &block_buf );

      if( read_len < 0 ) {
         errorf("fs_entry_read_remote_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s)) rc = %d\n", fent->file_id, fent->version, block_id, block_version, fs_path, (int)read_len );
      }
      else {
         // cache result on successful post-download processing 
         cache_result = true;
      }
   }
   
   // process the block 
   ssize_t processed_len = driver_read_block_postdown( core, core->closure, fs_path, fent, block_id, block_version, block_buf, read_len, block_bits, block_len );
   
   if( processed_len < 0 ) {
      free( block_buf );
      errorf("driver_read_postdown(%s = %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]) rc = %zd\n", fs_path, fent->file_id, fent->version, block_id, block_version, processed_len );
      return (ssize_t)processed_len;
   }
   else {
      if( cache_result ) {
         // successful post-download processing.  Cache the result.
         // NOTE: the cache will free the downloaded buffer.
         // NOTE: don't wait for it to finish (i.e. do NOT free the future)--it'll get freed by the cache.
         int cache_rc = 0;
         struct cache_block_future* fut = fs_entry_cache_write_block_async( core, core->cache, fent->file_id, fent->version, block_id, block_version, block_buf, read_len, true, &cache_rc );
         if( fut == NULL || cache_rc != 0 ) {
            errorf("WARN: failed to cache %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", fent->file_id, fent->version, block_id, block_version, cache_rc );
         }
         
         dbprintf("Cache MISS on %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "],\n", fent->file_id, fent->version, block_id, block_version );
      }
      else {
         // will not cache
         free( block_buf );
      }
      
      return (ssize_t)processed_len;
   }
}

// read a block, given a path and block ID
ssize_t fs_entry_read_block( struct fs_core* core, char const* fs_path, uint64_t block_id, char* block_bits, size_t block_len ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      return err;
   }

   ssize_t ret = fs_entry_read_block( core, fs_path, fent, block_id, block_bits, block_len );

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
   
   off_t total_read = 0;

   // if we're reading from an AG, then the blocksize is different...
   size_t block_len = 0;
   if( fh->is_AG )
      block_len = fh->AG_blocksize;
   else
      block_len = core->blocking_factor;
   
   off_t block_offset = offset % block_len;
   ssize_t ret = 0;
   

   char* block = CALLOC_LIST( char, block_len );

   dbprintf("fs_entry_read : blockLen = %ld, offset = %ld, readLen = %ld, filesize = %ld\n", block_len, offset, count, file_size);

   bool done = false;
   while( (size_t)total_read < count && ret >= 0 && !done ) {
      // read the next block
      fs_entry_rlock( fh->fent );
      bool eof = (offset + total_read >= fh->fent->size && !IS_STREAM_FILE( *(fh->fent) ));

      if( eof ) {
         dbprintf("EOF after reading %zd bytes\n", total_read);
         ret = 0;
         fs_entry_unlock( fh->fent );
         break;
      }

      uint64_t block_id = fs_entry_block_id( block_len, offset + total_read );
      ssize_t tmp = fs_entry_read_block( core, fh->path, fh->fent, block_id, block, block_len );
     
      dbprintf("fs_entry_read : block_id = %ld, block_read_size = %ld\n", block_id, tmp);
 
      if( tmp > 0 ) {
         off_t read_if_not_eof = (off_t)MIN( (off_t)(tmp - block_offset), (off_t)count - total_read );
         off_t read_if_eof = file_size - (total_read + offset);
         
         off_t total_copy = IS_STREAM_FILE( *(fh->fent) ) ? read_if_not_eof : MIN( read_if_eof, read_if_not_eof );
         
         if( total_copy == 0 ) {
            // EOF
            ret = total_read;
            done = true;
         }
         
         else {
            dbprintf("file_size = %ld, total_read = %ld, tmp = %zd, block_offset = %ld, count = %zu, total_copy = %ld\n", (long)file_size, (long)total_read, tmp, (long)block_offset, count, (long)total_copy);
            
            memcpy( buf + total_read, block + block_offset, total_copy );

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
         // EOF on a stream file?
         if( IS_STREAM_FILE( *(fh->fent) ) && tmp == -ENOENT ) {
            // at the end of the file
            ret = 0;
         }
         else {
            errorf( "could not read %s, rc = %zd\n", fh->path, tmp );
            ret = tmp;
         }
         done = true;
      }

      fs_entry_unlock( fh->fent );
      block_offset = 0;
   }

   free( block );

   if( ret >= 0 ) {
      ret = total_read;
   }
   
   fs_file_handle_unlock( fh );

   END_TIMING_DATA( read_ts, ts2, "read data" );
   
   END_TIMING_DATA( ts, ts2, "read" );

   return ret;
}
