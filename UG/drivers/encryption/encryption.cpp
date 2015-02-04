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

#include "encryption.h"


int closure_init( struct md_closure* closure, void** cls ) {
   return 0;
}

int closure_shutdown( void* cls ) {
   return 0;
}

char* get_driver_name(void) {
   return strdup( DRIVER_NAME );
}

// get the file's secret key, or generate a new one and store it
static int get_or_create_encryption_key_and_iv( struct fs_core* core, struct fs_entry* fent, unsigned char** ret_key, size_t* ret_key_len, unsigned char** ret_iv, size_t* ret_iv_len, bool fail_if_absent ) {
   
   // get the key and iv (64 bytes)
   char* key_and_iv = NULL;
   size_t key_and_iv_len = 0;
   int rc = 0;
   int cache_status = 0;
   
   rc = fs_entry_do_getxattr( core, fent, XATTR_ENCRYPT, &key_and_iv, &key_and_iv_len, &cache_status, false );
   if( rc != 0 ) {
      
      if( fail_if_absent ) {
         // don't try to get it 
         return -ENODATA;
      }
      
      if( rc != -ENOENT ) {
         errorf("fs_entry_do_getxattr(%" PRIX64 " %s) rc = %d\n", fent->file_id, XATTR_ENCRYPT, rc );
         return -ENODATA;
      }
      
      else {
         // no key or IV.  generate them and put them
         char new_key_and_iv[64];
         rc = md_read_urandom( new_key_and_iv, 64 );
         if( rc != 0 ) {
            errorf("md_read_urandom rc = %d\n", rc );
            return -ENODATA;
         }
         
         char* new_key_and_iv_b64 = NULL;
         size_t new_key_and_iv_b64_len = 0;
         
         rc = md_base64_encode( new_key_and_iv, 64, &new_key_and_iv_b64 );
         if( rc != 0 ) {
            errorf("md_base64_encode rc = %d\n", rc );
            return -ENODATA;
         }
         
         new_key_and_iv_b64_len = strlen( new_key_and_iv_b64 );
         
         // try to set it 
         char* actual_key_and_iv_b64 = NULL;
         size_t actual_key_and_iv_b64_len = 0;
         
         rc = fs_entry_get_or_set_xattr( core, fent, XATTR_ENCRYPT, new_key_and_iv_b64, new_key_and_iv_b64_len, &actual_key_and_iv_b64, &actual_key_and_iv_b64_len, 0770 );
         free( new_key_and_iv_b64 );
         
         if( rc != 0 ) {
            errorf("fs_entry_get_or_set_xattr(%" PRIX64 "%s) rc = %d\n", fent->file_id, XATTR_ENCRYPT, rc );
            
            return -ENODATA;
         }
         else {
            
            // got the data!
            key_and_iv = actual_key_and_iv_b64;
            key_and_iv_len = actual_key_and_iv_b64_len;
            rc = 0;
         }
      }
   }
   
   if( key_and_iv != NULL ) {
      // use the given key
      char* final_key_and_iv = NULL;
      size_t final_key_and_iv_len = 0;
      
      rc = md_base64_decode( key_and_iv, key_and_iv_len, &final_key_and_iv, &final_key_and_iv_len );
      if( rc != 0 ) {
         errorf("Failed to unserialize key, rc = %d\n", rc );
         
         free( key_and_iv );
         return -ENODATA;
      }
      else {
         // success!
         // split into key and iv 
         size_t sz = final_key_and_iv_len / 2;
         char* final_key = CALLOC_LIST( char, sz );
         char* final_iv = CALLOC_LIST( char, sz );
         
         memcpy( final_key, final_key_and_iv, sz );
         memcpy( final_iv, final_key_and_iv + sz, sz );
         
         *ret_key = (unsigned char*)final_key;
         *ret_key_len = sz;
         
         *ret_iv = (unsigned char*)final_iv;
         *ret_iv_len = sz;
      
         return 0;
      }
   }
   else {
      return -ENODATA;
   }
}


// encrypt a chunk of data, adding padding to it to increase its entropy
static int encrypt_chunk( unsigned char const* key, size_t key_len, unsigned char const* iv, size_t iv_len, char* chunk, size_t chunk_len, char** ciphertext, size_t* ciphertext_len ) {
   char* entropied_chunk = CALLOC_LIST( char, chunk_len + ENTROPY_BYTES );
   
   int rc = md_read_urandom( entropied_chunk, ENTROPY_BYTES );
   if( rc != 0 ) {
      errorf("md_read_urandom rc = %d\n", rc );
      free( entropied_chunk );
      return rc;
   }
   
   memcpy( entropied_chunk + ENTROPY_BYTES, chunk, chunk_len );
   
   // seal it 
   rc = md_encrypt_symmetric_ex( key, key_len, iv, iv_len, chunk, chunk_len, ciphertext, ciphertext_len );
   if( rc != 0 ) {
      errorf("md_encrypt_symmetric rc = %d\n", rc );
      return -ENODATA;
   }
   
   return 0;
}


// decrypt a chunk of data, removing the entropy padding
static int decrypt_chunk( unsigned char const* key, size_t key_len, unsigned char const* iv, size_t iv_len, char* ciphertext, size_t ciphertext_len, char** chunk, size_t* chunk_len ) {
   char* _chunk = NULL;
   size_t _chunk_len = 0;
   int rc = 0;
   
   // unseal 
   rc = md_decrypt_symmetric_ex( key, key_len, iv, iv_len, ciphertext, ciphertext_len, &_chunk, &_chunk_len );
   if( rc != 0 ) {
      errorf("md_decrypt_symmetric rc = %d\n", rc );
      return rc;
   }
   
   // sanity check 
   if( _chunk_len <= ENTROPY_BYTES ) {
      errorf("Plaintext too short (%zu)\n", _chunk_len );
      
      free( _chunk );
      return -EINVAL;
   }
   
   // remove entropy 
   size_t ret_len = _chunk_len - ENTROPY_BYTES;
   char* ret = CALLOC_LIST( char, ret_len );
   memcpy( ret, _chunk + ENTROPY_BYTES, ret_len );
   
   free( _chunk );
   
   *chunk = ret;
   *chunk_len = ret_len;
   
   return 0;
}


// seal a chunk of data 
static int seal_data( struct fs_core* core, struct fs_entry* fent, char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   int rc = 0;
   
   unsigned char *key = NULL, *iv = NULL;
   char* ciphertext = NULL;
   
   size_t key_len = 0, iv_len = 0, ciphertext_len = 0;
   
   // get or create encryption primitives
   rc = get_or_create_encryption_key_and_iv( core, fent, &key, &key_len, &iv, &iv_len, false );
   if( rc != 0 ) {
      errorf("get_or_create_encryption_key_and_iv rc = %d\n", rc );
      return -ENODATA;
   }
   
   // seal the data 
   rc = encrypt_chunk( key, key_len, iv, iv_len, in_data, in_data_len, &ciphertext, &ciphertext_len );
   if( rc != 0 ) {
      errorf("encrypt_chunk rc = %d\n", rc );
      
      free( key );
      free( iv );
      return -ENODATA;
   }
   
   *out_data = ciphertext;
   *out_data_len = ciphertext_len;
   
   return 0;
}


// unseal a chunk of data 
static int unseal_data( struct fs_core* core, struct fs_entry* fent, char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   int rc = 0;
   
   unsigned char *key = NULL, *iv = NULL;
   char* ciphertext = NULL;
   
   size_t key_len = 0, iv_len = 0, ciphertext_len = 0;
   
   // get the encryption primitives, but fail if they don't exist
   rc = get_or_create_encryption_key_and_iv( core, fent, &key, &key_len, &iv, &iv_len, true );
   if( rc != 0 ) {
      errorf("get_or_create_encryption_key_and_iv rc = %d\n", rc );
      return -ENODATA;
   }
   
   // unseal the data 
   rc = decrypt_chunk( key, key_len, iv, iv_len, in_data, in_data_len, &ciphertext, &ciphertext_len );
   if( rc != 0 ) {
      errorf("encrypt_chunk rc = %d\n", rc );
      
      free( key );
      free( iv );
      return -ENODATA;
   }
   
   *out_data = ciphertext;
   *out_data_len = ciphertext_len;
   
   return 0;
}

// connect to cache 
int connect_cache( struct fs_core* core, struct md_closure* closure, CURL* curl, char const* url, void* cls ) {
   return ms_client_volume_connect_cache( core->ms, curl, url );
}

// encrypt a block with the file's secret key
int write_block_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                       char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls ) {
   
   int rc = seal_data( core, fent, in_data, in_data_len, out_data, out_data_len );
   if( rc != 0 ) {
      errorf("seal_data(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", fs_path, fent->file_id, fent->version, block_id, block_version, rc );
      rc = -EIO;
   }
   
   return rc;
}


// encrypt a manifest with the file's secret key
int write_manifest_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                          char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls ) {
   
   int rc = seal_data( core, fent, in_data, in_data_len, out_data, out_data_len );
   if( rc != 0 ) {
      errorf("seal_data(%s %" PRIX64 ".%" PRId64 ".manifest.%" PRId64 ".%d rc = %d\n", fs_path, fent->file_id, fent->version, mtime_sec, mtime_nsec, rc );
      rc = -EIO;
   }
   
   return rc;
}

// decrypt a block with the file's secret key
ssize_t read_block_postdown( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                             char* in_data, size_t in_data_len, char* out_data, size_t out_data_len, void* cls ) {
   
   // maximal block size is expected...
   char* _out_data = NULL;
   size_t _out_data_len = 0;
   ssize_t ret = 0;
   
   int rc = unseal_data( core, fent, in_data, in_data_len, &_out_data, &_out_data_len );
   if( rc != 0 ) {
      errorf("unseal_data(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, block_version, rc );
      rc = -ENODATA;
      
      ret = rc;
   }
   
   else if( _out_data_len > out_data_len ) {
      // too big 
      errorf("unsealed data is too big (%zu > %zu)\n", _out_data_len, out_data_len );
      rc = -ENODATA;
      
      ret = rc;
   }
   
   else {
      memcpy( out_data, _out_data, _out_data_len );
      free( _out_data );
      rc = 0;
      
      ret = _out_data_len;
   }
   
   return ret;
}

// decrypt a manifest with the file's secret key
int read_manifest_postdown( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                            char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls ) {
   
   int rc = unseal_data( core, fent, in_data, in_data_len, out_data, out_data_len );
   if( rc != 0 ) {
      errorf("unseal_data(%s %" PRIX64 ".%" PRId64 ".manifest.%" PRId64 ".%d ) rc = %d\n", fs_path, fent->file_id, fent->version, mtime_sec, mtime_nsec, rc );
      rc = -ENODATA;
   }
   
   return rc;
}

// nothing to do for change of coordinator...
int chcoord_begin( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t new_coordinator_id, void* cls ) {
   return 0;
}

// nothing to do for change of coordinator...
int chcoord_end( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t new_coodinator_id, int chcoord_status, void* cls ) {
   return 0;
}