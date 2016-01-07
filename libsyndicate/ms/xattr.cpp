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

#include "libsyndicate/ms/xattr.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/url.h"

#include <endian.h>

// sort comparator 
// xattr_i1 and xattr_i2 are pointers to integers 
// cls is the list of xattr names
// the effect of this method is to order the integer array by xattr name.
static int ms_client_xattr_compar( const void* xattr_i1, const void* xattr_i2, void* cls ) {
   
   char** xattr_names = (char**)cls;
   int* i1 = (int*)xattr_i1;
   int* i2 = (int*)xattr_i2;
   
   return strcmp( xattr_names[*i1], xattr_names[*i2] );
}

// find the hash over a file's xattrs and metadata.
// xattr_names and xattr_values should be the same length, and should be null-terminated
// or, xattr_names and xattr_values and xattr_lengths can all be NULL
// the hash incorporates the volume ID, file ID, xattr nonce, xattr names, and xattr values, in that order.
// the numbers are converted to network byte order first.
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if the number of xattr names and values doesn't match, or if some but not all of (xattr_names, xattr_values, xattr_lengths) are NULL
int ms_client_xattr_hash( unsigned char* sha256_buf, uint64_t volume_id, uint64_t file_id, int64_t xattr_nonce, char** xattr_names, char** xattr_values, size_t* xattr_lengths ) {
   
   uint64_t volume_id_nb = htobe64( volume_id );
   uint64_t file_id_nb = htobe64( file_id );
   uint64_t xattr_nonce_nb = htobe64( (uint64_t)xattr_nonce );
   int* order = NULL;
   size_t num_xattrs = 0;
   size_t i = 0;
   
   if( !((xattr_names != NULL && xattr_values != NULL && xattr_lengths != NULL) || (xattr_names == NULL && xattr_values == NULL && xattr_lengths == NULL)) ) {
       return -EINVAL;
   }
   
    
   SHA256_CTX context;
   SHA256_Init( &context ); 
   
   if( xattr_names != NULL && xattr_values != NULL && xattr_lengths != NULL ) {
       
       // count xattrs
       for( i = 0; xattr_names[i] != NULL && xattr_values[i] != NULL; i++ );
        
       if( xattr_names[i] != NULL || xattr_values[i] != NULL ) {
           return -EINVAL;
       }
       
       order = SG_CALLOC( int, num_xattrs );
       if( order == NULL ) {
           return -ENOMEM;
       }
        
       for( i = 0; i < num_xattrs; i++ ) {
           order[i] = i;
       }
        
       // sort order on xattrs--xattr_names[order[i]] is the ith xattr
       qsort_r( order, num_xattrs, sizeof(int), ms_client_xattr_compar, xattr_names );
   }
   
   // hash metadata
   SHA256_Update( &context, &volume_id_nb, sizeof(volume_id_nb) );
   SHA256_Update( &context, &file_id_nb, sizeof(file_id_nb) );
   SHA256_Update( &context, &xattr_nonce_nb, sizeof(xattr_nonce_nb) );
   
   if( xattr_names != NULL && xattr_values != NULL && xattr_lengths != NULL ) {
        
       // hash xattrs 
       for( size_t i = 0; i < num_xattrs; i++ ) {
        
           SHA256_Update( &context, xattr_names[ order[i] ], strlen(xattr_names[order[i]]) );
           SHA256_Update( &context, xattr_values[ order[i] ], xattr_lengths[ order[i] ] );
       }
       
       SG_safe_free( order );
   }
   
   SHA256_Final( sha256_buf, &context );
   return 0;
}


// extract xattr names, values, and lengths from an ms reply 
// return 0 on success, and set *xattr_names, *xattr_values, *xattr_lengths (the former two will be NULL-terminated)
// return -ENOMEM on OOM 
// return -EINVAL for mismatched quantities of each.
static int ms_client_extract_xattrs( ms::ms_reply* reply, char*** xattr_names, char*** xattr_values, size_t** xattr_lengths ) {
   
   int rc = 0;
   
   if( reply->xattr_names_size() != reply->xattr_values_size() ) {
      return -EINVAL;
   }
   
   size_t num_xattrs = reply->xattr_names_size();
   char** ret_names = NULL;
   char** ret_values = NULL;
   size_t* ret_lengths = NULL;
   
   ret_names = SG_CALLOC( char*, num_xattrs + 1 );
   ret_values = SG_CALLOC( char*, num_xattrs + 1 );
   ret_lengths = SG_CALLOC( size_t, num_xattrs + 1 );
   
   if( ret_names == NULL || ret_values == NULL || ret_lengths == NULL ) {
      SG_safe_free( ret_names );
      SG_safe_free( ret_values );
      SG_safe_free( ret_lengths );
      return -ENOMEM;
   }
   
   for( size_t i = 0; i < num_xattrs; i++ ) {
      
      char* name = SG_strdup_or_null( reply->xattr_names(i).c_str() );
      if( name == NULL ) {
         rc = -ENOMEM;
         break;
      }
      
      char* value = SG_CALLOC( char, reply->xattr_values(i).size() );
      if( value == NULL ) {
         
         SG_safe_free( name );
         rc = -ENOMEM;
         break;
      }
      
      memcpy( value, reply->xattr_values(i).data(), reply->xattr_values(i).size() );
      
      ret_names[i] = name;
      ret_values[i] = value;
      ret_lengths[i] = reply->xattr_values(i).size();
   }
   
   if( rc == -ENOMEM ) {
      
      SG_FREE_LIST( ret_names, free );
      SG_FREE_LIST( ret_values, free );
      
      SG_safe_free( ret_lengths );
      return rc;
   }
   
   *xattr_names = ret_names;
   *xattr_values = ret_values;
   *xattr_lengths = ret_lengths;
   return 0;
}


// fetch and verify all xattrs.
// this method should only be called by the coordinator for the file.
// return 0 on success
// return -EPERM if we failed to verify the set of xattrs against the hash
// return -ENOENT if the file doesn't exist or either isn't readable or writable.
// return -ENODATA if the semantics in flags can't be met.
// return -ENOMEM if OOM 
// return -ENODATA if the replied message has no xattr field
// return -EBADMSG on reply's signature mismatch
// return -EPROTO on HTTP 400-level error
// return -EREMOTEIO for HTTP 500-level error 
// return -errno on socket, connect, and recv related errors
int ms_client_fetchxattrs( struct ms_client* client, uint64_t volume_id, uint64_t file_id, int64_t xattr_nonce, unsigned char* xattr_hash, char*** xattr_names, char*** xattr_values, size_t** xattr_lengths ) {
   
   char* fetchxattrs_url = NULL;
   ms::ms_reply reply;
   int rc = 0;
   char** names = NULL;
   char** values = NULL;
   size_t* lengths = NULL;
   unsigned char hash_buf[SHA256_DIGEST_LENGTH];
   
   fetchxattrs_url = ms_client_fetchxattrs_url( client->url, volume_id, ms_client_volume_version( client ), ms_client_cert_version( client ), file_id );
   if( fetchxattrs_url == NULL ) {
      return -ENOMEM;
   }
   
   rc = ms_client_read( client, fetchxattrs_url, &reply );
   
   SG_safe_free( fetchxattrs_url );
   
   if( rc != 0 ) {
      SG_error("ms_client_read(fetchxattrs) rc = %d\n", rc );
      return rc;
   }
   else {
      
      // check for errors 
      if( reply.error() != 0 ) {
         SG_error("MS replied with error %d\n", reply.error() );
         return reply.error();
      }
      
      // extract the xattrs
      rc = ms_client_extract_xattrs( &reply, &names, &values, &lengths );
      if( rc != 0 ) {
         SG_error("ms_client_extract_xattrs rc = %d\n", rc );
         return rc;
      }
      
      // find the hash over them 
      rc = ms_client_xattr_hash( hash_buf, volume_id, file_id, xattr_nonce, names, values, lengths );
      if( rc != 0 ) {
         
         SG_FREE_LIST( names, free );
         SG_FREE_LIST( values, free );
         SG_safe_free( lengths );
         return rc;
      }
      
      // hash match?
      if( sha256_cmp( xattr_hash, hash_buf ) != 0 ) {
         
         SG_FREE_LIST( names, free );
         SG_FREE_LIST( values, free );
         SG_safe_free( lengths );
         
         char xattr_hash_printable[2*SHA256_DIGEST_LENGTH + 1];
         char hash_buf_printable[2*SHA256_DIGEST_LENGTH + 1];
         
         if( xattr_hash != NULL ) {
            sha256_printable_buf( xattr_hash, xattr_hash_printable );
         }
         else {
            memset( xattr_hash_printable, '0', 2*SHA256_DIGEST_LENGTH );
            xattr_hash_printable[ 2*SHA256_DIGEST_LENGTH ] = 0;
         }
         
         sha256_printable_buf( hash_buf, hash_buf_printable );
         
         SG_error("hash mismatch: %s != %s\n", xattr_hash_printable, hash_buf_printable );
         
         return -EPERM;
      }
      
      // hash match!
      // can save 
      *xattr_names = names;
      *xattr_values = values;
      *xattr_lengths = lengths;
      
      return 0;
   }
}


// make a putxattr request 
// NOTE: shallow-copies
// return 0 on success 
int ms_client_putxattr_request( struct ms_client* ms, struct md_entry* ent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, unsigned char* xattr_hash, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->op = ms::ms_request::PUTXATTR;
   request->flags = 0;
   
   request->ent = ent;
   
   request->xattr_name = xattr_name;
   request->xattr_value = xattr_value;
   request->xattr_value_len = xattr_value_len;
   request->xattr_hash = xattr_hash;
   
   return 0;
}

// put a new xattr name/value, new xattr nonce, and xattr signature.
// only the coordinator should call this, and only to keep its xattrs replica coherent with the MS
// return 0 on success
// return -EPERM if we failed to sign the xattr, for some reason
// return -ENOENT if the file doesn't exist or either isn't readable or writable.
// return -ENODATA if the semantics in flags can't be met.
// return -ENOMEM if OOM 
// return -ENODATA if the replied message has no xattr field
// return -EBADMSG on reply's signature mismatch
// return -EPROTO on HTTP 400-level error, or an MS RPC-level error
// return -EREMOTEIO for HTTP 500-level error 
// return -errno on socket, connect, and recv related errors
int ms_client_putxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, unsigned char* xattr_hash ) {
   
   int rc = 0;
   
   struct ms_client_request request;
   struct ms_client_request_result result;
   
   memset( &request, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   ms_client_putxattr_request( client, ent, xattr_name, xattr_value, xattr_value_len, xattr_hash, &request );
   
   rc = ms_client_single_rpc( client, &request, &result );
   if( rc != 0 ) {
      return rc;
   }
   
   if( result.reply_error != 0 ) {
      // protocol-level error 
      ms_client_request_result_free( &result );
      return -EPROTO;
   }
   
   if( result.rc != 0 ) {
      ms_client_request_result_free( &result );
      return result.rc;
   }
   
   ms_client_request_result_free( &result );
   return 0;
}

/*
// TODO: route through coordinator gateway
// set a file's xattr.
// flags is either 0, XATTR_CREATE, or XATTR_REPLACE (see setxattr(2))
// return 0 on success
// return -EPERM if we failed to sign the xattr, for some reason
// return -ENOENT if the file doesn't exist or either isn't readable or writable.
// return -ENODATA if the semantics in flags can't be met.
// return -ENOMEM if OOM 
// return -ENODATA if the replied message has no xattr field
// return -EBADMSG on reply's signature mismatch
// return -EPROTO on HTTP 400-level error
// return -EREMOTEIO for HTTP 500-level error 
// return -errno on socket, connect, and recv related errors
int ms_client_setxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, int flags ) {
   
   // sanity check...can't have both XATTR_CREATE and XATTR_REPLACE
   if( (flags & (XATTR_CREATE | XATTR_REPLACE)) == (XATTR_CREATE | XATTR_REPLACE) ) {
      return -EINVAL;
   }
   
   // generate our update
   struct md_update up;
   int rc = 0;
   
   ms_client_populate_update( &up, ms::ms_request::SETXATTR, flags, ent );
   
   // add the xattr information (these won't be free'd, so its safe to cast)
   up.xattr_name = (char*)xattr_name;
   
   up.xattr_value = (char*)xattr_value;
   up.xattr_value_len = xattr_value_len;
   
   // TODO: xattr hash 
   
   rc = ms_client_update_rpc( client, &up );
   
   return rc;
}
*/


// make a removexattr request 
// return 0 on success 
int ms_client_removexattr_request( struct ms_client* client, struct md_entry* ent, char const* xattr_name, unsigned char* xattr_hash, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->op = ms::ms_request::REMOVEXATTR;
   request->flags = 0;
   
   request->ent = ent;
   
   request->xattr_name = xattr_name;
   request->xattr_hash = xattr_hash;
   
   return 0;
}

// remove an xattr.
// fails if the file isn't readable or writable, or the xattr exists and it's not writable
// succeeds even if the xattr doesn't exist (i.e. idempotent)
// return 0 on success 
// return -ENOMEM if OOM 
// return -ENODATA if the replied message has no xattr field
// return -EBADMSG on reply's signature mismatch
// return -EPROTO on HTTP 400-level error or an MS RPC error
// return -EREMOTEIO for HTTP 500-level error 
// return -errno on socket, connect, and recv related errors
int ms_client_removexattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, unsigned char* xattr_hash ) {
   
   int rc = 0;
   struct ms_client_request request;
   struct ms_client_request_result result;
   
   memset( &request, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   ms_client_removexattr_request( client, ent, xattr_name, xattr_hash, &request );
   
   rc = ms_client_single_rpc( client, &request, &result );
   if( rc != 0 ) {
      return rc;
   }
   
   if( result.reply_error != 0 ) {
      // protocol-level error 
      ms_client_request_result_free( &result );
      return -EPROTO;
   }
   
   if( result.rc != 0 ) {
      ms_client_request_result_free( &result );
      return result.rc;
   }
   
   ms_client_request_result_free( &result );
   return 0;
}

