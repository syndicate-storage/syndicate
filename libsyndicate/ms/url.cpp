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

#include "libsyndicate/ms/url.h"

// make a URL to a given MS request path
// return the URL on success 
// return NULL on OOM
char* ms_client_url( char const* ms_url, uint64_t volume_id, char const* metadata_path ) {
   char volume_id_str[50];
   sprintf(volume_id_str, "%" PRIu64, volume_id);

   char* volume_md_path = md_fullpath( metadata_path, volume_id_str, NULL );
   
   if( volume_md_path == NULL ) {
      return NULL;
   }

   char* url = md_fullpath( ms_url, volume_md_path, NULL );

   free( volume_md_path );

   return url;
}

// POST url for a file
// return the URL on success
// return NULL on OOM
char* ms_client_file_url( char const* ms_url, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version ) {
   
   char volume_id_str[50];
   char volume_version_str[50];
   char cert_version_str[50];
   
   sprintf( volume_id_str, "%" PRIu64, volume_id );
   sprintf( volume_version_str, "%" PRIu64, volume_version );
   sprintf( cert_version_str, "%" PRIu64, cert_version );

   char* volume_file_path = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/FILE/") + 1 + strlen(volume_id_str) + 1 + strlen(volume_version_str) + 1 + strlen(cert_version_str) + 1 );
   if( volume_file_path == NULL ) {
      return NULL;
   }

   sprintf( volume_file_path, "%s/FILE/%s.%s.%s", ms_url, volume_id_str, volume_version_str, cert_version_str );
   
   return volume_file_path;
}

// query arg concat
int ms_client_arg_concat( char* url, char const* arg, bool first ) {
   
   if( first ) {
      strcat( url, "?" );
      strcat( url, arg );
   }
   else {
      strcat( url, "&" );
      strcat( url, arg );
   }
   
   return 0;
}

// GETATTR url for a file
// return the URL on success
// return NULL on OOM
char* ms_client_file_getattr_url( char const* ms_url, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version, uint64_t file_id, int64_t version, int64_t write_nonce ) {

   char volume_id_str[50];
   char volume_version_str[50];
   char cert_version_str[50];
   
   sprintf( volume_id_str, "%" PRIu64, volume_id );
   sprintf( volume_version_str, "%" PRIu64, volume_version );
   sprintf( cert_version_str, "%" PRIu64, cert_version );

   char file_id_str[50];
   sprintf( file_id_str, "%" PRIX64, file_id );

   char version_str[50];
   sprintf( version_str, "%" PRId64, version );

   char write_nonce_str[60];
   sprintf( write_nonce_str, "%" PRId64, write_nonce );

   char* volume_file_url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/FILE/GETATTR/") + 1 + strlen(volume_id_str) + 1 + strlen(volume_version_str) + 1 + 
                                            strlen(cert_version_str) + 1 + strlen(file_id_str) + 1 + strlen(version_str) + 1 + strlen(write_nonce_str) + 1 );

   if( volume_file_url == NULL ) {
      return NULL;  
   }
   
   sprintf( volume_file_url, "%s/FILE/GETATTR/%s.%s.%s/%s.%s.%s", ms_url, volume_id_str, volume_version_str, cert_version_str, file_id_str, version_str, write_nonce_str );
   
   return volume_file_url;
}


// GETCHILD url for a file
// return the URL on success
// return NULL on OOM
char* ms_client_file_getchild_url( char const* ms_url, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version, uint64_t file_id, char* name ) {

   char volume_id_str[50];
   char volume_version_str[50];
   char cert_version_str[50];
   
   sprintf( volume_id_str, "%" PRIu64, volume_id );
   sprintf( volume_version_str, "%" PRIu64, volume_version );
   sprintf( cert_version_str, "%" PRIu64, cert_version );
   
   char file_id_str[50];
   sprintf( file_id_str, "%" PRIX64, file_id );

   char* volume_file_url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/FILE/GETCHILD/") + 1 + strlen(volume_id_str) + 1 + strlen(volume_version_str) + 1 + strlen(cert_version_str) + 1 +
                                            strlen(file_id_str) + 1 + strlen(name) + 1 );
   
   if( volume_file_url == NULL ) {
      return NULL;
   }
   
   sprintf( volume_file_url, "%s/FILE/GETCHILD/%s.%s.%s/%s/%s", ms_url, volume_id_str, volume_version_str, cert_version_str, file_id_str, name );
   
   return volume_file_url;
}

// LISTDIR url for a file
// if page_id >= 0, include page_id=...
// if least_unknown_generation >= 0, include lug=...
// return the URL on success
// return NULL on OOM
char* ms_client_file_listdir_url( char const* ms_url, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version, uint64_t file_id, int64_t page_id, int64_t least_unknown_generation ) {

   char volume_id_str[50];
   char volume_version_str[50];
   char cert_version_str[50];
   
   sprintf( volume_id_str, "%" PRIu64, volume_id );
   sprintf( volume_version_str, "%" PRIu64, volume_version );
   sprintf( cert_version_str, "%" PRIu64, cert_version );
   
   char file_id_str[50];
   sprintf( file_id_str, "%" PRIX64, file_id );
   
   size_t page_id_len = 0;
   size_t file_ids_only_len = 0;
   bool query_args = false;
   
   if( page_id >= 0 ) {
      page_id_len = 50;
   }
   
   if( least_unknown_generation >= 0 ) {
      file_ids_only_len = strlen("&lug=") + 50;
   }
   
   char* volume_file_url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/FILE/LISTDIR/") + 1 + strlen(volume_id_str) + 1 + strlen(volume_version_str) + 1 + strlen(cert_version_str) + 1 + 
                                            strlen(file_id_str) + 1 + page_id_len + 1 + file_ids_only_len + 1 );
   
   if( volume_file_url == NULL ) {
      return NULL;
   }
   
   sprintf( volume_file_url, "%s/FILE/LISTDIR/%s.%s.%s/%s", ms_url, volume_id_str, volume_version_str, cert_version_str, file_id_str );
   
   if( page_id >= 0 ) {
      
      char page_id_buf[60];
      sprintf( page_id_buf, "page_id=%" PRId64, page_id );
      
      ms_client_arg_concat( volume_file_url, page_id_buf, !query_args );
      query_args = true;
   }
   
   if( least_unknown_generation >= 0 ) {
      
      char least_unknown_generation_buf[60];
      sprintf( least_unknown_generation_buf, "lug=%" PRId64, least_unknown_generation );
      
      ms_client_arg_concat( volume_file_url, least_unknown_generation_buf, !query_args );
      query_args = true;
   }
   
   return volume_file_url;
}

// FETCHXATTRS url 
// return the URL on success 
// return NULL on OOM
char* ms_client_fetchxattrs_url( char const* ms_url, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version, uint64_t file_id ) {
   
   char volume_id_str[50];
   char volume_version_str[50];
   char cert_version_str[50];
   
   sprintf( volume_id_str, "%" PRIu64, volume_id );
   sprintf( volume_version_str, "%" PRIu64, volume_version );
   sprintf( cert_version_str, "%" PRIu64, cert_version );

   char file_id_str[50];
   sprintf( file_id_str, "%" PRIX64, file_id );

   char* listxattr_path = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/FILE/FETCHXATTRS/") + 1 + strlen(volume_id_str) + 1 + strlen(volume_version_str) + 1 + strlen(cert_version_str) + 1 + strlen(file_id_str) + 1 );
   if( listxattr_path == NULL ) {
      return NULL;
   }
   
   sprintf( listxattr_path, "%s/FILE/FETCHXATTRS/%s.%s.%s/%s", ms_url, volume_id_str, volume_version_str, cert_version_str, file_id_str );
   
   return listxattr_path;
}

// URL to read a file's vacuum log
// return the URL on success 
// return NULL on OOM
char* ms_client_vacuum_url( char const* ms_url, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version, uint64_t file_id ) {
   
   char volume_id_str[50];
   char volume_version_str[50];
   char cert_version_str[50];
   
   sprintf( volume_id_str, "%" PRIu64, volume_id );
   sprintf( volume_version_str, "%" PRIu64, volume_version );
   sprintf( cert_version_str, "%" PRIu64, cert_version );

   char file_id_str[50];
   sprintf( file_id_str, "%" PRIX64, file_id );

   char* vacuum_path = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/FILE/VACUUM/") + 1 + strlen(volume_id_str) + 1 + strlen(volume_version_str) + 1 + strlen(cert_version_str) + 1 + strlen(file_id_str) + 1 );
   if( vacuum_path == NULL ) {
      return NULL;
   }
   
   sprintf( vacuum_path, "%s/FILE/VACUUM/%s.%s.%s/%s", ms_url, volume_id_str, volume_version_str, cert_version_str, file_id_str );
   
   return vacuum_path;
}
 

// URL to a Volume, by ID
// return the URL on success 
// return NULL on OOM
char* ms_client_volume_url( char const* ms_url, uint64_t volume_id ) {
   char buf[50];
   sprintf(buf, "%" PRIu64, volume_id );

   char* volume_md_path = md_fullpath( "/VOLUME/", buf, NULL );
   if( volume_md_path == NULL ) {
      return NULL;
   }

   char* url = md_fullpath( ms_url, volume_md_path, NULL );

   free( volume_md_path );

   return url;
}

// URL to a Volume, by name
// return the URL on success 
// return NULL on OOM
char* ms_client_volume_url_by_name( char const* ms_url, char const* name ) {
   char* volume_md_path = md_fullpath( "/VOLUME/", name, NULL );

   if( volume_md_path == NULL ) {
      return NULL;
   }
   
   char* url = md_fullpath( ms_url, volume_md_path, NULL );
   
   free( volume_md_path );

   return url;
}

/*
// URL to register with the MS, using a gateway keypair
// return the URL on success
// return NULL on OOM
char* ms_client_public_key_register_url( char const* ms_url ) {
   
   char* url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/REGISTER/") + 1 );
   if( url == NULL ) {
      return NULL;
   }
   
   sprintf( url, "%s/REGISTER", ms_url );
   
   return url;
}
*/

/*
// URL to register with the MS, using an OpenID username/password
// return the URL on success 
// return NULL on OOM
char* ms_client_openid_register_url( char const* ms_url, uint64_t gateway_type, char const* gateway_name, char const* username ) {
   // build the /REGISTER/ url

   char gateway_type_str[10];
   ms_client_gateway_type_str( gateway_type, gateway_type_str );

   char* url = SG_CALLOC( char, strlen(ms_url) + 1 +
                                strlen("/REGISTER/") + 1 +
                                strlen(gateway_name) + 1 +
                                strlen(username) + 1 +
                                strlen(gateway_type_str) + 1 +
                                strlen("/begin") + 1);

   if( url == NULL ) {
      return NULL;
   }
   
   sprintf(url, "%s/REGISTER/%s/%s/%s/begin", ms_url, gateway_type_str, gateway_name, username );

   return url;
}
*/

// URL to perform an RPC with the MS, using OpenID to authenticate
// return the URL on success
// return NULL on OOM
char* ms_client_openid_rpc_url( char const* ms_url ) {
   // build the /API/ url for OpenID
   
   char* url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/API/begin") + 1);

   if( url == NULL ) {
      return NULL;
   }
   
   sprintf(url, "%s/API/begin", ms_url );

   return url;
}

// URL to fetch the MS's public key
// return the URL on success
// return NULL on OOM
char* ms_client_syndicate_pubkey_url( char const* ms_url ) {
   // build the /PUBKEY url 
   
   char* url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/PUBKEY") + 1 );
   
   if( url == NULL ) {
      return NULL;
   }
   
   sprintf(url, "%s/PUBKEY", ms_url );
   
   return url;
}

// URL to a certificate manifest 
// if include_gateway_id is not SG_GATEWAY_ANON, then request its cert information as well.
// return the URL on success
// return NULL on OOM
char* ms_client_cert_manifest_url( char const* ms_url, uint64_t volume_id, uint64_t cert_version, uint64_t include_gateway_id ) {
   
   size_t gateway_id_len = 0;
   if( include_gateway_id > 0 ) {
      gateway_id_len = 70;
   }
   
   char* url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/CERT/") + 1 + 21 + 1 + strlen("manifest.") + 21 + 1 + gateway_id_len + 1 );
   if( url == NULL ) {
      return NULL;
   }
   
   if( include_gateway_id != SG_GATEWAY_ANON ) {
      sprintf(url, "%s/CERT/%" PRIu64 "/manifest.%" PRIu64 "?include_cert=%" PRIu64, ms_url, volume_id, cert_version, include_gateway_id );
   }
   else {
      sprintf(url, "%s/CERT/%" PRIu64 "/manifest.%" PRIu64, ms_url, volume_id, cert_version );
   }
   
   return url;
}


// get a certificate URL
// return the URL on success 
// return NULL on OOM
char* ms_client_cert_url( char const* ms_url, uint64_t volume_id, uint64_t cert_version, uint64_t gateway_type, uint64_t gateway_id, uint64_t gateway_cert_version ) {
   
   char* url = SG_CALLOC( char, strlen(ms_url) + 1 + strlen("/CERT/") + 1 + 21 + 1 + 21 + 1 + 21 + 1 + 21 + 1 );
   if( url == NULL ) {
      return NULL;
   }
   
   sprintf( url, "%s/CERT/%" PRIu64 "/%" PRIu64 "/%" PRIu64 "/%" PRIu64, ms_url, volume_id, cert_version, gateway_id, gateway_cert_version );
   
   return url;
}
