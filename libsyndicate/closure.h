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

#ifndef _MD_CLOSURE_H_
#define _MD_CLOSURE_H_

#include "libsyndicate.h"

#include <dlfcn.h>
#include "libjson-compat.h"

#define MD_CLOSURE_TMPFILE_NAME ".syndicate-closure-XXXXXX"

// closure callback table...
struct md_closure_callback_entry {
   char* sym_name;
   void* sym_ptr;
};

#define MD_CLOSURE_CALLBACK( sym_name ) {(char*)sym_name, NULL}

typedef map<string, string> md_closure_conf_t;
typedef md_closure_conf_t md_closure_secrets_t;

struct md_closure {
   void* so_handle;     // dynamic driver library handle
   char* so_path;       // path to the driver so file
   
   md_closure_conf_t* closure_conf;     // closure config params
   md_closure_secrets_t* closure_secrets;       // closure secrets
   
   char* spec;          // (AG only) path <--> command translation 
   size_t spec_len;     // (AG only) len(spec)
   
   void* cls;           // supplied by the closure on initialization
   int running;         // set to non-zero of this driver is initialized
   
   pthread_rwlock_t reload_lock;                // if write-locked, no method can be called here (i.e. the closure is reloading)
   
   struct md_closure_callback_entry* callbacks;
   
   bool ignore_stubs;           // if true, then we will reload a closure even if it doesn't have some methods we need
   bool on_disk;                // if true, then this closure was loaded from an already-existing file on disk.  Don't unlink it.
};

typedef int (*md_closure_init_func)( struct md_closure*, void** );
typedef int (*md_closure_shutdown_func)( void* );

// driver loading and processing
int md_write_driver( struct md_syndicate_conf* conf, char** _so_path_ret, char const* driver_text, size_t driver_text_len );
int md_load_driver( struct md_closure* closure, char const* so_path, struct md_closure_callback_entry* closure_symtable );

// locking...
int md_closure_rlock( struct md_closure* closure );
int md_closure_wlock( struct md_closure* closure );
int md_closure_unlock( struct md_closure* closure );

// initialization, reload, and shutdown 
int md_closure_init( struct md_closure* closure,
                     struct md_syndicate_conf* conf,
                     EVP_PKEY* pubkey, EVP_PKEY* privkey,
                     struct md_closure_callback_entry* prototype,
                     char const* closure_text, size_t closure_text_len,
                     bool ignore_stubs );

int md_closure_init_bin( struct md_syndicate_conf* conf, struct md_closure* closure, char const* so_path, struct md_closure_callback_entry* driver_prototype, bool ignore_stubs );
int md_closure_reload( struct md_closure* closure, struct md_syndicate_conf* conf, EVP_PKEY* pubkey, EVP_PKEY* privkey, char const* closure_text, size_t closure_text_len );
int md_closure_shutdown( struct md_closure* closure );

// AG-specific 
int md_closure_load_AG_specfile( char* specfile_json_str, size_t specfile_json_str_len, char** specfile_text, size_t* specfile_text_len );

// closure config API 
int md_closure_get_config( struct md_closure* closure, char const* key, char** value, size_t* len );
int md_closure_get_secret( struct md_closure* closure, char const* key, char** value, size_t* len );

// driver management and querying 
int md_closure_driver_reload( struct md_syndicate_conf* conf, struct md_closure* closure, char const* driver_text, size_t driver_text_len );
void* md_closure_find_callback( struct md_closure* closure, char const* cb_name );


#define MD_CLOSURE_CALL( ret, closure, symname, signature, ... ) \
   do {         \
      if( (closure) == NULL ) { \
         break; \
      } \
      pthread_rwlock_rdlock( &(closure)->reload_lock ); \
      \
      if( (closure)->running == 0 ) {     \
         pthread_rwlock_unlock( &(closure)->reload_lock ); \
         break; \
      } \
      void* sym = md_closure_find_callback( (closure), (symname) );      \
      if( sym != NULL ) {       \
         ret = (reinterpret_cast<signature>(sym))( __VA_ARGS__ );        \
      } \
      pthread_rwlock_unlock( &(closure)->reload_lock ); \
   } while( 0 ); 
   
   
#define MD_CLOSURE_PROTOTYPE_BEGIN( prototype_name ) \
   struct md_closure_callback_entry prototype_name[] = { \
      MD_CLOSURE_CALLBACK( "closure_init"),     \
      MD_CLOSURE_CALLBACK( "closure_shutdown"),

#define MD_CLOSURE_PROTOTYPE_END \
      , {NULL, NULL} \
};
      
#endif