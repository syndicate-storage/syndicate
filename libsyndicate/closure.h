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

#define MD_CLOSURE_TMPFILE_NAME "closure-XXXXXX"

// closure callback table...
struct md_closure_callback_entry {
   char* sym_name;
   void* sym_ptr;
};

#define MD_CLOSURE_CALLBACK( sym_name ) {(char*)sym_name, NULL}

struct md_closure {
   void* so_handle;     // dynamic driver library handle
   char* so_path;       // path to the driver so file
   
   void* cls;           // supplied by the closure
   int running;         // set to non-zero of this driver is initialized
   
   pthread_rwlock_t reload_lock;                // if write-locked, no method can be called here (i.e. the closure is reloading)
   
   int (*init)( struct md_syndicate_conf*, void** );
   int (*shutdown)( void* );
   
   struct md_closure_callback_entry* callbacks;
};

// closure loading and processing
int md_write_closure( struct md_syndicate_conf* conf, char** so_path, char const* closure_text, size_t closure_text_len );
int md_load_closure( struct md_closure* closure, char* so_path, struct md_closure_callback_entry* callbacks );
int md_install_binary_closure( struct md_syndicate_conf* conf, struct md_closure** closure, struct md_closure_callback_entry* prototype, char const* closure_text_b64, size_t closure_text_len_b64 );

// locking...
int md_closure_rlock( struct md_closure* closure );
int md_closure_wlock( struct md_closure* closure );
int md_closure_unlock( struct md_closure* closure );

// driver interface 
int md_closure_init( struct md_syndicate_conf* conf, struct md_closure* closure, struct md_closure_callback_entry* callbacks, char const* closure_text, size_t closure_text_len );
int md_closure_reload( struct md_syndicate_conf* conf, struct md_closure* closure, char const* closure_text, size_t closure_text_len );
int md_closure_shutdown( struct md_closure* closure );

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
   
      
#endif