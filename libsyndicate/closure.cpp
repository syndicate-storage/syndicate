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

#include "closure.h"

// write the MS-supplied closure to a temporary file
// return the path to it on success
int md_write_closure( struct md_syndicate_conf* conf, char** _so_path_ret, char const* closure_text, size_t closure_text_len ) {
   char* so_path = md_fullpath( conf->data_root, MD_CLOSURE_TMPFILE_NAME, NULL );
   int rc = 0;
   
   int fd = mkstemp( so_path );
   if( fd < 0 ) {
      int rc = -errno;
      errorf("mkstemp(%s) rc = %d\n", so_path, rc );
      free( so_path );
      return rc;
   }
   
   // write it out
   off_t nw = 0;
   off_t w_off = 0;
   
   while( nw < (signed)closure_text_len ) {
      ssize_t w = write( fd, closure_text + w_off, closure_text_len - w_off );
      if( w < 0 ) {
         int errsv = -errno;
         errorf("write(%d) rc = %d\n", fd, errsv);
         nw = errsv;
         break;
      }
      
      nw += w;
   }
   
   close( fd );
   
   if( nw < 0 ) {
      // failure 
      unlink( so_path );
      free( so_path );
      rc = nw;
   }
   else {
      *_so_path_ret = so_path;
   }
   
   return rc;
}



// read and link MS-supplied closure from the temporary file we created
int md_load_closure( struct md_closure* closure, char* so_path, struct md_closure_callback_entry* closure_symtable ) {
   closure->so_handle = dlopen( so_path, RTLD_LAZY );
   if ( closure->so_handle == NULL ) {
      errorf( "dlopen error = %s\n", dlerror() );
      return -ENODATA;
   }
   
   // load each symbol into its respective address
   for( int i = 0; closure_symtable[i].sym_name != NULL; i++ ) {
      *(void **)(&closure_symtable[i].sym_ptr) = dlsym( closure, closure_symtable[i].sym_name );
      
      if( closure_symtable[i].sym_ptr == NULL ) {
         errorf("WARN: dlsym(%s) error = %s\n", closure_symtable[i].sym_name, dlerror());
      }
   }
   
   return 0;
}


// load or reload a binary closure, from a base64-encoded string
int md_install_binary_closure( struct md_syndicate_conf* conf, struct md_closure** closure, struct md_closure_callback_entry* prototype, char const* closure_text_b64, size_t closure_text_len_b64 ) {
   // decode...
   int rc = 0;
   size_t closure_text_len = 0;
   char* closure_text = NULL;
   rc = Base64Decode( closure_text_b64, closure_text_len_b64, &closure_text, &closure_text_len );
   
   if( rc != 0 ) {
      errorf("failed to decode closure text, rc = %d\n", rc );
   }
   
   else {
      char const* method = NULL;
      
      if( *closure ) {
         rc = md_closure_reload( conf, *closure, closure_text, closure_text_len );
         method = "md_closure_reload";
      }
      else {
         *closure = CALLOC_LIST( struct md_closure, 1 );
         
         rc = md_closure_init( conf, *closure, prototype, closure_text, closure_text_len );
         method = "md_closure_init";
      }
      
      free( closure_text );
      
      if( rc != 0 ) {
         errorf("%s rc = %d\n", method, rc );
      }
   }
   
   return rc;
}


// duplicate a callback table 
static struct md_closure_callback_entry* md_closure_callback_table_dup( struct md_closure_callback_entry* callbacks ) {
   // count them...
   int num_cbs = 0;
   for( int i = 0; callbacks[i].sym_name != NULL; i++ ) {
      num_cbs++;
   }
   
   struct md_closure_callback_entry* ret = CALLOC_LIST( struct md_closure_callback_entry, num_cbs + 1 );
   
   for( int i = 0; callbacks[i].sym_name != NULL; i++ ) {
      ret[i].sym_name = strdup( callbacks[i].sym_name );
      ret[i].sym_ptr = callbacks[i].sym_ptr;
   }
   
   return ret;
}


// free a callback table 
static void md_closure_callback_table_free( struct md_closure_callback_entry* callbacks ) {
   for( int i = 0; callbacks[i].sym_name != NULL; i++ ) {
      if( callbacks[i].sym_name ) {
         free( callbacks[i].sym_name );
         callbacks[i].sym_name = NULL;
      }
   }
}


int md_closure_rlock( struct md_closure* closure ) {
   return pthread_rwlock_rdlock( &closure->reload_lock );
}


int md_closure_wlock( struct md_closure* closure ) {
   return pthread_rwlock_wrlock( &closure->reload_lock );
}


int md_closure_unlock( struct md_closure* closure ) {
   return pthread_rwlock_unlock( &closure->reload_lock );
}


int md_closure_init( struct md_syndicate_conf* conf, struct md_closure* closure, struct md_closure_callback_entry* callbacks_prototype, char const* closure_text, size_t closure_text_len ) {
   // closure code was loaded from the MS.  It should be a .so binary for this architecture.
   // load it up!
   memset( closure, 0, sizeof(struct md_closure) );
   
   int rc = 0;
   char* so_path = NULL;
   
   struct md_closure_callback_entry* callbacks = md_closure_callback_table_dup( callbacks_prototype );
   
   rc = md_write_closure( conf, &so_path, closure_text, closure_text_len );
   if( rc != 0 ) {
      if( rc != -ENOENT ) {
         errorf("md_write_closure rc = %d\n", rc);
         return rc;
      }
   }
   
   if( rc == 0 ) {
      // got a closure.  Try to load it.
      rc = md_load_closure( closure, so_path, callbacks );
      if( rc != 0 ) {
         errorf("md_load_closure(%s) rc = %d\n", so_path, rc );
         unlink( so_path );
         free( so_path );
         md_closure_callback_table_free( callbacks );
         free( callbacks );
      }
      else {
         // initialize the closure cls, if we have an init method given
         if( closure->init ) {
            int closure_init_rc = (*closure->init)( conf, &closure->cls );
            if( closure_init_rc != 0 ) {
               errorf("closure->init() rc = %d\n", closure_init_rc );
               rc = closure_init_rc;
               
               // clean up
               unlink( so_path );
               free( so_path );
               dlclose( closure->so_handle );
               md_closure_callback_table_free( callbacks );
               free( callbacks );
            }
         }
      }
   }
   else if( rc == -ENOENT ) {
      // no closure given; this shouldn't stop us from initializing the rest of the closure structure
      rc = 0;
   }

   if( rc == 0 ) {
      // save these pointers...
      closure->callbacks = callbacks;
      
      // initialize the rest of the closure 
      pthread_rwlock_init( &closure->reload_lock, NULL );
      closure->so_path = so_path;
      closure->running = 1;
   }
   
   else {
      rc = -ENODATA;
   }
   
   return rc;
}


// reload the given closure.  Shut it down, get the new code, and start it back up again
int md_closure_reload( struct md_syndicate_conf* conf, struct md_closure* closure, char const* closure_text, size_t closure_text_len ) {
   int rc = 0;
   md_closure_wlock( closure );
   
   if( closure->shutdown ) {
      int closure_shutdown_rc = (*closure->shutdown)( closure->cls );
      if( closure_shutdown_rc != 0 ) {
         errorf("WARN: closure->shutdown rc = %d\n", closure_shutdown_rc );
      }
   }
   
   struct md_closure new_closure;
   
   memset( &new_closure, 0, sizeof(struct md_closure) );
   
   char* new_so_path = NULL;
   rc = md_write_closure( conf, &new_so_path, closure_text, closure_text_len );
   if( rc != 0 && rc != -ENOENT ) {
      errorf("Failed to save closure, rc = %d\n", rc);
      md_closure_unlock( closure );
      return -ENODATA;
   }
   
   if( rc == 0 ) {
      // there's closure code to be had...
      rc = md_load_closure( &new_closure, new_so_path, closure->callbacks );
      if( rc != 0 ) {
         errorf("closure_load(%s) rc = %d\n", new_so_path, rc );
         unlink( new_so_path );
         free( new_so_path );
      }
      else {
         // success!
         // copy over the new callbacks
         closure->init = new_closure.init;
         closure->shutdown = new_closure.shutdown;
         
         // copy over the dynamic link handle
         dlclose( closure->so_handle );
         closure->so_handle = new_closure.so_handle;
         
         // clean up cached closure code
         unlink( closure->so_path );
         free( closure->so_path );
         closure->so_path = new_so_path;
      }
   }
   else if( rc == -ENOENT ) {
      // no closure found on reload.
      closure->init = NULL;
      closure->shutdown = NULL;
      
      md_closure_callback_table_free( closure->callbacks );
      free( closure->callbacks );
      closure->callbacks = NULL;
      
      if( closure->so_handle ) {
         dlclose( closure->so_handle );
         closure->so_handle = NULL;
      }
      
      if( closure->so_path ) {
         unlink( closure->so_path );
         free( closure->so_path );
         closure->so_path = NULL;
      }
   }
   
   md_closure_unlock( closure );
   return rc;
}


int md_closure_shutdown( struct md_closure* closure ) {
   // call the closure shutdown...
   int rc = 0;
   
   md_closure_wlock( closure );
   
   closure->running = 0;
   
   if( closure->shutdown ) {
      int closure_shutdown_rc = (*closure->shutdown)( closure );
      if( closure_shutdown_rc != 0 ) {
         errorf("WARN: closure->shutdown rc = %d\n", closure_shutdown_rc );
      }
   }
   
   if( closure->so_path ) {
      unlink( closure->so_path );
      free( closure->so_path );
      closure->so_path = NULL;
   }
   
   if( closure->so_handle ) {
      dlclose( closure->so_handle );
      closure->so_handle = NULL;
   }
   
   if( closure->callbacks ) {
      md_closure_callback_table_free( closure->callbacks );
      free( closure->callbacks );
      closure->callbacks = NULL;
   }
   
   md_closure_unlock( closure );
   pthread_rwlock_destroy( &closure->reload_lock );
 
   return rc;
}

// look up a callback 
void* md_closure_find_callback( struct md_closure* closure, char const* cb_name ) {
   if( closure == NULL )
      return NULL;
   
   if( closure->running == 0 )
      return NULL;
   
   if( closure->callbacks == NULL )
      return NULL;
   
   md_closure_rlock( closure );
   
   void* ret = NULL;
   
   for( int i = 0; closure->callbacks[i].sym_name != NULL; i++ ) {
      if( strcmp( cb_name, closure->callbacks[i].sym_name ) == 0 ) {
         ret = closure->callbacks[i].sym_ptr;
      }
   }
   
   md_closure_unlock( closure );
   
   return ret;
}

// compile test....
int md_closure_test( struct md_closure* closure, CURL* curl, void* cls ) {
   md_closure_rlock( closure );
   
   int ret = 0;
   
   MD_CLOSURE_CALL( ret, closure, "connect_cache", int (*)(CURL*, void*), curl, cls );
   
   md_closure_unlock( closure );
   
   return ret;
}

