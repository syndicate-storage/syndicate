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

#include "driver.h"

// write the MS-supplied closure to a temporary file
// return the path to it on success
static char* driver_write_closure( struct fs_core* core, int* _rc ) {
   char* so_path = md_fullpath( core->conf->data_root, DRIVER_TMPFILE_NAME, NULL );
   
   int fd = mkstemp( so_path );
   if( fd < 0 ) {
      int rc = -errno;
      errorf("mkstemp(%s) rc = %d\n", so_path, rc );
      free( so_path );
      
      *_rc = rc;
      return NULL;
   }
   
   // get the driver from the MS
   char* closure_text = NULL;
   uint64_t closure_text_len = 0;
   
   int rc = ms_client_get_closure_text( core->ms, &closure_text, &closure_text_len );
   if( rc != 0 ) {
      if( rc == -ENOENT ) {
         errorf("No driver given (rc = %d)\n", rc );
      }
      else {
         errorf("ms_client_get_closure_text rc = %d\n", rc );
      }
      unlink( so_path );
      free( so_path );
      close( fd );
      
      *_rc = rc;
      return NULL;
   }
   
   // write it out
   uint64_t nw = 0;
   off_t w_off = 0;
   
   while( nw < closure_text_len ) {
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
      so_path = NULL;
      
      *_rc = nw;
   }
      
   return so_path;
}



// read and link MS-supplied closure from the temporary file we created
static int driver_load( struct fs_core* core, struct storage_driver* driver, char* so_path ) {
   driver->driver = dlopen( so_path, RTLD_LAZY );
   if ( driver->driver == NULL ) {
      errorf( "driver_load error = %s\n", dlerror() );
      return -ENODATA;
   }
   
   // driver symbol table...
   struct driver_symtable_entry {
      char const* sym_name;
      void* sym_ptr;
   };
   
   struct driver_symtable_entry driver_symtable[] = {
      {"init",                  (void*)(driver->init)                   },
      {"shutdown",              (void*)(driver->shutdown)               },
      {"connect_cache",         (void*)(driver->connect_cache)          },
      {"write_preup",           (void*)(driver->write_preup)            },
      {"read_postdown",         (void*)(driver->read_postdown)          },
      {"chcoord_begin",         (void*)(driver->chcoord_begin)          },
      {"chcoord_end",           (void*)(driver->chcoord_end)            },
      {NULL,                    NULL                                    }
   };
   
   // load each symbol into its respective address
   for( int i = 0; driver_symtable[i].sym_name != NULL; i++ ) {
      *(void **)(&driver_symtable[i].sym_ptr) = dlsym( driver, driver_symtable[i].sym_name );
      
      if( driver_symtable[i].sym_ptr == NULL ) {
         errorf("WARN: driver_load(%s) error = %s\n", driver_symtable[i].sym_name, dlerror());
      }
   }
   
   return 0;
}


int driver_init( struct fs_core* core, struct storage_driver* driver ) {
   // driver code was loaded from the MS.  It should be a .so binary for this architecture.
   // load it up!
   memset( driver, 0, sizeof(struct storage_driver) );
   
   int rc = 0;
   
   char* so_path = driver_write_closure( core, &rc );
   if( so_path == NULL ) {
      if( rc != -ENOENT ) {
         errorf("driver_write_closure rc = %d\n", rc);
         return rc;
      }
   }
   
   if( rc == 0 ) {
      // got a driver.  Try to load it.
      rc = driver_load( core, driver, so_path );
      if( rc != 0 ) {
         errorf("driver_load(%s) rc = %d\n", so_path, rc );
         unlink( so_path );
         free( so_path );
      }
      else {      
         
         // initialize the driver cls, if we have an init method given
         if( driver->init ) {
            int driver_init_rc = (*driver->init)( core, &driver->cls );
            if( driver_init_rc != 0 ) {
               errorf("driver->init() rc = %d\n", driver_init_rc );
               rc = driver_init_rc;
               
               // clean up
               unlink( so_path );
               free( so_path );
               dlclose( driver->driver );
            }
         }
      }
   }
   else if( rc == -ENOENT ) {
      // no driver given; this shouldn't stop us from initializing the rest of the driver structure
      rc = 0;
   }

   if( rc == 0 ) {
      // initialize the rest of the driver 
      pthread_rwlock_init( &driver->reload_lock, NULL );
      driver->so_path = so_path;
      driver->running = 1;
   }
   
   else {
      rc = -ENODATA;
   }
   
   return rc;
}


// reload the given driver.  Shut it down, get the new code, and start it back up again
int driver_reload( struct fs_core* core, struct storage_driver* driver ) {
   int rc = 0;
   pthread_rwlock_wrlock( &driver->reload_lock );
   
   if( driver->shutdown ) {
      int driver_shutdown_rc = (*driver_shutdown)( core, driver );
      if( driver_shutdown_rc != 0 ) {
         errorf("WARN: driver->shutdown rc = %d\n", driver_shutdown_rc );
      }
   }
   
   struct storage_driver new_driver;
   
   memset( &new_driver, 0, sizeof(struct storage_driver) );
   
   char* new_so_path = driver_write_closure( core, &rc );
   if( new_so_path == NULL && rc != -ENOENT ) {
      errorf("Failed to save closure, rc = %d\n", rc);
      pthread_rwlock_unlock( &driver->reload_lock );
      return -ENODATA;
   }
   
   if( rc == 0 ) {
      // there's driver code to be had...
      rc = driver_load( core, &new_driver, new_so_path );
      if( rc != 0 ) {
         errorf("driver_load(%s) rc = %d\n", new_so_path, rc );
         unlink( new_so_path );
         free( new_so_path );
      }
      else {
         // success!
         // copy over the new callbacks
         driver->init = new_driver.init;
         driver->shutdown = new_driver.shutdown;
         driver->connect_cache = new_driver.connect_cache;
         driver->write_preup = new_driver.write_preup;
         driver->read_postdown = new_driver.read_postdown;
         driver->chcoord_begin = new_driver.chcoord_begin;
         driver->chcoord_end = new_driver.chcoord_end;
         
         // copy over the dynamic link handle
         dlclose( driver->driver );
         driver->driver = new_driver.driver;
         
         // clean up cached driver code
         unlink( driver->so_path );
         free( driver->so_path );
         driver->so_path = new_so_path;
      }
   }
   else if( rc == -ENOENT ) {
      // no driver found on reload.
      driver->init = NULL;
      driver->shutdown = NULL;
      driver->connect_cache = NULL;
      driver->write_preup = NULL;
      driver->read_postdown = NULL;
      driver->chcoord_begin = NULL;
      driver->chcoord_end = NULL;
      
      if( driver->driver ) {
         dlclose( driver->driver );
         driver->driver = NULL;
      }
      
      if( driver->so_path ) {
         unlink( driver->so_path );
         free( driver->so_path );
         driver->so_path = NULL;
      }
   }
   
   pthread_rwlock_unlock( &driver->reload_lock );
   return rc;
}


int driver_shutdown( struct fs_core* core, struct storage_driver* driver ) {
   // call the driver shutdown...
   int rc = 0;
   
   pthread_rwlock_wrlock( &driver->reload_lock );
   
   driver->running = 0;
   
   if( driver->shutdown ) {
      int driver_shutdown_rc = (*driver_shutdown)( core, driver );
      if( driver_shutdown_rc != 0 ) {
         errorf("WARN: driver->shutdown rc = %d\n", driver_shutdown_rc );
      }
   }
   
   if( driver->so_path ) {
      unlink( driver->so_path );
      free( driver->so_path );
      driver->so_path = NULL;
   }
   
   if( driver->driver ) {
      dlclose( driver->driver );
      driver->driver = NULL;
   }
   
   pthread_rwlock_unlock( &driver->reload_lock );
   pthread_rwlock_destroy( &driver->reload_lock );
 
   return rc;
}

// connect to the caches
char* driver_connect_cache( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent, char const* fs_path, uint64_t block_id, uint64_t block_version ) {
   pthread_rwlock_rdlock( &driver->reload_lock );
   
   char* ret = NULL;
   
   if( driver->connect_cache ) {
      ret = (*driver->connect_cache)( core, driver->cls, fent, fs_path, block_id, block_version );
   }
   
   pthread_rwlock_unlock( &driver->reload_lock );
   return ret;
}


// process data before uploading 
int driver_write_preup( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* block_data, size_t block_data_len ) {
   pthread_rwlock_rdlock( &driver->reload_lock );
   
   int ret = 0;
   if( driver->write_preup ) {
      ret = (*driver->write_preup)( core, driver->cls, fent, block_id, block_version, block_data, block_data_len );
   }
   
   pthread_rwlock_unlock( &driver->reload_lock );
   return ret;
}


// process data after downloading 
int driver_read_postdown( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* block_data, size_t block_data_len ) {
   pthread_rwlock_rdlock( &driver->reload_lock );
   
   int ret = 0;
   if( driver->read_postdown ) {
      ret = (*driver->read_postdown)( core, driver->cls, fent, block_id, block_version, block_data, block_data_len );
   }
   
   pthread_rwlock_unlock( &driver->reload_lock );
   return ret;
}


// begin changing coordinator.  This is called *before* the coordinator change request is sent.  fent->coordinator still refers to the old coordinator
int driver_chcoord_begin( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent ) {
   pthread_rwlock_rdlock( &driver->reload_lock );
   
   int ret = 0;
   if( driver->read_postdown ) {
      ret = (*driver->chcoord_begin)( core, driver->cls, fent );
   }
   
   pthread_rwlock_unlock( &driver->reload_lock );
   return ret;
}

// end changing coordinator.  This is called *after* the coordintaor changes.
// chcoord_status is the MS's return code (0 for success, negative for error). 
// If chcoord_status == 0, fent->coordinator refers to the new coordinator
int driver_chcoord_end( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent, int chcoord_status ) {
   pthread_rwlock_rdlock( &driver->reload_lock );
   
   int ret = 0;
   if( driver->read_postdown ) {
      ret = (*driver->chcoord_end)( core, driver->cls, fent, chcoord_status );
   }
   
   pthread_rwlock_unlock( &driver->reload_lock );
   return ret;
}

