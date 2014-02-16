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

#include "libsyndicate/storage.h"

char* md_load_file_as_string( char const* path, size_t* size ) {
   char* ret = load_file( path, size );

   if( ret == NULL ) {
      errorf("failed to load %s\n", path );
      return NULL;
   }

   ret = (char*)realloc( ret, *size + 1 );

   ret[ *size ] = 0;

   return ret;
}  


// initialize local storage
int md_init_local_storage( struct md_syndicate_conf* c ) {
   
   char cwd[PATH_MAX + 1];
   int rc = 0;
   
   if( c->storage_root == NULL ) {
      // make a PID-named directory
      pid_t my_pid = 0;
      
   #ifndef _SYNDICATE_NACL_
      my_pid = getpid();
   #else
      my_pid = (pid_t)rand();
   #endif
      
      sprintf(cwd, "/tmp/syndicate.%d", my_pid );
   }
   else {
      if( strlen(c->storage_root) >= PATH_MAX - 20 ) {
         errorf("Directory '%s' too long\n", c->storage_root );
         return -EINVAL;
      }
      
      strncpy( cwd, c->storage_root, PATH_MAX );
   }
   
   char* data_root = md_fullpath( cwd, "data/", NULL );
   char* logfile_path = md_fullpath( cwd, "access.log", NULL );
   char* replica_logfile_path = md_fullpath( cwd, "replica.log", NULL );
   
   if( c->data_root == NULL )
      c->data_root = strdup( data_root );

   if( c->logfile_path == NULL )
      c->logfile_path = strdup( logfile_path );

   if( c->replica_logfile == NULL )
      c->replica_logfile = strdup( replica_logfile_path );

   free( data_root );
   free( logfile_path );
   free( replica_logfile_path );

   if( c->gateway_type == SYNDICATE_UG ) {
      dbprintf("data root:     %s\n", c->data_root );
      dbprintf("replica log:   %s\n", c->replica_logfile );
   }

   dbprintf("access log:    %s\n", c->logfile_path );

   // make sure the storage roots exist
   if( rc == 0 ) {
      const char* dirs[] = {
         c->data_root,
         NULL
      };

#ifndef _SYNDICATE_NACL_
      for( int i = 0; dirs[i] != NULL; i++ ) {         
         rc = md_mkdirs( dirs[i] );
         if( rc != 0 ) {
            errorf("md_mkdirs(%s) rc = %d\n", dirs[i], rc );
            return rc;
         }
      }
#endif
   }

   return rc;
}

// recursively make a directory.
// return 0 if the directory exists at the end of the call.
// return negative if the directory could not be created.
int md_mkdirs2( char const* dirp, int start, mode_t mode ) {
   char* currdir = (char*)calloc( strlen(dirp) + 1, 1 );
   unsigned int i = start;
   while( i <= strlen(dirp) ) {
      if( dirp[i] == '/' || i == strlen(dirp) ) {
         strncpy( currdir, dirp, i == 0 ? 1 : i );
         struct stat statbuf;
         int rc = stat( currdir, &statbuf );
         if( rc == 0 && !S_ISDIR( statbuf.st_mode ) ) {
            free( currdir );
            return -EEXIST;
         }
         if( rc != 0 ) {
            rc = mkdir( currdir, mode );
            if( rc != 0 ) {
               free(currdir);
               return -errno;
            }
         }
      }
      i++;
   }
   free(currdir);
   return 0;
}

int md_mkdirs3( char const* dirp, mode_t mode ) {
   return md_mkdirs2( dirp, 0, mode );
}

int md_mkdirs( char const* dirp ) {
   return md_mkdirs2( dirp, 0, 0755 );
}

// remove a bunch of empty directories
int md_rmdirs( char const* dirp ) {
   char* dirname = strdup( dirp );
   int rc = 0;
   while( strlen(dirname) > 0 ) {
      rc = rmdir( dirname );
      if( rc != 0 ) {
         break;
      }
      else {
         char* tmp = md_dirname( dirname, NULL );
         free( dirname );
         dirname = tmp;
      }
   }
   free( dirname );
   return rc;
}