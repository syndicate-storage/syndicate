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

// load a file as a string.  return the buffer with the file on success, or NULL on error
char* md_load_file_as_string( char const* path, size_t* size ) {
   
   off_t size_or_error = 0;
   char* buf = md_load_file( path, &size_or_error );

   if( buf == NULL ) {
      SG_error("md_load_file('%s') rc = %d\n", path, (int)size_or_error );
      return NULL;
   }

   *size = size_or_error;
   
   char* ret = (char*)realloc( buf, *size + 1 );
   if( ret == NULL ) {
      SG_safe_free( buf );
      return NULL;
   }
   
   ret[ *size ] = 0;

   return ret;
}  


// safely load secret information as a null-terminated string, ensuring that the memory allocated is mlocked
// return 0 on success
// return negative errno on stat(2) failure on path
// return -ENODATA if we failed to allocate a buffer of sufficient size for the file referred to by the path 
// return -ENODATA if we failed to open path for reading, or failed to read all of the file
// return -EINVAL if the path does not refer to a regular file (or a symlink to a regular file)
// return -EOVERFLOW if the buf was allocated, but does not contain sufficient space
int md_load_secret_as_string( struct mlock_buf* buf, char const* path ) {
   
   struct stat statbuf;
   int rc = 0;
   
   rc = stat( path, &statbuf );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("stat(%s) errno = %d\n", path, rc );
      return rc;
   }
   
   if( !S_ISREG( statbuf.st_mode ) ) {
      
      return -EINVAL;
   }
   
   bool alloced = false;
   
   if( buf->ptr == NULL ) {
      
      rc = mlock_calloc( buf, statbuf.st_size + 1 );
      if( rc != 0 ) {
         
         SG_error("mlock_calloc rc = %d\n", rc );
         return -ENODATA;
      }
      
      alloced = true;
   }
   else if( buf->len <= (size_t)statbuf.st_size ) {
      
      SG_error("insufficient space for %s\n", path );
      return -EOVERFLOW;
   }
   
   FILE* f = fopen( path, "r" );
   if( f == NULL ) {
      rc = -errno;
      
      if( alloced ) {
         mlock_free( buf );
      }
      
      SG_error("fopen(%s) rc = %d\n", path, rc );
      return -ENODATA;
   }
   
   buf->len = fread( buf->ptr, 1, statbuf.st_size, f );
   fclose( f );
   
   if( buf->len != (unsigned)statbuf.st_size ) {
      
      SG_error("Read only %zu of %zu bytes\n", buf->len, statbuf.st_size );
      
      if( alloced ) {
         mlock_free( buf );
      }
      
      return -ENODATA;
   }

   // null-terminate
   char* char_ptr = (char*)buf->ptr;
   char_ptr[ buf->len ] = 0;

   return 0;
}  


// initialize local storage
// return 0 on success
// return -ENOMEM if OOM
// return negative on storage-related error (md_mkdirs)
int md_init_local_storage( struct md_syndicate_conf* c ) {
   
   char cwd[PATH_MAX + 1];
   memset(cwd, 0, PATH_MAX + 1 );
   
   int rc = 0;
   
   char* storage_root = c->storage_root;
   char* data_root = c->data_root;
   char* logfile_path = c->logfile_path;
   
   bool alloc_storage_root = false;
   bool alloc_data_root = false;
   bool alloc_logfile_path = false;
   
   if( storage_root == NULL ) {
      // make a PID-named directory
      pid_t my_pid = 0;
      
      my_pid = getpid();
      
      sprintf(cwd, "/tmp/syndicate-%d", my_pid );
      
      storage_root = SG_strdup_or_null(cwd);
      alloc_storage_root = true;
   }
   else {
      
      if( strlen(storage_root) >= PATH_MAX - 20 ) {          // - 20 for any schema prefixes we'll apply; shouldn't be a problem otherwise
         SG_error("Directory '%s' too long\n", storage_root );
         return -EINVAL;
      }
      
      strncpy( cwd, storage_root, PATH_MAX );
   }
   
   if( data_root == NULL ) {
      data_root = md_fullpath( cwd, "data/", NULL );
      alloc_data_root = true;
   }
   
   if( logfile_path == NULL ) {
      logfile_path = md_fullpath( cwd, "access.log", NULL );
      alloc_logfile_path = true;
   }
   
   // check for allocation errors
   if( (alloc_data_root && data_root == NULL) ||
       (alloc_storage_root && storage_root == NULL ) || 
       (alloc_logfile_path && logfile_path == NULL) ) {
      
      if( alloc_data_root ) {
         SG_safe_free( data_root );
      }
      
      if( alloc_storage_root ) {
         SG_safe_free( storage_root );
      }
      
      if( alloc_logfile_path ) {
         SG_safe_free( logfile_path );
      }
      
      return -ENOMEM;
   }
   
   SG_debug("data root:     %s\n", data_root );
   SG_debug("access log:    %s\n", logfile_path );
   
   // try to set up directories 
   rc = md_mkdirs( data_root );
   if( rc != 0 ) {
      
      SG_error("md_mkdirs('%s') rc = %d\n", data_root, rc );
      
      // clean up 
      
      if( alloc_data_root ) {
         SG_safe_free( data_root );
      }
      
      if( alloc_storage_root ) {
         SG_safe_free( storage_root );
      }
      
      if( alloc_logfile_path ) {
         SG_safe_free( logfile_path );
      }
      
      return rc;
   }
   
   // success!
   c->data_root = data_root;
   c->storage_root = storage_root;
   c->logfile_path = logfile_path;

   return rc;
}

// recursively make a directory.
// return 0 if the directory exists at the end of the call.
// return -ENOMEM if OOM
// return negative if the directory could not be created.
int md_mkdirs2( char const* dirp, int start, mode_t mode ) {
   
   unsigned int i = start;
   struct stat statbuf;
   int rc = 0;
   char* currdir = SG_CALLOC( char, strlen(dirp) + 1 );
   
   if( currdir == NULL ) {
      return -ENOMEM;
   }
   
   while( i <= strlen(dirp) ) {
      
      if( dirp[i] == '/' || i == strlen(dirp) ) {
         
         strncpy( currdir, dirp, i == 0 ? 1 : i );
         
         rc = stat( currdir, &statbuf );
         if( rc == 0 && !S_ISDIR( statbuf.st_mode ) ) {
            
            SG_safe_free( currdir );
            return -EEXIST;
         }
         if( rc != 0 ) {
            
            rc = mkdir( currdir, mode );
            if( rc != 0 ) {
               
               rc = -errno;
               SG_safe_free(currdir);
               return rc;
            }
         }
      }
      
      i++;
   }
   
   SG_safe_free(currdir);
   return 0;
}

int md_mkdirs3( char const* dirp, mode_t mode ) {
   return md_mkdirs2( dirp, 0, mode );
}

int md_mkdirs( char const* dirp ) {
   return md_mkdirs2( dirp, 0, 0755 );
}

// remove a bunch of empty directories
// return 0 on success 
// return -ENOMEM on OOM
// return negative on error from rmdir(2)
int md_rmdirs( char const* dirp ) {
   
   char* dirname = SG_strdup_or_null( dirp );
   if( dirname == NULL ) {
      return -ENOMEM;
   }
   
   char* dirname_buf = SG_CALLOC( char, strlen(dirp) + 1 );
   if( dirname_buf == NULL ) {
      
      SG_safe_free( dirname );
      return -ENOMEM;
   }
   
   int rc = 0;
   
   while( strlen(dirname) > 0 ) {
      
      rc = rmdir( dirname );
      if( rc != 0 ) {
         
         rc = -errno;
         break;
      }
      else {
         
         md_dirname( dirname, dirname_buf );
         strcpy( dirname, dirname_buf );
      }
   }
   
   SG_safe_free( dirname );
   SG_safe_free( dirname_buf );
   return rc;
}
