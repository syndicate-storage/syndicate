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