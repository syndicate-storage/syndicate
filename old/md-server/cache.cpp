/*
   Copyright 2012 Jude Nelson

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

#include "cache.h"

md_cache* Cache = NULL;

void cache_init() {
   Cache = new md_cache( 5000 );
}

void cache_shutdown() {
   delete Cache;
   Cache = NULL;
}

md_cache::md_cache( uint64_t max_size ) {
   pthread_rwlock_init( &this->lock, NULL );
   this->max_size = max_size;
   this->size = 0;
}

md_cache::~md_cache() {
   pthread_rwlock_rdlock( &this->lock );

   for( cache_map::iterator itr = this->data.begin(); itr != this->data.end(); itr++ ) {
      free( itr->second.data );
      itr->second.data = NULL;
   }

   this->data.clear();
   this->data_freq.clear();
   
   pthread_rwlock_unlock( &this->lock );
   pthread_rwlock_destroy( &this->lock );

   //printf(" cache shutdown\n");
}

int md_cache::get( char const* path, char** serialized, size_t* len, uid_t user ) {

   int rc = 0;
   
   string s( path );
   
   pthread_rwlock_wrlock( &this->lock );

   cache_map::iterator itr = this->data.find( s );
   if( itr != this->data.end() ) {

      if( user != itr->second.user && (!(itr->second.mode & S_IRGRP) || !(itr->second.mode & S_IROTH)) ) {
         // insufficient permission
         rc = -EACCES;
         //printf(" user %d cannot access %s\n", user, path );
      }
      else {
         *serialized = CALLOC_LIST( char, itr->second.data_len );
         memcpy( *serialized, itr->second.data, itr->second.data_len );
         *len = itr->second.data_len;
         
         uint64_t time_millis = itr->second.atime;
         itr->second.atime = currentTimeMillis();

         this->data_freq.erase( time_millis );
         this->data_freq[ itr->second.atime ] = s;

      }
   }
   else {
      rc = -ENOENT;
   }

   pthread_rwlock_unlock( &this->lock );
   
   
   return rc;
}


int md_cache::put( char const* path, char* ent_data, size_t len, uid_t user, mode_t mode ) {

   int rc = 0;
   
   string s(path);
   
   pthread_rwlock_wrlock( &this->lock );

   if( this->size >= this->max_size ) {
      
      time_map::iterator lru = this->data_freq.begin();
      cache_map::iterator itr = this->data.find( lru->second );
      
      if( itr != this->data.end() ) {
         free( itr->second.data );
         this->data.erase( itr );
      }

      this->do_clear( lru->second );
   }

   // if this is a file, clear its containing directory
   if( s.size() > 0 && s[ s.size() - 1 ] != '/' ) {
      
      this->do_clear( s );
      
   }
   
   uint64_t time_millis = currentTimeMillis();

   
   this->data[ s ].data = ent_data;
   this->data[ s ].atime = time_millis;
   this->data[ s ].user = user;
   this->data[ s ].mode = mode;
   this->data[ s ].data_len = len;
   this->data_freq[ time_millis ] = s;

   this->size++;

   
   pthread_rwlock_unlock( &this->lock );

   return rc;
}


// remove every cached item that starts with s's parent directory 
// must be write-locked first
int md_cache::do_clear( string s ) {

   fflush(stdout);
   
   char dirpath[PATH_MAX];
   memset( dirpath, 0, PATH_MAX );

   // s will end with '/' if it's a directory; we'll need to clear it if so
   if( s.size() > 0 && s[ s.size() - 1 ] == '/' ) {
      s[s.size()-1] = '\0';
   }
   
   md_dirname( s.c_str(), dirpath );

   for( cache_map::iterator itr = this->data.begin(); itr != this->data.end(); itr++ ) {
      if( itr->first.find( dirpath ) == 0 ) {
         uint64_t time_millis = itr->second.atime;
         
         free( itr->second.data );

         cache_map::iterator old = itr;
         itr++;

         this->data.erase( old );

         int64_t num_cleared = (int64_t)this->data_freq.erase( time_millis );

         if( num_cleared > 0 )
            this->size -= num_cleared;

         if( itr == this->data.end() )
            break;
      }
   }

   return 0;
}


int md_cache::clear( char const* path ) {

   // clear every entry that starts with path
   
   pthread_rwlock_wrlock( &this->lock );

   this->do_clear( string(path) );
   
   pthread_rwlock_unlock( &this->lock );
   
   return 0;
}
