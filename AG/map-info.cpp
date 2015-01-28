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

#include <algorithm>
#include "driver.h"
#include "map-info.h"
#include "core.h"
#include "cache.h"

// is a path an immediate child of another?
// return true if child refers to an immediate child in parent; false if not
// parent and child must be normalized paths
static bool AG_path_is_immediate_child( char const* parent, char const* child ) {
   
   char* child_parent = md_dirname( child, NULL );
   
   bool ret = (strcmp( child_parent, parent ) == 0);
   
   free( child_parent );
   
   return ret;
}

// get the maximum path depth over a set of paths
int AG_max_depth( AG_fs_map_t* map_infos ) {
   
   int max_depth = 0;
   
   for( AG_fs_map_t::iterator itr = map_infos->begin(); itr != map_infos->end(); itr++ ) {
      
      int cur_depth = md_depth( itr->first.c_str() );
      
      if( cur_depth > max_depth ) {
         max_depth = cur_depth;
      }
   }
   
   return max_depth;
}

// make a map info
void AG_map_info_init( struct AG_map_info* dest, int type, char const* query_string, mode_t file_perm, uint64_t reval_sec, struct AG_driver* driver ) {
   
   memset( dest, 0, sizeof(struct AG_map_info) );
   
   dest->type = type;
   
   if( query_string != NULL ) {
      dest->query_string = strdup(query_string);
   }
   
   dest->file_perm = file_perm;
   dest->reval_sec = reval_sec;
   
   struct timespec now;
   clock_gettime( CLOCK_MONOTONIC, &now );
   
   dest->refresh_deadline = dest->reval_sec + now.tv_sec;
   dest->driver = driver;
}

// free a map info
void AG_map_info_free( struct AG_map_info* mi ) {
   if( mi->query_string ) {
      free( mi->query_string );
      mi->query_string = NULL;
   }
}

// free an AG_fs_map_t
void AG_fs_map_free( AG_fs_map_t* fs_map ) {
   
   for( AG_fs_map_t::iterator itr = fs_map->begin(); itr != fs_map->end(); itr++ ) {
      
      if( itr->second != NULL ) {
         AG_map_info_free( itr->second );
         free( itr->second );
      }
   }
   
   fs_map->clear();
}

// merge info from a fresh AG_map_info into an existing AG_map_info, respecting which field(s) are read-only.
// this is preferred to duplicating an AG_map_info with AG_map_info_dup
void AG_map_info_merge( struct AG_map_info* dest, struct AG_map_info* src ) {
   
   if( src->cache_valid ) {
      
      AG_map_info_make_coherent_with_MS_data( dest, src->file_id, src->file_version, src->write_nonce, src->num_children, src->generation, src->capacity );
      AG_map_info_make_coherent_with_AG_data( dest, src->block_version, src->refresh_deadline );
   }
   
   if( src->query_string != NULL ) {
      
      if( dest->query_string != NULL ) {
         free( dest->query_string );
      }
      
      dest->query_string = strdup_or_null( src->query_string );
   }
   
   if( src->driver != NULL ) {
      
      dest->driver = src->driver;
   }
   
   if( dest->type != MD_ENTRY_DIR && dest->type != MD_ENTRY_FILE ) {
      dest->type = src->type;
   }
   
   if( src->driver_cache_valid ) {  
      
      AG_map_info_make_coherent_with_driver_data( dest, src->pubinfo.size, src->pubinfo.mtime_sec, src->pubinfo.mtime_nsec );
   }
}


// dump a mpa_info to stdout 
void AG_dump_map_info( char const* path, struct AG_map_info* mi ) {

   char* query_type = NULL;
   
   if( mi->driver != NULL ) {
      query_type = AG_driver_get_query_type( mi->driver );
   }
   
   dbprintf("%s:  addr=%p perm=%o reval=%" PRIu64 " driver=%s query_string=%s cache_valid=%d; cache { file_id=%" PRIX64 " version=%" PRId64 " write_nonce=%" PRId64 ", num_children=%" PRId64 ", capacity=%" PRId64 " }\n",
            path, mi, mi->file_perm, mi->reval_sec, query_type, mi->query_string, mi->cache_valid, mi->file_id, mi->file_version, mi->write_nonce, mi->num_children, mi->capacity );
   
   if( query_type != NULL ) {
      free( query_type );
   }
}

// read-lock the fs structure within an AG_fs
int AG_fs_rlock( struct AG_fs* ag_fs ) {
   int rc = pthread_rwlock_rdlock( &ag_fs->fs_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_rdlock(AG_fs %p) rc = %d\n", ag_fs, rc );
   }
   return rc;
}

// write-lock the fs structure within an AG_fs
int AG_fs_wlock( struct AG_fs* ag_fs ) {
   int rc = pthread_rwlock_wrlock( &ag_fs->fs_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_wrlock(AG_fs %p) rc = %d\n", ag_fs, rc );
   }
   return rc;
}

// unlock the fs structure within an AG_fs
int AG_fs_unlock( struct AG_fs* ag_fs ) {
   int rc = pthread_rwlock_unlock( &ag_fs->fs_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_unlock(AG_fs %p) rc = %d\n", ag_fs, rc );
   }
   return rc;
}


// duplicate an AG_fs from an AG_fs_map_t 
int AG_fs_map_dup( AG_fs_map_t* dest, AG_fs_map_t* src ) {
   
   for( AG_fs_map_t::iterator itr = src->begin(); itr != src->end(); itr++ ) {
      
      struct AG_map_info* mi = CALLOC_LIST( struct AG_map_info, 1 );
      
      if( mi == NULL ) {
         AG_fs_map_free( dest );
         return -ENOMEM;
      }
      
      AG_map_info_dup( mi, itr->second );
      
      (*dest)[ itr->first ] = mi;
   }
   
   return 0;
}

// given two fs_maps--old_fs and new_fs--find out the set of operations needed to transform old into new.
// that is, which elements must be deleted, updated, and published (to_delete, to_update, and to_publish, respectively).
// to_delete will contain pointers to map_infos in old_fs
// to_remain will contain pointers to map_infos from new_fs that are in both old_fs and new_fs, and ARE equal according to mi_equ
// to_update will contain pointers to map_infos in new_fs that are in both old_fs and new_fs, but NOT equal according to mi_equ
// to_publish, to_update, and to_delete should be empty when this method is called.
// NOTE: to_publish, to_update, and to_delete SHOULD NOT BE FREED.
int AG_fs_map_transforms( AG_fs_map_t* old_fs, AG_fs_map_t* new_fs, AG_fs_map_t* to_publish, AG_fs_map_t* to_remain, AG_fs_map_t* to_update, AG_fs_map_t* to_delete, AG_map_info_equality_func_t mi_equ ) {
   
   set<string> intersection;
   
   for( AG_fs_map_t::iterator itr = old_fs->begin(); itr != old_fs->end(); itr++ ) {
      
      const string& old_path = itr->first;
      struct AG_map_info* old_mi = itr->second;
      
      // is this entry in new_fs?
      AG_fs_map_t::iterator new_itr = new_fs->find( old_path );
      if( new_itr == new_fs->end() ) {
         
         // this old entry is not in the new fs.  We should delete it.
         (*to_delete)[ old_path ] = old_mi;
      }
      else {
         
         // this old entry is also in the new fs.
         if( !(*mi_equ)( old_mi, new_itr->second ) ) {
            (*to_update)[ old_path ] = new_itr->second;
         }
         else {
            (*to_remain)[ old_path ] = new_itr->second;
         }
         
         // track intersection between old_fs and new_fs
         intersection.insert( old_path );
      }
   }
   
   // find the entries in new_fs that are not in old_fs 
   for( AG_fs_map_t::iterator itr = new_fs->begin(); itr != new_fs->end(); itr++ ) {
      
      const string& new_path = itr->first;
      struct AG_map_info* new_mi = itr->second;
      
      // is this entry in old_fs?
      if( intersection.count( new_path ) > 0 ) {
         // already accounted for 
         continue;
      }
      else {
         // should publish this, since it's not in old_fs
         (*to_publish)[ new_path ] = new_mi;
      }
   }
   
   return 0;
}


// extract useful metadata from an md_entry into a map_info
// this will make the map info's MS and driver data coherent
int AG_copy_metadata_to_map_info( struct AG_map_info* mi, struct md_entry* ent ) {
   mi->file_id = ent->file_id;
   mi->file_version = ent->version;
   mi->write_nonce = ent->write_nonce;
   mi->type = ent->type;
   mi->num_children = ent->num_children;
   mi->generation = ent->generation;
   mi->capacity = ent->capacity;
   mi->cache_valid = true;
   
   mi->pubinfo.size = ent->size;
   mi->pubinfo.mtime_sec = ent->mtime_sec;
   mi->pubinfo.mtime_nsec = ent->mtime_nsec;
   mi->driver_cache_valid = true;
   
   dbprintf("%s (%" PRIX64 ") size=%zu modtime=%" PRId64 ".%" PRId32 "\n", ent->name, ent->file_id, ent->size, ent->mtime_sec, ent->mtime_nsec );
   return 0;
}


// invalidate cached MS data, so we get new listings for it when we ask the MS again 
int AG_invalidate_cached_metadata( struct AG_map_info* mi ) {
   mi->write_nonce = md_random64();
   mi->cache_valid = false;
   return 0;
}

// invalidate driver metadata 
int AG_invalidate_driver_metadata( struct AG_map_info* mi ) {
   mi->driver_cache_valid = false;
   return 0;
}

// invalidate all driver metadata 
int AG_invalidate_metadata_all( AG_fs_map_t* fs_map, int (*invalidator)( struct AG_map_info* ) ) {
   
   for( AG_fs_map_t::iterator itr = fs_map->begin(); itr != fs_map->end(); itr++ ) {
      (*invalidator)( itr->second );
   }
   
   return 0;
}

// invalidate a path's worth of metadata 
static int AG_invalidate_path_metadata( AG_fs_map_t* fs_map, char const* path ) {
   
   // invalidate all prefixes 
   char** prefixes = NULL;
   AG_path_prefixes( path, &prefixes );
   
   for( int i = 0; prefixes[i] != NULL; i++ ) {
      
      AG_fs_map_t::iterator itr = fs_map->find( string(prefixes[i]) );
      if( itr == fs_map->end() ) {
         // not found 
         errorf("Not found: %s\n", prefixes[i] );
         
         FREE_LIST( prefixes );
         return -ENOENT;
      }
      
      dbprintf("Invalidate %s\n", prefixes[i] );
      AG_invalidate_cached_metadata( itr->second );
   }
   
   FREE_LIST( prefixes );
   return 0;
}

// extract root information from the ms client 
int AG_map_info_get_root( struct ms_client* client, struct AG_map_info* root ) {
   
   int rc = 0;
   
   // get the volume root
   struct md_entry volume_root;
   memset( &volume_root, 0, sizeof(struct md_entry) );
   
   rc = ms_client_get_volume_root( client, &volume_root );
   if( rc != 0 ) {
      errorf("ms_client_get_volume_root() rc = %d\n", rc );
      return rc; 
   }
   
   AG_map_info_init( root, MD_ENTRY_DIR, NULL, volume_root.mode, volume_root.max_read_freshness / 1000, NULL );
   AG_copy_metadata_to_map_info( root, &volume_root );
   
   md_entry_free( &volume_root );
   return 0;
}


// initialize an AG_fs.
// NOTE: the AG_fs takes ownership of the fs_map
int AG_fs_init( struct AG_fs* ag_fs, AG_fs_map_t* fs_map, struct ms_client* ms ) {
   
   // initialize the fs 
   pthread_rwlock_init( &ag_fs->fs_lock, NULL );
   ag_fs->fs = fs_map;
   ag_fs->ms = ms;
   
   return 0;
}

// free an AG_fs 
// NOTE: you must ensure exclusive access on your own
int AG_fs_free( struct AG_fs* ag_fs ) {
   
   if( ag_fs->fs != NULL ) {
      
      AG_fs_map_free( ag_fs->fs );
      
      delete ag_fs->fs;
      ag_fs->fs = NULL;
   }
   
   pthread_rwlock_destroy( &ag_fs->fs_lock );
   
   return 0;
}

// duplicate a map info
void AG_map_info_dup( struct AG_map_info* dest, struct AG_map_info* src ) {
   AG_map_info_init( dest, src->type, src->query_string, src->file_perm, src->reval_sec, src->driver );
   AG_map_info_merge( dest, src );
}


// verify the structural integrity of an fs_map.
// * every path must have all of its ancestors
// * every ancestor must be a directory
int AG_validate_map_info( AG_fs_map_t* fs ) {
   
   // cache verified-good paths
   set<string> verified;
   
   // order paths by length, starting with the longest.
   vector<string> paths;
   
   for( AG_fs_map_t::iterator itr = fs->begin(); itr != fs->end(); itr++ ) {
      
      paths.push_back( itr->first );
   }
   
   // comparator by string length, deeper paths < shorter paths
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         return md_depth( s1.c_str() ) > md_depth( s2.c_str() );
      }
   };
   
   // sort them by decreasing length 
   sort( paths.begin(), paths.end(), local_string_length_comparator::comp );
   
   int rc = 0;
   
   for( unsigned int i = 0; i < paths.size(); i++ ) {
      
      // if this path is known good, then continue 
      if( verified.count( paths[i] ) != 0 ) {
         continue;
      }
      
      // otherwise, find its ancestors.
      // to do so, get all prefixes of this path
      char** ancestors = NULL;
      AG_path_prefixes( paths[i].c_str(), &ancestors );
      
      // omit the last prefix, which is the path itself 
      unsigned int last = 0;
      for( last = 0; ancestors[last] != NULL; last++ );
      
      if( last > 0 ) {
         free( ancestors[last-1] );
         ancestors[last-1] = NULL;
      }
      
      int err = 0;
      
      // each ancestor must be a directory 
      for( unsigned int j = 0; ancestors[j] != NULL; j++ ) {
         
         AG_fs_map_t::iterator itr = fs->find( string(ancestors[j]) );
         if( itr == fs->end() ) {
            // missing!
            errorf("ERR: Missing %s (ancestor of %s)\n", ancestors[j], paths[i].c_str() );
            err = -ENOENT;
            break;
         }
         
         // must be a directory 
         struct AG_map_info* mi = itr->second;
         if( mi->type != MD_ENTRY_DIR ) {
            // not a directory 
            errorf("ERR: not a directory: %s (ancestor of %s)\n", ancestors[j], paths[i].c_str() );
            err = -ENOTDIR;
            break;
         }
      }
      
      // remember this error, if one occurred
      if( err != 0 ) {
         rc = err;
      }
      else {
         // this path is good 
         verified.insert( paths[i] );
      }
      
      // clean up memory 
      FREE_LIST( ancestors );
   }
   
   return rc;
}


// given a path and map_info, get its pubinfo.
// check the cache first, and use the driver to get the pubinfo on cache miss.  cache the result.
// if mi is not coherent, then this method will skip the cache and poll the pubinfo from the driver.
// return 0 on success
// return -ENODATA if there is no driver loaded for the map info, and we couldn't find any cached data
int AG_get_publish_info_lowlevel( struct AG_state* state, char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pub_info ) {

   int rc = 0;
   
   if( mi->driver_cache_valid ) {
      
      dbprintf("Cache HIT on driver metadata for %s\n", path );
      
      // get cached pubinfo 
      if( pub_info != &mi->pubinfo ) {
         memcpy( pub_info, &mi->pubinfo, sizeof(struct AG_driver_publish_info) );
      }
      
      return 0;
   }
   
   // miss, or no driver.  Try to get details from the driver, if we have one.
   // (we might not, if we're deleting something that we discovered on the MS)
   if( mi->driver != NULL ) {
      
      dbprintf("Cache MISS on driver metadata for %s\n", path );
      
      // cache miss. ask the driver
      rc = AG_driver_stat( mi->driver, path, mi, pub_info );
      if( rc != 0 ) {
         errorf("AG_driver_stat(%s) rc = %d\n", path, rc );
         return rc;
      }
      else {
         
         // cache this 
         if( pub_info != &mi->pubinfo ) {
            memcpy( &mi->pubinfo, pub_info, sizeof(struct AG_driver_publish_info) );
         }
         
         mi->driver_cache_valid = true;
      }
   }
   else {
      // cache miss and no driver
      errorf("No driver for %s\n", path );
      return -ENODATA;
   }
   
   return rc;
}


// populate an md_entry from AG-specfile-given data and a map_info.
// if mi is coherent, then fill in its cached metadata
void AG_populate_md_entry_from_AG_info( struct md_entry* entry, struct AG_map_info* mi, uint64_t volume_id, uint64_t owner_id, uint64_t gateway_id, char const* path_basename ) {
   
   // fill in basics from our map info 
   entry->type = mi->type;
   entry->name = strdup( path_basename );
   entry->mode = mi->file_perm;
   entry->owner = owner_id;
   entry->coordinator = gateway_id;
   entry->volume = volume_id;
   entry->max_read_freshness = mi->reval_sec * 1000;
}


// populate an md_entry from cached MS-given data in a map_info 
// NOTE: don't check if coherent 
void AG_populate_md_entry_from_MS_info( struct md_entry* entry, uint64_t file_id, int64_t file_version, int64_t write_nonce ) {
   
   entry->file_id = file_id;
   entry->version = file_version;
   entry->write_nonce = write_nonce;
}


// populate an md_entry with driver-given data 
void AG_populate_md_entry_from_driver_info( struct md_entry* entry, struct AG_driver_publish_info* pub_info ) {
   
   entry->size = pub_info->size;
   entry->mtime_sec = pub_info->mtime_sec;
   entry->mtime_nsec = pub_info->mtime_nsec;
   entry->manifest_mtime_sec = pub_info->mtime_sec;
   entry->manifest_mtime_nsec = pub_info->mtime_nsec;
}


// get pubinfo from the driver.
// return 0 on success
// if the driver is NULL or the AG is shutting down, return -ENOTCONN
// if the driver callback fails, return its error code 
int AG_get_publish_info( char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pub_info ) {
   
   // use cached or driver-given pubinfo
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      return -ENOTCONN;
   }
   
   int rc = AG_get_publish_info_lowlevel( state, path, mi, pub_info );

   AG_release_state( state );
   
   if( rc != 0 && (rc != -ENODATA || rc == -ENODATA) ) {
      errorf("AG_get_publish_info_lowlevel( %s ) rc = %d\n", path, rc );
      return rc;
   }
   else {
      rc = 0;
   }
   
   return rc;
}


// populate an fs_map with driver info 
int AG_get_publish_info_all( struct AG_state* state, AG_fs_map_t* fs_map ) {
   
   int rc = 0;
   
   for( AG_fs_map_t::iterator itr = fs_map->begin(); itr != fs_map->end(); itr++ ) {
      
      rc = AG_get_publish_info_lowlevel( state, itr->first.c_str(), itr->second, &itr->second->pubinfo );
      if( rc != 0 ) {
         errorf("AG_get_publish_info_lowlevel(%s) rc = %d\n", itr->first.c_str(), rc );
         return rc;
      }
   }
   
   return rc;
}

// fill in basic fields for an md_entry, getting information from the driver and the map_info.
// if driver_required is true, then a driver is needed for the map_info (otherwise, this method will tolerate its absence and skip filling the md_entry with driver-supplied information)
// if opt_pubinfo is not NULL, it will be used in lieu of querying the driver or cache for the same information.  This implies driver_required == False
// this is useful for deleting entries, where driver-supplied information is not necessary.
int AG_populate_md_entry( struct ms_client* ms, struct md_entry* entry, char const* path, struct AG_map_info* mi, struct AG_map_info* parent_mi, int flags, struct AG_driver_publish_info* opt_pubinfo ) {
   
   int rc = 0;
   
   // sanity check 
   if( (flags & AG_POPULATE_NO_DRIVER) && opt_pubinfo == NULL ) {
      errorf("No data available for %s\n", path );
      return -EINVAL;
   }
   
   memset( entry, 0, sizeof(struct md_entry) );
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct AG_driver_publish_info pub_info;
   memset( &pub_info, 0, sizeof(pub_info) );
   
   if( (flags & AG_POPULATE_NO_DRIVER) ) {
      
      // use caller-given pubinfo
      memcpy( &pub_info, opt_pubinfo, sizeof(struct AG_driver_publish_info) );
   }
   else {
      
      // use internal (possibly cached) data
      rc = AG_get_publish_info( path, mi, &mi->pubinfo );
      if( rc != 0 ) {
         errorf("AG_get_publish_info(%s) rc = %d\n", path, rc );
         return rc;
      }
      
      memcpy( &pub_info, &mi->pubinfo, sizeof(struct AG_driver_publish_info) );
   }
   
   char* path_basename = md_basename( path, NULL );
   
   if( !(flags & AG_POPULATE_SKIP_DRIVER_INFO) ) {
      
      // fill the entry with the driver-given data 
      AG_populate_md_entry_from_driver_info( entry, &pub_info );
   }
   
   // fill in the entry with our AG-specific data 
   AG_populate_md_entry_from_AG_info( entry, mi, volume_id, ms->owner_id, ms->gateway_id, path_basename );
   
   // fill in the entry with our cached metadata, if we're coherent or if the caller wants us to
   if( mi->cache_valid || !(flags & AG_POPULATE_USE_MS_CACHE) ) {
      AG_populate_md_entry_from_MS_info( entry, mi->file_id, mi->file_version, mi->write_nonce );
   }
   
   free( path_basename );
   
   // don't publish these anyway 
   entry->xattr_nonce = 0;
   entry->error = 0;
   
   // supply parent info 
   entry->parent_name = md_dirname( path, NULL );
   
   if( parent_mi != NULL ) {
      entry->parent_id = parent_mi->file_id;
   }
   
   return rc;
}


// generate all prefixes for a path, including the path itself
// return the number of prefixes, or negative on error
int AG_path_prefixes( char const* path, char*** ret_prefixes ) {
   
   char* fs_path = strdup(path);
   char* fs_ms_path_tok = fs_path;
   char* tmp = NULL;
   
   vector<char*> prefixes;
   
   // start with root
   prefixes.push_back( strdup("/") );
   
   size_t buf_len = strlen(path);
   char* buf = CALLOC_LIST( char, buf_len + 5 );
   
   while( true ) {
      
      // next path part
      char* path_part = strtok_r( fs_ms_path_tok, "/", &tmp );
      fs_ms_path_tok = NULL;
      
      if( path_part == NULL ) {
         // no more path 
         break;
      }
      if( strlen(path_part) == 0 ) {
         break;
      }
      
      char* last_prefix = prefixes[ prefixes.size() - 1 ];
      
      // generate new prefix 
      // NOTE: omit the trailing / for directories
      memset( buf, 0, buf_len + 1 );
      md_fullpath( last_prefix, path_part, buf );
      
      prefixes.push_back( strdup(buf) );
   }
   
   free( buf );
   free( fs_path );
   
   // convert to char** 
   char** ret = CALLOC_LIST( char*, prefixes.size() + 1 );
   for( unsigned int i = 0; i < prefixes.size(); i++ ) {
      ret[i] = prefixes[i];
   }
   
   *ret_prefixes = ret;
   return prefixes.size();
}


// clone an item from an fs_map to another fs_map.
// return -ENOENT if not found
static int AG_clone_and_store_map_info( AG_fs_map_t* fs_map, char const* path, AG_fs_map_t* dest_map ) {

   string prefix_string(path);
   
   // find the path 
   AG_fs_map_t::iterator itr = fs_map->find( prefix_string );
   if( itr == fs_map->end() ) {
      
      // out of path 
      return -ENOENT;
   }
   
   struct AG_map_info* mi = itr->second;
   
   // dup it 
   struct AG_map_info* dup_info = CALLOC_LIST( struct AG_map_info, 1 );
   AG_map_info_dup( dup_info, mi );
   
   // store it 
   (*dest_map)[ prefix_string ] = dup_info;
   
   return 0;
}


// clone a path from AG_fs_map 
int AG_fs_map_clone_path( AG_fs_map_t* fs_map, char const* path, AG_fs_map_t* path_data ) {
   
   // get path prefixes...
   char** path_prefixes = NULL;
   AG_path_prefixes( path, &path_prefixes );
   
   int rc = 0;
   
   // use them to copy out directories from fs_map 
   for( int i = 0; path_prefixes[i] != NULL; i++ ) {
      
      rc = AG_clone_and_store_map_info( fs_map, path_prefixes[i], path_data );
      if( rc != 0 ) {
         
         errorf("Not found: %s\n", path_prefixes[i] );
         
         // out of path 
         FREE_LIST( path_prefixes );
         AG_fs_map_free( path_data );
         
         return -ENOENT;
      }
   }
   
   FREE_LIST( path_prefixes );
   
   return 0;
}


// merge a tree into an AG_fs_map.  Only merge new data on request (if merge_new is true).
// if merge_new is false, not_merged must not be NULL.  not_merged will contain pointers to all entries in path data that were not merged.
// return 0 on success, and -EINVAL on error.
// NOTE: this consumes data from path_data.  Don't free it after calling this method.
int AG_fs_map_merge_tree( AG_fs_map_t* fs_map, AG_fs_map_t* path_data, bool merge_new, AG_fs_map_t* not_merged ) {
   
   if( !merge_new && not_merged == NULL ) {
      return -EINVAL;
   }
   
   for( AG_fs_map_t::iterator itr = path_data->begin(); itr != path_data->end(); itr++ ) {
      
      const string& path_string = itr->first;
      struct AG_map_info* info = itr->second;
      
      struct AG_map_info* old_info = NULL;
      
      // find existing
      AG_fs_map_t::iterator itr2 = fs_map->find( path_string );
      if( itr2 != fs_map->end() ) {
         
         // exists
         old_info = itr2->second;
         
         // copy over relevant fields
         AG_map_info_merge( old_info, info );
         
         // consumed!
         itr->second = NULL;
         
         if( old_info != info ) {
            AG_map_info_free( info );
            free( info );
         }
      }
      else {
         if( !merge_new ) {
            // don't add new ones 
            (*not_merged)[ path_string ] = info;
         }
         else {
            // do add new ones
            (*fs_map)[ path_string ] = info;
         }
      }
   }
   
   path_data->clear();
   
   return 0;
}


// delete a tree of metadata 
// succeeds even if the data in to_delete isn't present in fs_map.
int AG_fs_map_delete_tree( AG_fs_map_t* fs_map, AG_fs_map_t* to_delete ) {
   
   for( AG_fs_map_t::iterator itr = to_delete->begin(); itr != to_delete->end(); itr++ ) {
      
      struct AG_map_info* mi = NULL;
      
      AG_fs_map_t::iterator found = fs_map->find( itr->first );
      if( found != fs_map->end() ) {
         
         mi = found->second;
         AG_map_info_free( mi );
         free( mi );
         
         fs_map->erase( found );
      }
   }
   
   return 0;
}


// copy over MS cached metadata
int AG_map_info_copy_MS_data( struct AG_map_info* dest, struct AG_map_info* src ) {
   if( src->cache_valid ) {
      return AG_map_info_make_coherent_with_MS_data( dest, src->file_id, src->file_version, src->write_nonce, src->num_children, src->generation, src->capacity );
   }
   else {
      return -EINVAL;
   }
}

// copy over driver cached metadata
int AG_map_info_copy_driver_data( struct AG_map_info* dest, struct AG_map_info* src ) {
   if( src->driver_cache_valid ) {
      return AG_map_info_make_coherent_with_driver_data( dest, src->pubinfo.size, src->pubinfo.mtime_sec, src->pubinfo.mtime_nsec );
   }
   else {
      return -EINVAL;
   }
}

// copy over AG cached metadata
int AG_map_info_copy_AG_data( struct AG_map_info* dest, struct AG_map_info* src ) {
   return AG_map_info_make_coherent_with_AG_data( dest, src->block_version, src->refresh_deadline );
}


// copy a tree's cached data into an AG_fs_map.  Don't copy data that exists in src but not in dest.  Only copy if coherent.
// src must be read-locked 
// dest must be write-locked
int AG_fs_copy_cached_data( struct AG_fs* dest, struct AG_fs* src, int (*copy)( struct AG_map_info* dest, struct AG_map_info* src ) ) {
   
   for( AG_fs_map_t::iterator itr = src->fs->begin(); itr != src->fs->end(); itr++ ) {
      
      const string& path_string = itr->first;
      struct AG_map_info* info = itr->second;
      int rc = 0;
      
      // find the matching dest 
      AG_fs_map_t::iterator dest_itr = dest->fs->find( path_string );
      if( dest_itr == dest->fs->end() ) {
         continue;
      }
      
      rc = (*copy)( dest_itr->second, info );
      if( rc != 0 ) {
         errorf("WARN: Failed to copy data from %p to %p (%s), rc = %d\n", info, dest_itr->second, path_string.c_str(), rc );
      }
   }
   
   return 0;
}


// given a path, map_info, and md_entry, store a duplicate of the map_info (populated by the given mi and ent) to the given new_data fs_map.
// this will silently overwrite (and free) existing entries in new_data
static int AG_accumulate_data_from_md_entry( AG_fs_map_t* new_data, char const* path, struct AG_map_info* mi, struct md_entry* ent ) {
   // save this to new_data
   struct AG_map_info* new_info = CALLOC_LIST( struct AG_map_info, 1 );
   
   if( mi != NULL ) {
      AG_map_info_dup( new_info, mi );
   }
   else {
      AG_map_info_init( new_info, ent->type, NULL, ent->mode, ent->max_read_freshness / 1000, NULL );
   }
   
   // got it!
   AG_copy_metadata_to_map_info( new_info, ent );
   
   // merge into new_data, freeing an old entry if need be
   AG_fs_map_t::iterator itr = new_data->find( string(path) );
   if( itr != new_data->end() ) {
      
      struct AG_map_info* old_mi = itr->second;
      new_data->erase( itr );
      
      AG_map_info_free( old_mi );
      free( old_mi );
   }
   
   (*new_data)[ string(path) ] = new_info;
   
   return 0;
}


// convert a path and its associated AG map infos to an ms_path.
// the data in path_info must be coherent.
// return 0 on success
// return -EINVAL if path_info is missing data referred to by path
static int AG_path_info_to_ms_path_ex( uint64_t volume_id, char const* path, AG_fs_map_t* path_info, ms_path_t* ms_path, bool (*filter)(struct AG_map_info* mi, void* cls), void* cls ) {
   
   char** prefixes = NULL;
   int num_prefixes = 0;
   struct ms_path_ent ms_ent;
   AG_fs_map_t::iterator itr;
   struct AG_map_info* mi = NULL;
   uint64_t parent_id = 0;
   char* name = NULL;
   bool include = false;
   
   num_prefixes = AG_path_prefixes( path, &prefixes );
   
   // verify they're all there 
   for( int i = 0; i < num_prefixes; i++ ) {
      
      if( path_info->find( string(prefixes[i]) ) == path_info->end() ) {
         
         FREE_LIST( prefixes );
         return -EINVAL;
      }
   }
   
   // convert them 
   for( int i = 0; i < num_prefixes; i++ ) {
      
      itr = path_info->find( string(prefixes[i]) );
      mi = itr->second;
      
      // filter?
      include = true;
      
      if( filter != NULL ) {
         include = (*filter)( mi, cls );
      }
      
      if( !include ) {
         continue;
      }
      
      memset( &ms_ent, 0, sizeof(struct ms_path_ent) );
      
      name = md_basename( prefixes[i], NULL );
      
      ms_client_make_path_ent( &ms_ent, volume_id, parent_id, mi->file_id, mi->file_version, mi->write_nonce, mi->num_children, mi->generation, mi->capacity, name, NULL );
      
      free( name );
      
      ms_path->push_back( ms_ent );
      
      // next entry 
      parent_id = mi->file_id;
   }
   
   FREE_LIST( prefixes );
   
   return 0;
}


// given a path and path info for it, generate an ms_path such that:
// * the first element has fresh data 
// * all subsequent elements are stale
// this is called a consistency work path, since it will be used as a work queue for consistency checks
// return 0 on success
// return -EINVAL if we failed to generate a path (can only be due to incomplete data from the caller)
static int AG_consistency_work_path_init( struct ms_client* client, char const* path, AG_fs_map_t* path_info, ms_path_t* ms_path ) {
   
   ms_path_t ms_path_fresh;
   int rc = 0;
   uint64_t volume_id = ms_client_get_volume_id( client );
   
   // convert fresh entries to ms_path_fresh 
   rc = AG_path_info_to_ms_path_ex( volume_id, path, path_info, &ms_path_fresh, AG_path_filters::is_fresh, NULL );
   if( rc != 0 ) {
      errorf("AG_path_info_to_ms_path_ex(%s, fresh) rc = %d\n", path, rc );
      return -EINVAL;
   }
   
   // convert stale entries to ms_path
   rc = AG_path_info_to_ms_path_ex( volume_id, path, path_info, ms_path, AG_path_filters::is_stale, NULL );
   if( rc != 0 ) {
      errorf("AG_path_info_to_ms_path_ex(%s, stale) rc = %d\n", path, rc );
      
      ms_client_free_path( &ms_path_fresh, NULL );
      return -EINVAL;
   }
   
   // include the deepest fresh path as the head of ms_path, so we can resolve the stale entries 
   ms_path->insert( ms_path->begin(), ms_path_fresh[ ms_path_fresh.size() - 1 ] );
   ms_path_fresh.pop_back();
   
   ms_client_free_path( &ms_path_fresh, NULL );
   
   return 0;
}
   

// given a coherent AG_map_info and the path to it, list its contents.
// merge the entries into new_data.
// return 0 on success
// return -EINVAL if dir_info isn't coherent
// return negative on MS communication or protocol error
static int AG_listdir( struct ms_client* client, char const* fs_path, struct AG_map_info* dir_info, AG_fs_map_t* new_data ) {
   
   int rc = 0;
   struct ms_client_multi_result results;
   char* fp = NULL;
   
   if( !dir_info->cache_valid ) {
      errorf("Directory %s is not valid\n", fs_path );
      return -EINVAL;
   }
   
   memset( &results, 0, sizeof(struct ms_client_multi_result));
   
   // get the listing
   rc = ms_client_listdir( client, dir_info->file_id, dir_info->num_children, dir_info->capacity, &results );
   
   if( rc != 0 ) {
      errorf("ms_client_listdir(%" PRIX64 " %s) rc = %d\n", dir_info->file_id, fs_path, rc );
      return rc;
   }
   
   if( results.reply_error != 0 ) {
      errorf("ms_client_listdir(%" PRIX64 " %s) rc = %d\n", dir_info->file_id, fs_path, rc );
      
      ms_client_multi_result_free( &results );
      return rc;
   }
   
   // merge listing into new data
   for( unsigned int i = 0; i < results.num_ents; i++ ) {
      
      fp = md_fullpath( fs_path, results.ents[i].name, NULL );
      
      AG_accumulate_data_from_md_entry( new_data, fp, NULL, &results.ents[i] );
      
      free( fp );
   }
   
   ms_client_multi_result_free( &results );
   return 0;
}


// get a path-worth of metadata, and merge it into ret_new_data.
// merging is all-or-nothing.
// return 0 on success
// return negative on failure
// return -EIO to indicate a bug
static int AG_path_download( struct ms_client* client, char const* path, AG_fs_map_t* path_info, AG_fs_map_t* ret_new_data ) {
   
   int rc = 0;
   
   ms_path_t ms_path;           // all stale path entries
   AG_fs_map_t new_data;        // downloaded data
   
   int download_rc = 0;
   int download_error_idx = 0;
   
   dbprintf("Get metadata for %s\n", path );
   
   // make the work path 
   rc = AG_consistency_work_path_init( client, path, path_info, &ms_path );
   if( rc != 0 ) {
      errorf("AG_consistency_work_path_init(%s, fresh) rc = %d\n", path, rc );
      return -EINVAL;
   }
   
   // download the path 
   rc = ms_client_path_download( client, &ms_path, NULL, NULL, &download_rc, &download_error_idx );
   if( rc != 0 ) {
      errorf("ms_client_path_download(%s) rc = %d\n", path, rc );
      
      ms_client_free_path( &ms_path, NULL );
      return rc;
   }
   
   if( download_rc != 0 ) {
      errorf("ms_client_path_download(%s) download rc = %d\n", path, download_rc );
      
      ms_client_free_path( &ms_path, NULL );
      return download_rc;
   }
   
   // merge the data (skip the first path entry, since it was fresh already)
   for( unsigned int i = 1; i < ms_path.size(); i++ ) {
      
      char* prefix = ms_path_to_string( &ms_path, i );
      struct AG_map_info* dup = AG_fs_lookup_path_in_map( path_info, prefix );
      
      if( dup == NULL ) {
         // not found!  shouldn't happen, so this is a bug 
         errorf("BUG: %s not found\n", prefix);
         free( prefix );
         rc = -EIO;
         break;
      }
      
      AG_map_info_make_coherent_with_MS_data( dup, ms_path[i].file_id, ms_path[i].version, ms_path[i].write_nonce, ms_path[i].num_children, ms_path[i].generation, ms_path[i].capacity );
      
      new_data[ string(prefix) ] = dup;
   }
   
   // success?  if so, merge in 
   if( rc == 0 ) {
      
      AG_fs_map_merge_tree( ret_new_data, &new_data, true, NULL );
   }
  
   return rc;
}


// ensure that a path-worth of metadata is cached and valid.
// if force_reload is true, then re-download all the metadata regardless.
// on success, all map_infos along the path will be coherent
// NOTE: only entries represented by the path will be modified.  Newly-discovered child entires, for example, will NOT be added to the fs
// AG_fs must not be locked
int AG_fs_refresh_path_metadata( struct AG_fs* ag_fs, char const* path, bool force_reload ) {
   
   dbprintf("Refresh %s in %p\n", path, ag_fs->fs );
   
   AG_fs_map_t path_info;
   AG_fs_map_t new_path_info;
   int rc = 0;
   
   // get a copy of this path from the fs 
   AG_fs_rlock( ag_fs );
   rc = AG_fs_map_clone_path( ag_fs->fs, path, &path_info );
   AG_fs_unlock( ag_fs );
   
   if( rc != 0 ) {
      errorf("AG_fs_map_clone_path(%s) rc = %d\n", path, rc );
      return rc;
   }
   
   // if we need to force reload, then invalidate the path first 
   if( force_reload ) {
      AG_invalidate_path_metadata( ag_fs->fs, path );
   }
   
   // get the metadata
   rc = AG_path_download( ag_fs->ms, path, &path_info, &new_path_info );
   if( rc != 0 ) {
      errorf("AG_path_download(%s) rc = %d\n", path, rc );
      
      AG_fs_map_free( &path_info );
      AG_fs_map_free( &new_path_info );
      return rc;
   }
   
   AG_fs_map_free( &path_info );
   
   AG_fs_map_t not_merged;
   
   // merge the path back in (do so here, to avoid locking the tree during I/O)
   // Do NOT merge new data--we should already know all map_infos
   AG_fs_wlock( ag_fs );
   rc = AG_fs_map_merge_tree( ag_fs->fs, &new_path_info, false, &not_merged );
   AG_fs_unlock( ag_fs );
   
   AG_fs_map_free( &not_merged );
   
   if( rc != 0 ) {
      errorf("AG_fs_map_merge_tree(%s) rc = %d\n", path, rc );
      
      return rc;
   }
   
   return rc;
}


// look up an AG_map_info from a path, in a map_info 
// return a duplicate of it on success
// return NULL if not found.
struct AG_map_info* AG_fs_lookup_path_in_map( AG_fs_map_t* map_info, char const* path ) {
   
   // do we have a map_info for this?
   AG_fs_map_t::iterator child_itr = map_info->find( string(path) );
   if( child_itr == map_info->end() ) {
      
      return NULL;
   }
   
   // send back a copy 
   struct AG_map_info* ret = CALLOC_LIST( struct AG_map_info, 1 );
   
   AG_map_info_dup( ret, child_itr->second );
   
   return ret;
}


// look up an AG_map_info from a path 
// AG_fs must not be locked
struct AG_map_info* AG_fs_lookup_path( struct AG_fs* ag_fs, char const* path ) {

   AG_fs_rlock( ag_fs );
   
   struct AG_map_info* ret = AG_fs_lookup_path_in_map( ag_fs->fs, path );
   
   AG_fs_unlock( ag_fs );
   
   return ret;
}


// make a given map_info coherent with new MS data 
int AG_map_info_make_coherent_with_MS_data( struct AG_map_info* mi, uint64_t file_id, int64_t file_version, int64_t write_nonce, uint64_t num_children, int64_t generation, int64_t capacity ) {

   // update the cache data
   mi->file_id = file_id;
   mi->file_version = file_version;
   mi->write_nonce = write_nonce;
   mi->num_children = num_children;
   mi->generation = generation;
   mi->capacity = capacity;
   
   mi->cache_valid = true;
   
   return 0;
}
   
// make a given map info coherent with new driver data 
int AG_map_info_make_coherent_with_driver_data( struct AG_map_info* mi, size_t size, int64_t mtime_sec, int32_t mtime_nsec ) {
   
   mi->pubinfo.size = size;
   mi->pubinfo.mtime_sec = mtime_sec;
   mi->pubinfo.mtime_nsec = mtime_nsec;
   mi->driver_cache_valid = true;
   
   return 0;
}

// make a given map info coherent with AG runtime data 
int AG_map_info_make_coherent_with_AG_data( struct AG_map_info* mi, int64_t block_version, uint64_t refresh_deadline ) {

   mi->block_version = block_version;
   mi->refresh_deadline = refresh_deadline;
   
   return 0;
}
   
// set an AG_map_info's cached metadata in-place, making it coherent with the reference map info (which must be coherent)
// optionally fill updated_mi with the newly-coherent information.
int AG_fs_make_coherent( struct AG_fs* ag_fs, char const* path, struct AG_map_info* ref_mi, struct AG_map_info* updated_mi ) {
   
   if( !ref_mi->cache_valid ) {
      return -EINVAL;   
   }
   
   if( !ref_mi->driver_cache_valid ) {
      return -EINVAL;
   }
   
   AG_fs_wlock( ag_fs );
   
   // do we have a map_info for this?
   AG_fs_map_t::iterator child_itr = ag_fs->fs->find( string(path) );
   if( child_itr == ag_fs->fs->end() ) {
      
      AG_fs_unlock( ag_fs );
      return -ENOENT;
   }
   
   // update the versions 
   struct AG_map_info* mi = child_itr->second;
   
   AG_map_info_make_coherent_with_MS_data( mi, ref_mi->file_id, ref_mi->file_version, ref_mi->write_nonce, ref_mi->num_children, ref_mi->generation, ref_mi->capacity );
   AG_map_info_make_coherent_with_driver_data( mi, ref_mi->pubinfo.size, ref_mi->pubinfo.mtime_sec, ref_mi->pubinfo.mtime_nsec );
   AG_map_info_make_coherent_with_AG_data( mi, ref_mi->block_version, ref_mi->refresh_deadline );
   
   if( updated_mi != NULL ) {
      AG_map_info_dup( updated_mi, mi );
   }
   
   AG_fs_unlock( ag_fs );
   
   return 0;
}


// insert a map info into an fs map 
// the fs_map takes ownership of the mi
// return 0 on success
// return -EEXIST if it's already present
// ag_fs must not be locked
int AG_fs_map_insert( struct AG_fs* ag_fs, char const* path, struct AG_map_info* mi ) {
   
   int rc = 0;
   AG_fs_wlock( ag_fs );
   
   // do we have a map_info for this?
   AG_fs_map_t::iterator child_itr = ag_fs->fs->find( string(path) );
   if( child_itr != ag_fs->fs->end() ) {
      
      AG_fs_unlock( ag_fs );
      return -EEXIST;
   }
   
   (*ag_fs->fs)[ string(path) ] = mi;
   
   AG_fs_unlock( ag_fs );
     
   return rc;
}


// remove a map info from an fs_map
// return 0 on success
// return -ENOENT if it's not there 
// ag_fs must not be locked
int AG_fs_map_remove( struct AG_fs* ag_fs, char const* path, struct AG_map_info** ret_mi ) {
   
   int rc = 0;
   AG_fs_wlock( ag_fs );
   
   // do we have a map_info for this?
   AG_fs_map_t::iterator child_itr = ag_fs->fs->find( string(path) );
   if( child_itr != ag_fs->fs->end() ) {
      
      AG_fs_unlock( ag_fs );
      return -ENOENT;
   }
   
   *ret_mi = child_itr->second;
  
   ag_fs->fs->erase( child_itr );
   
   AG_fs_unlock( ag_fs );
     
   return rc;
}

// make an absolute reval deadline from a given map_info lifetime (reval_sec)
int64_t AG_map_info_make_deadline( int64_t reval_sec ) {
   
   struct timespec ts;
   clock_gettime( CLOCK_MONOTONIC, &ts );
   
   return reval_sec + ts.tv_sec;
}


// find all directories and the number of children they have in an fs_map 
int AG_fs_count_children( AG_fs_map_t* fs_map, map<string, int>* child_counts ) {
   
   // find all directories in the specfile, and count their children in specfile_child_counts
   for( AG_fs_map_t::iterator itr = fs_map->begin(); itr != fs_map->end(); itr++ ) {
      
      struct AG_map_info* mi = itr->second;
      
      char* path = strdup( itr->first.c_str() );
      if( path == NULL ) {
         return -ENOMEM;
      }
      
      md_sanitize_path( path );
      
      string path_s( path );
      
      // add count entry if directory
      if( mi->type == MD_ENTRY_DIR ) {
         
         map<string, int>::iterator count_itr = child_counts->find( path_s );
         if( count_itr == child_counts->end() ) {
            
            (*child_counts)[ path_s ] = 0;
         }
      }
      
      // increment parent count 
      char* parent_path = md_dirname( path, NULL );
      if( parent_path == NULL ) {
         free( path );
         return -ENOMEM;
      }
      
      // ensure r-stripped of /
      md_sanitize_path( parent_path );
      
      // increment parent count 
      string parent_path_s( parent_path );
      
      map<string, int>::iterator count_itr = child_counts->find( parent_path_s );
      if( count_itr == child_counts->end() ) {
         
         (*child_counts)[ itr->first ] = 1;
      }
      else {
         count_itr->second ++;
      }
      
      free( parent_path );
      free( path );
   }
   
   return 0;
}


// given the specfile and cached MS data, find the frontier of the cached data
// that is, find all directories in the cached data that:
// * have a child in the specfile that is not cached
// * have a different number of children than what the specfile says
static int AG_fs_find_frontier( AG_fs_map_t* specfile, AG_fs_map_t* on_MS, vector<string>* frontier ) {
   
   map<string, int> specfile_child_counts;
   map<string, int> MS_child_counts;
   
   // count up children 
   AG_fs_count_children( specfile, &specfile_child_counts );
   AG_fs_count_children( on_MS, &MS_child_counts );
   
   set<string> frontier_set;    // use this to prevent duplicate insertions 
   
   // find all directories in the specfile that aren't on the MS, or that have a different number of children
   for( map<string, int>::iterator itr = specfile_child_counts.begin(); itr != specfile_child_counts.end(); itr++ ) {
      
      map<string, int>::iterator ms_itr = MS_child_counts.find( itr->first );
      
      if( ms_itr == MS_child_counts.end() ) {
         
         char** prefixes = NULL;
         int num_prefixes = AG_path_prefixes( itr->first.c_str(), &prefixes );
         
         // find the deepest ancestor in the cached data; this is the frontier directory
         for( int i = num_prefixes - 1; i >= 0; i-- ) {
            
            string prefix_s( prefixes[i] );
            
            ms_itr = MS_child_counts.find( prefix_s );
            if( ms_itr != MS_child_counts.end() ) {
               
               if( frontier_set.count( prefix_s ) == 0 ) {
                  
                  dbprintf("Add %s to frontier: it is in the specfile, but not cached\n", itr->first.c_str() );
               
                  frontier->push_back( prefix_s );
                  frontier_set.insert( prefix_s );
               }
               
               break;
            }
         }
         
         FREE_LIST( prefixes );
      }
      else if( ms_itr->second != itr->second ) {
         
         if( frontier_set.count( itr->first ) == 0 ) {
            
            dbprintf("Add %s to frontier: specfile lists %d children, but the cache has %d\n", itr->first.c_str(), itr->second, ms_itr->second );
            frontier->push_back( itr->first );
            frontier_set.insert( itr->first );
         }
      }
   }
   
   return 0;
}


// find the set of entries that already exist on the MS, and put them into on_MS.
// don't re-download items that are already present in on_MS
// specfile_fs contains the specfile's data, and must be well-formed (i.e. every element has a parent except root)
// NOTE: regardless of sucess or failure, the caller must free the on_MS's contents
int AG_download_MS_fs_map( struct ms_client* ms, AG_fs_map_t* specfile_fs, AG_fs_map_t* on_MS ) {
   
   dbprintf("%s", "Begin downloading\n");
   
   int rc = 0;
   vector<string> frontier;
   
   if( on_MS->size() == 0 ) {
      
      // do the whole tree
      struct AG_map_info* root = CALLOC_LIST( struct AG_map_info, 1 );
      
      rc = AG_map_info_get_root( ms, root );
      if( rc != 0 ) {
         errorf("AG_map_info_get_root rc = %d\n", rc );
         
         free( root );
         
         dbprintf("End downloading (failure, rc = %d)\n", rc);
         return rc;
      }
      
      (*on_MS)[ string("/") ] = root;
      
      frontier.push_back( string("/") );
   }
   
   else {
      
      // build up our frontier from the empty directories in on_MS
      rc = AG_fs_find_frontier( specfile_fs, on_MS, &frontier );
      if( rc != 0 ) {
         errorf("AG_fs_find_frontier rc = %d\n", rc );
         return rc;
      }
   }
   
   while( frontier.size() > 0 ) {
      
      // next directory 
      string dir_path_s = frontier.front();
      frontier.erase( frontier.begin() );
      
      char const* dir_path = dir_path_s.c_str();
      struct AG_map_info* dir_info = NULL;
      
      dbprintf("Explore '%s'\n", dir_path );
         
      // newly-discovered data 
      AG_fs_map_t new_info;
      
      // children of the deepest directory
      struct ms_client_multi_result children;
      memset( &children, 0, sizeof(struct ms_client_multi_result) );
      
      // find this directory's info 
      dir_info = AG_fs_lookup_path_in_map( on_MS, dir_path );
      if( dir_info == NULL ) {
         // not found 
         errorf("Not found: %s\n", dir_path );
         
         rc = -ENOENT;
         break;
      }
      
      // read this directory
      rc = AG_listdir( ms, dir_path, dir_info, &new_info );
      
      AG_map_info_free( dir_info );
      free( dir_info );
      
      if( rc != 0 ) {
         errorf("AG_listdir(%s) rc = %d\n", dir_path, rc );
         break;
      }
      
      // find unexplored info, and invalidate newly-discovered directories
      for( AG_fs_map_t::iterator itr = new_info.begin(); itr != new_info.end(); itr++ ) {
         
         char const* child_path = itr->first.c_str();
         struct AG_map_info* mi = itr->second;
         
         // ignore non-immediate children of the current directory, and ignore root
         if( !AG_path_is_immediate_child( dir_path, child_path ) || strcmp( child_path, "/" ) == 0 ) {
            dbprintf("Ignore %s\n", child_path);
            continue;
         }
         
         // explore child directories
         if( mi->type == MD_ENTRY_DIR ) {
            
            dbprintf("Will explore '%s'\n", child_path );
            
            string child_path_s( child_path );
            
            frontier.push_back( child_path_s );
         }
      }
      
      // merge discovered data back in (NOTE: don't free new_info, since it will be merged)
      rc = AG_fs_map_merge_tree( on_MS, &new_info, true, NULL );
      
      if( rc != 0 ) {
         errorf("AG_fs_map_merge_tree(%s) rc = %d\n", dir_path, rc );
         break;
      }
   }
   
   if( rc == 0 ) {
      
      dbprintf("Downloaded file mapping %p:\n", on_MS);
      AG_dump_fs_map( on_MS );
      
      dbprintf("%s", "End downloading (success)\n");
   }
   else {
      
      dbprintf("End downloading (failure, rc = %d)\n", rc);
   }
   
   return rc;
}


// log the contents of an fs map 
int AG_dump_fs_map( AG_fs_map_t* fs_map ) {
   
   // print in order by path, and then by lexical order
   vector<string> paths;
   
   for( AG_fs_map_t::iterator itr = fs_map->begin(); itr != fs_map->end(); itr++ ) {
      
      paths.push_back( itr->first );
   }
   
   // comparator by depth, shortest < longest
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         return md_depth(s1.c_str()) < md_depth(s2.c_str());
      }
   };
   
   // sort them by decreasing length 
   sort( paths.begin(), paths.end(), local_string_length_comparator::comp );
   
   dbprintf("Begin FS map %p\n", fs_map );
   
   for( unsigned int i = 0; i < paths.size(); i++ ) {
      
      char const* path = paths[i].c_str();
      struct AG_map_info* mi = (*fs_map)[ paths[i] ];
   
      AG_dump_map_info( path, mi );
   }
   
   dbprintf("End FS map %p\n", fs_map );
   
   return 0;
}
