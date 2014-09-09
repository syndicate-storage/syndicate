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
#include "map-info.h"
#include "core.h"
#include "cache.h"

static int AG_path_prefixes( char const* path, char*** ret_prefixes );

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

void AG_map_info_free( struct AG_map_info* mi ) {
   if( mi->query_string ) {
      free( mi->query_string );
      mi->query_string = NULL;
   }
}

// free an AG_fs_map_t
void AG_fs_map_free( AG_fs_map_t* fs_map ) {
   
   for( AG_fs_map_t::iterator itr = fs_map->begin(); itr != fs_map->end(); itr++ ) {
      
      AG_map_info_free( itr->second );
      free( itr->second );
   }
   
   fs_map->clear();
}

// merge info from a fresh AG_map_info into an existing AG_map_info, respecting which field(s) are read-only 
void AG_map_info_make_coherent( struct AG_map_info* dest, struct AG_map_info* src ) {
   
   if( !dest->cache_valid && src->cache_valid ) {
      dest->file_id = src->file_id;
      dest->file_version = src->file_version;
      dest->block_version = src->block_version;
      dest->write_nonce = src->write_nonce;
      dest->cache_valid = src->cache_valid;
      dest->refresh_deadline = src->refresh_deadline;
   }
   
   if( dest->query_string == NULL && src->query_string != NULL ) {
      dest->query_string = strdup_or_null( src->query_string );
   }
   
   if( dest->driver == NULL && src->query_string != NULL ) {
      dest->driver = src->driver;
   }
   
   if( dest->type != MD_ENTRY_DIR && dest->type != MD_ENTRY_FILE ) {
      dest->type = src->type;
   }
}


// dump a mpa_info to stdout 
void AG_dump_map_info( char const* path, struct AG_map_info* mi ) {

   char* query_type = NULL;
   
   if( mi->driver != NULL ) {
      query_type = AG_driver_get_query_type( mi->driver );
   }
   
   dbprintf("%s:  addr=%p perm=%o reval=%" PRIu64 " driver=%s query_string=%s cache_valid=%d; cache { file_id=%" PRIX64 " version=%" PRId64 " write_nonce=%" PRId64 " }\n",
            path, mi, mi->file_perm, mi->reval_sec, query_type, mi->query_string, mi->cache_valid, mi->file_id, mi->file_version, mi->write_nonce );
   
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
// to_publish and to_update will contain pointers to map_infos in new_fs.
// to be put into to_update, the elements mapped to the same path in old_fs and new_fs must FAIL the equality test mi_equ.
// to_publish, to_update, and to_delete should be empty when this method is called.
// NOTE: to_publish, to_update, and to_delete SHOULD NOT BE FREED.
int AG_fs_map_transforms( AG_fs_map_t* old_fs, AG_fs_map_t* new_fs, AG_fs_map_t* to_publish, AG_fs_map_t* to_update, AG_fs_map_t* to_delete, AG_map_info_equality_func_t mi_equ ) {
   
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
         // is there any difference between the two that warrants an update?
         if( !(*mi_equ)( old_mi, new_itr->second ) ) {
            (*to_update)[ old_path ] = new_itr->second;
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
static int AG_copy_metadata_to_map_info( struct AG_map_info* mi, struct md_entry* ent ) {
   mi->file_id = ent->file_id;
   mi->file_version = ent->version;
   mi->write_nonce = ent->write_nonce;
   mi->type = ent->type;
   
   return 0;
}

// invalidate cached data, so we get new listings for it when we ask the MS again 
static int AG_invalidate_cached_metadata( struct AG_map_info* mi ) {
   dbprintf("Invalidate %p\n", mi);
   mi->write_nonce = md_random64();
   mi->cache_valid = false;
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
   
   root->cache_valid = true;
   
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
int AG_fs_free( struct AG_fs* ag_fs ) {
   
   AG_fs_wlock( ag_fs );
   
   if( ag_fs->fs ) {
      
      AG_fs_map_free( ag_fs->fs );
      
      delete ag_fs->fs;
      ag_fs->fs = NULL;
   }
   
   AG_fs_unlock( ag_fs );
   pthread_rwlock_destroy( &ag_fs->fs_lock );
   
   return 0;
}

// duplicate a map info
void AG_map_info_dup( struct AG_map_info* dest, struct AG_map_info* src ) {
   AG_map_info_init( dest, src->type, src->query_string, src->file_perm, src->reval_sec, src->driver );
   
   if( src->cache_valid ) {
      AG_map_info_make_coherent( dest, src );
   }
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
   
   // comparator by string length, longest < shortest
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         return (s1.size() > s2.size());
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


// given a path and coherent map_info, get its pubinfo.
// check the cache first, and use the driver to get the pubinfo on cache miss.  cache the result.
// if mi is not coherent, then this method will skip the cache and poll the pubinfo from the driver.  The result will NOT be cached, since the mi isn't coherent
// return 0 on success
// return -ENODATA if there is no driver loaded for the map info, and we couldn't find any cached data
int AG_get_pub_info( struct AG_state* state, char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pub_info ) {

   int rc = 0;
   
   if( mi->cache_valid ) {
      
      // see if we have a cached pubinfo 
      rc = AG_cache_get_stat( state, path, mi->file_version, pub_info );
      
      if( rc == 0 ) {
         // cache hit 
         AG_cache_promote_stat( state, path, mi->file_version );
         
         return rc;
      }
   }

   // get details from the driver, if we have one
   // (we might not, if we're deleting something that we discovered on the MS)
   if( mi->driver != NULL ) {
      
      // cache miss. ask the drivers
      rc = AG_driver_stat( mi->driver, path, mi, pub_info );
      if( rc != 0 ) {
         errorf("AG_driver_stat(%s) rc = %d\n", path, rc );
         return rc;
      }
      
      if( mi->cache_valid ) {
         
         // have coherent information, so we can cache this
         rc = AG_cache_put_stat_async( state, path, mi->file_version, pub_info );
         if( rc != 0 ) {
            
            // mask error, since this isn't required for correctness
            errorf("WARN: AG_cache_put_stat_async(%s) rc = %d\n", path, rc );
            rc = 0;
         }
      }
   }
   else {
      // cache miss and no driver
      return -ENODATA;
   }
   
   return rc;
}


// populate an md_entry from data in a map_info.
// if mi is coherent, then fill in its cached metadata
static void AG_populate_md_entry_from_map_info( struct md_entry* entry, struct AG_map_info* mi, uint64_t volume_id, uint64_t owner_id, uint64_t gateway_id, char const* path_basename ) {
   
   // fill in basics from our map info 
   entry->type = mi->type;
   entry->name = strdup( path_basename );
   entry->mode = mi->file_perm;
   entry->owner = owner_id;
   entry->coordinator = gateway_id;
   entry->volume = volume_id;
   entry->max_read_freshness = mi->reval_sec * 1000;
   
   if( mi->cache_valid ) {
      entry->file_id = mi->file_id;
      entry->version = mi->file_version;
      entry->write_nonce = mi->write_nonce;
   }
}


// populate an md_entry with driver-given data 
static void AG_populate_md_entry_from_publish_info( struct md_entry* entry, struct AG_driver_publish_info* pub_info ) {
   
   entry->size = pub_info->size;
   entry->mtime_sec = pub_info->mtime_sec;
   entry->mtime_nsec = pub_info->mtime_nsec;
   entry->manifest_mtime_sec = pub_info->mtime_sec;
   entry->manifest_mtime_nsec = pub_info->mtime_nsec;
}

// fill in basic fields for an md_entry, getting information from the driver and the map_info.  map_info must be coherent.
// if driver_required is true, then a driver is needed for the map_info (otherwise, this method will tolerate its absence and skip filling the md_entry with driver-supplied information)
// if opt_pubinfo is not NULL, it will be used in lieu of querying the driver or cache for the same information.  This implies driver_required == False
// this is useful for deleting entries, where driver-supplied information is not necessary.
static int AG_populate_md_entry( struct ms_client* ms, struct md_entry* entry, char const* path, struct AG_map_info* mi, struct AG_map_info* parent_mi, struct AG_driver_publish_info* opt_pubinfo, bool driver_required ) {
   
   if( !mi->cache_valid ) {
      return -ESTALE;
   }
   
   int rc = 0;
   
   // sanity check 
   if( driver_required && mi->driver == NULL && opt_pubinfo == NULL ) {
      errorf("No data available for %s\n", path );
      return -EINVAL;
   }
   
   memset( entry, 0, sizeof(struct md_entry) );
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct AG_driver_publish_info pub_info;
   memset( &pub_info, 0, sizeof(pub_info) );
   
   if( opt_pubinfo != NULL ) {
      // use caller-given pubinfo
      memcpy( &pub_info, opt_pubinfo, sizeof(struct AG_driver_publish_info) );
   }
   else {
      
      // use cached or driver-given pubinfo
      struct AG_state* state = AG_get_state();
      if( state == NULL ) {
         return -ENOTCONN;
      }
      
      rc = AG_get_pub_info( state, path, mi, &pub_info );
   
      AG_release_state( state );
      
      if( rc != 0 && (rc != -ENODATA || (rc == -ENODATA && driver_required) ) ) {
         errorf("AG_get_pub_info( %s ) rc = %d\n", path, rc );
         return rc;
      }
      else {
         rc = 0;
      }
   }
   
   char* path_basename = md_basename( path, NULL );
   
   // fill the entry with the driver-given data 
   AG_populate_md_entry_from_publish_info( entry, &pub_info );
   
   // fill in the entry with our cached data 
   AG_populate_md_entry_from_map_info( entry, mi, volume_id, ms->owner_id, ms->gateway_id, path_basename );
   
   free( path_basename );
   
   // don't publish these anyway 
   entry->xattr_nonce = 0;
   entry->error = 0;
   
   // supply parent info 
   entry->parent_name = md_dirname( path, NULL );
   entry->parent_id = parent_mi->file_id;
   
   return rc;
}

// generate all prefixes for a path, including the path itself
static int AG_path_prefixes( char const* path, char*** ret_prefixes ) {
   
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
   return 0;
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
// if merge_new is false, not_merged must not be NULL.  not_merged will contain pointers to all entries in path data that were not merged
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
         
         // make it coherent 
         AG_map_info_make_coherent( old_info, info );
         
         // consumed!
         itr->second = NULL;
         AG_map_info_free( info );
         free( info );
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

// copy a tree's cached data into an AG_fs_map.  Don't copy data that exists in src but not in dest
// neither FS must be locked
int AG_fs_copy_cached_data( struct AG_fs* dest, struct AG_fs* src ) {
   
   AG_fs_rlock( src );
   
   for( AG_fs_map_t::iterator itr = src->fs->begin(); itr != src->fs->end(); itr++ ) {
      
      const string& path_string = itr->first;
      struct AG_map_info* info = itr->second;
      
      if( info->cache_valid ) {
         AG_fs_make_coherent( dest, path_string.c_str(), info->file_id, info->file_version, info->block_version, info->write_nonce, info->refresh_deadline, NULL );
      }
   }
   
   AG_fs_unlock( src );
   
   return 0;
}

// does a map_info have all the cached metadata we need?
bool AG_has_valid_cached_metadata( char const* path, struct AG_map_info* mi ) {
      
   if( !mi->cache_valid ) {
      return false;
   }
   
   return true;
}


// make an ms_path_ent from an AG_map_info 
static int AG_map_info_to_ms_path_ent( struct ms_path_ent* path_ent, struct AG_map_info* mi, uint64_t volume_id ) {
   
   memset( path_ent, 0, sizeof(struct ms_path_ent) );
   
   path_ent->volume_id = volume_id;
   path_ent->file_id = mi->file_id;
   path_ent->version = mi->file_version;
   path_ent->write_nonce = mi->write_nonce;
   
   return 0;
}


// find the deepest known map_info and path, and the shallowest unknown map_info and path in an AG_fs_map_t.
// if all entries have fresh metadata, then *deepest_known_path points to a duplicate of the longest path in path_info, *deepest_known_mi points to the associated map_info, and 
// *shallowest_unknown_mi and *shallowest_unknown_path are both set to NULL.
// *deepest_known_path is always non-NULL, since root is always known.
// NOTE: path_info must contain *only* the prefix set of a path.
static int AG_find_refresh_point( AG_fs_map_t* path_info, char** deepest_known_path, struct AG_map_info** deepest_known_mi, char** shallowest_unknown_path, struct AG_map_info** shallowest_unknown_mi ) {
   
   // assert that root is here 
   AG_fs_map_t::iterator root_itr = path_info->find( string("/") );
   if( root_itr == path_info->end() ) {
      return -EINVAL;
   }
   
   char const* deepest_known_path_const = "/";
   *deepest_known_mi = root_itr->second;
   
   // search in order shallowest to deepest
   vector<string> paths;
   
   for( AG_fs_map_t::iterator itr = path_info->begin(); itr != path_info->end(); itr++ ) {
      
      paths.push_back( itr->first );
   }
   
   // comparator by string length 
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         return (s1.size() < s2.size());
      }
   };
   
   // sort them by increasing length 
   sort( paths.begin(), paths.end(), local_string_length_comparator::comp );
   
   for( unsigned int i = 0; i < paths.size(); i++ ) {
      
      char const* mi_path = paths[i].c_str();
      struct AG_map_info* mi = (*path_info)[ paths[i] ];
      
      if( !AG_has_valid_cached_metadata( mi_path, mi ) ) {
         
         *shallowest_unknown_path = strdup( mi_path );
         *shallowest_unknown_mi = mi;
         
         break;
      }
      else {
         // we have cached data for these 
         deepest_known_path_const = mi_path;
         *deepest_known_mi = mi;
      }
   }
   
   *deepest_known_path = strdup( deepest_known_path_const );
   
   return 0;
}

// validate an ms_response listing against a requested path and its map info.
// we should have gotten back a response that corresponds to the given map_info.
// return 0 if we have a valid listing 
// return 1 if the listing indicates that there has been no remote change on the MS.
// return negative on error
static int AG_validate_MS_response( char const* path, struct AG_map_info* mi, ms_response_t* ms_response ) {
   
   if( ms_response->size() != 1 ) {
      errorf("MS replied with %zu entry listings\n", ms_response->size() );
      return -EBADMSG;
   }
   
   // get the response listing
   uint64_t file_id = ms_response->begin()->first;
   struct ms_listing* listing = &ms_response->begin()->second;
   
   // ignore if absent 
   if( listing->status == MS_LISTING_NONE ) {
      return -ENOENT;
   }
   
   // ignore if unchanged (i.e. we had the right local info).
   else if( listing->status == MS_LISTING_NOCHANGE ) {
      dbprintf("Have fresh cached metadata for %s\n", path );
      return 1;
   }
   
   // the first entry corresponds to this path 
   struct md_entry* ent = &(*listing->entries)[0];
   if( ent == NULL || ent->name == NULL ) {
      // nothing to load
      errorf("MS missing data for entry %s\n", path );
      return -ENODATA;
   }
   
   // sanity check: this must be for us 
   if( mi->file_id != file_id ) {
      errorf("Requested listing for %" PRIX64 "; MS replied with listing for %" PRIX64 "\n", file_id, mi->file_id );
      return -EBADMSG;
   }
   
   // sanity check: types must match 
   if( ent->type != mi->type ) {
      // somehow the type changed 
      errorf("Requested listing for %s the %s; MS replied as if it were a %s\n", path, (mi->type == MD_ENTRY_FILE ? "file" : "directory"), (ent->type == MD_ENTRY_DIR ? "file" : "directory") );
      return -EBADMSG;
   }
      
   char* name = md_basename( path, NULL );

   // sanity check: file names must match 
   if( strcmp(ent->name, name) != 0 ) {
      errorf("Requested listing for '%s'; MS replied with a listing for '%s'\n", ent->name, name );
      
      free( name );
      return -EBADMSG;
   }
   
   free( name );
   
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
   new_info->cache_valid = true;
   
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


// merge data from an MS response listing for a path's shallowest unknown map_info.  This updates the given shallowest unknown map info and makes it coherent, if it isn't NULL.
// return 0 if the response had the requested data.
// return -ENOENT if it did not (indicating that the shallowest unknown map_info shouldn't exist)
// if new_data is not NULL, copy over all newly-discovered map_info data that belongs to this gateway.  Map infos copied to new_data are marked as coherent.
static int AG_accumulate_data_from_ms_response( char const* deepest_known_path, vector<struct md_entry>* deepest_known_entry_listing,
                                                char const* shallowest_unknown_path, struct AG_map_info* shallowest_unknown_map_info,
                                                uint64_t gateway_id, uint64_t volume_id,
                                                AG_fs_map_t* new_data ) {

   bool found = false;
   
   char* shallowest_unknown_name = md_basename( shallowest_unknown_path, NULL );
   
   // find the entry for the shallowest unknown info 
   // NOTE: deepest_known_entry_listing[0] is the deepest known entry's data
   for( unsigned int i = 1; i < deepest_known_entry_listing->size(); i++ ) {
      
      struct md_entry* ent = &deepest_known_entry_listing->at(i);
      
      // ignore entries that we aren't the coordinator of.
      // track directories, however.
      if( (ent->type == MD_ENTRY_FILE && ent->coordinator != gateway_id) || ent->volume != volume_id ) {
         continue;
      }
      
      // match on name--is this the shallowest unknown listing?
      if( strcmp( ent->name, shallowest_unknown_name ) == 0 && shallowest_unknown_map_info != NULL ) {
         
         dbprintf("Reload %s\n", shallowest_unknown_path );
         
         if( new_data ) {
            AG_accumulate_data_from_md_entry( new_data, shallowest_unknown_path, shallowest_unknown_map_info, ent );
         }
         
         // update in-place.  The shallowest unknown map_info is now cache-coherent, and thus known.
         AG_copy_metadata_to_map_info( shallowest_unknown_map_info, ent );
         shallowest_unknown_map_info->cache_valid = true;
         
         dbprintf("%s at %p is now valid\n", shallowest_unknown_path, shallowest_unknown_map_info );
         
         found = true;
      }
      else if( new_data ) {
         
         // what path is this entry at in this directory?
         char* child_path = md_fullpath( deepest_known_path, ent->name, NULL );
         
         AG_accumulate_data_from_md_entry( new_data, child_path, NULL, ent );
         
         dbprintf("New valid data: %s\n", child_path );
         
         free( child_path );
      }
   }
   
   free( shallowest_unknown_name );
   
   if( found ) {
      return 0;
   }
   else {
      return -ENOENT;
   }
}


// walk down a path and ensure that we have metadata for a path of map_infos.
// download the metadata if we don't have it.
// if new_data is not NULL, then new or refreshed map infos will be duplicated and copied to it.
// if explore_last_directory is true, and the path refers to a directory, then this method downloads the directory's children
// NOTE: path_info must contain *exactly* the prefix set of path
static int AG_get_path_metadata( struct ms_client* client, char const* path, AG_fs_map_t* path_info, AG_fs_map_t* new_data, bool explore_last_directory ) {
   
   dbprintf("Get metadata for %s\n", path );
   
   int rc = 0;
   
   // our volume and gateway IDs
   uint64_t volume_id = ms_client_get_volume_id( client );
   uint64_t gateway_id = client->gateway_id;
   bool searching = true;
   bool explored_children = false;
   
   // walk down the path and get metadata for its entries
   while( searching ) {
      
      // find the point in the path where we need to refresh
      struct AG_map_info* deepest_known_map_info = NULL;
      struct AG_map_info* shallowest_unknown_map_info = NULL;
      char* deepest_known_path = NULL;
      char* shallowest_unknown_path = NULL;
      
      rc = AG_find_refresh_point( path_info, &deepest_known_path, &deepest_known_map_info, &shallowest_unknown_path, &shallowest_unknown_map_info );
      if( rc != 0 ) {
         errorf("AG_find_deepest_known_info(%s) rc = %d\n", path, rc );
         break;
      }
      
      if( shallowest_unknown_path == NULL || shallowest_unknown_map_info == NULL ) {
         
         // all metadata on this path is known 
         // do we need to explore the children?
         if( explore_last_directory && deepest_known_map_info->type == MD_ENTRY_DIR && !explored_children ) {
            
            dbprintf("Load children of %s\n", deepest_known_path );
            
            // this is a directory, and we should fetch its children
            shallowest_unknown_path = strdup( deepest_known_path );
            
            // stop searching after this 
            searching = false;
         }
         else {
            dbprintf("All metadata is known for %s\n", path );
            free( deepest_known_path );
            
            break;
         }
      }
      
      dbprintf("Deepest known path of '%s' is '%s'; shallowest unknown path is '%s'\n", path, deepest_known_path, shallowest_unknown_path );
      
      // prepare a request for the deepest known info's children, so we can populate the shallowest unknown info
      ms_path_t ms_requests;
      ms_response_t ms_response;
      struct ms_path_ent path_ent;
      
      AG_map_info_to_ms_path_ent( &path_ent, deepest_known_map_info, volume_id );
      
      // make *sure* we get back data from the MS, instead of MS_LISTING_NOCHANGE
      path_ent.write_nonce = md_random64();
      
      ms_requests.push_back( path_ent );
      
      // get the listing for this path 
      rc = ms_client_get_listings( client, &ms_requests, &ms_response );
      
      if( rc != 0 ) {
         errorf("ms_client_get_listings(%s) rc = %d\n", deepest_known_path, rc );
         
         free(deepest_known_path);
         if( shallowest_unknown_path ) {
            free( shallowest_unknown_path );
         }
         
         ms_client_free_response( &ms_response );
         break;
      }
      
      // make sure this response is for the deepest_known_mi, and that it has data
      rc = AG_validate_MS_response( deepest_known_path, deepest_known_map_info, &ms_response );
      if( rc != 0 ) {
         errorf("AG_validate_MS_response(%s) rc = %d\n", deepest_known_path, rc );
         
         free(deepest_known_path);
         if( shallowest_unknown_path ) {
            free( shallowest_unknown_path );
         }
         
         ms_client_free_response( &ms_response );
         break;
      }
      
      // extract useful info 
      vector<struct md_entry>* deepest_known_entry_listing = ms_response[ deepest_known_map_info->file_id ].entries;
      struct md_entry* deepest_known_entry_metadata = &deepest_known_entry_listing->at(0);
      
      // merge response into deepest known info.  The deepest known info is now cache-coherent.
      AG_copy_metadata_to_map_info( deepest_known_map_info, deepest_known_entry_metadata );
      deepest_known_map_info->cache_valid = true;
      
      dbprintf("%s at %p is now valid\n", deepest_known_path, deepest_known_map_info );
      
      // copy the deepest known path into new_data, if given 
      if( new_data ) {
         AG_accumulate_data_from_md_entry( new_data, deepest_known_path, deepest_known_map_info, deepest_known_entry_metadata );
      }
      
      bool found = false;
      
      // if shallowest_unkown_path and deepest_known_path are the same, then we've found it and reloaded it already
      if( shallowest_unknown_path != NULL && strcmp(shallowest_unknown_path, deepest_known_path) == 0 ) {
         found = true;
         
         // we will have also explored any children of the shallowest unknown path as well (assuming its a directory).
         explored_children = true;
      }
      
      // verify that the MS gave back data for the shallowest unknown path, and get the new data.
      // Mark the shallowest unknown map info as cache-coherent if we got back data for it, as well as any other listings 
      // copied into new_data
      rc = AG_accumulate_data_from_ms_response( deepest_known_path, deepest_known_entry_listing, shallowest_unknown_path, shallowest_unknown_map_info, gateway_id, volume_id, new_data );
      
      ms_client_free_response( &ms_response );
      
      if( !found && rc != 0 ) {
         errorf("AG_accumulate_data_from_ms_response(%s) rc = %d\n", shallowest_unknown_path, rc );
         
         free(deepest_known_path);
         if( shallowest_unknown_path ) {
            free( shallowest_unknown_path );
         }
         
         rc = -ENOENT;
         break;
      }
      
      free(deepest_known_path);
      if( shallowest_unknown_path ) {
         free( shallowest_unknown_path );
      }
      
      rc = 0;
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
   rc = AG_get_path_metadata( ag_fs->ms, path, &path_info, &new_path_info, false );
   if( rc != 0 ) {
      errorf("AG_get_path_metadata(%s) rc = %d\n", path, rc );
      
      AG_fs_map_free( &path_info );
      AG_fs_map_free( &new_path_info );
      return rc;
   }
   
   AG_fs_map_free( &path_info );
   
   AG_fs_map_t not_merged;
   
   // merge the path back in.
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


// look up an AG_map_info from a path 
// AG_fs must not be locked
struct AG_map_info* AG_fs_lookup_path( struct AG_fs* ag_fs, char const* path ) {

   AG_fs_rlock( ag_fs );
   
   // do we have a map_info for this?
   AG_fs_map_t::iterator child_itr = ag_fs->fs->find( string(path) );
   if( child_itr == ag_fs->fs->end() ) {
      
      AG_fs_unlock( ag_fs );
      return NULL;
   }
   
   // send back a copy 
   struct AG_map_info* ret = CALLOC_LIST( struct AG_map_info, 1 );
   
   AG_map_info_dup( ret, child_itr->second );
   
   AG_fs_unlock( ag_fs );
   
   return ret;
}


// set an AG_map_info's cached metadata in-place, making it coherent
int AG_fs_make_coherent( struct AG_fs* ag_fs, char const* path, uint64_t file_id, int64_t file_version, int64_t block_version, int64_t write_nonce, int64_t refresh_deadline, struct AG_map_info* updated_mi ) {
   
   AG_fs_wlock( ag_fs );
   
   // do we have a map_info for this?
   AG_fs_map_t::iterator child_itr = ag_fs->fs->find( string(path) );
   if( child_itr == ag_fs->fs->end() ) {
      
      AG_fs_unlock( ag_fs );
      return -ENOENT;
   }
   
   // update the versions 
   struct AG_map_info* mi = child_itr->second;
   
   mi->file_id = file_id;
   mi->file_version = file_version;
   mi->block_version = block_version;
   mi->write_nonce = write_nonce;
   mi->cache_valid = true;
   mi->refresh_deadline = refresh_deadline;
   
   if( updated_mi != NULL ) {
      AG_map_info_dup( updated_mi, mi );
   }
   
   AG_fs_unlock( ag_fs );
   
   return 0;
}


// remove an AG_map_info
// return -ENOENT if not found.
// NOTE: this doesn't consider directories.  Only do this operation of a corresponding ms_client_delete() succeeded for this path.
int AG_fs_remove( struct AG_fs* ag_fs, char const* path ) {
   
   AG_fs_wlock( ag_fs );
   
   // do we have a map_info for this?
   AG_fs_map_t::iterator child_itr = ag_fs->fs->find( string(path) );
   if( child_itr == ag_fs->fs->end() ) {
      
      AG_fs_unlock( ag_fs );
      return -ENOENT;
   }
   
   ag_fs->fs->erase( child_itr );
   
   AG_fs_unlock( ag_fs );
   
   return 0;
}


static int64_t AG_map_info_make_deadline( int64_t reval_sec ) {
   
   struct timespec ts;
   clock_gettime( CLOCK_MONOTONIC, &ts );
   
   return reval_sec + ts.tv_sec;
}

// publish a (path, map_info) via the driver
// this creates the file/directory, and will fail if it already exists.
// NOTE: the AG must have cached metadata for the parent directory of path!
int AG_fs_publish( struct AG_fs* ag_fs, char const* path, struct AG_map_info* mi ) {
   
   dbprintf("Publish %s in %p\n", path, ag_fs );
   
   struct md_entry entry;
   memset( &entry, 0, sizeof(struct md_entry) );
   
   AG_fs_rlock( ag_fs );
   
   uint64_t volume_id = ms_client_get_volume_id( ag_fs->ms );
   uint64_t owner_id = ag_fs->ms->owner_id;
   uint64_t gateway_id = ag_fs->ms->gateway_id;
   
   AG_fs_unlock( ag_fs );
   
   int rc = 0;
   
   // make sure we have the necessary metadata for the parent, since we'll need the parent's file ID
   char* parent_path = md_dirname( path, NULL );
   rc = AG_fs_refresh_path_metadata( ag_fs, parent_path, false );
   if( rc != 0 ) {
      errorf("AG_fs_refresh_path_metadata(%s) rc = %d\n", parent_path, rc );
      free( parent_path );
      return rc;
   }
   
   // get the map_info for the parent
   struct AG_map_info* parent_info = AG_fs_lookup_path( ag_fs, parent_path );
   if( parent_info == NULL ) {
      errorf("No such entry '%s'\n", parent_path );
      free( parent_path );
      return -ENOENT;
   }
   
   uint64_t parent_id = parent_info->file_id;
   
   AG_map_info_free( parent_info );
   free( parent_info );
   
   // set versioning information in this copy
   mi->file_version = md_random64();
   mi->block_version = md_random64();
   
   // get pubinfo from the driver
   struct AG_driver_publish_info pub_info;
   memset( &pub_info, 0, sizeof(struct AG_driver_publish_info) );
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      free( parent_path );
      
      return -ENOTCONN;
   }
   
   // we're not coherent.  ensure we get the driver-given data 
   mi->cache_valid = false;
   
   rc = AG_get_pub_info( state, path, mi, &pub_info );

   AG_release_state( state );
   
   if( rc != 0 ) {
      free( parent_path );
      
      errorf("AG_get_pub_info( %s ) rc = %d\n", path, rc );
      return rc;
   }
   
   char* mi_name = md_basename( path, NULL );
   
   // fill in the entry with our mi data 
   AG_populate_md_entry_from_map_info( &entry, mi, volume_id, owner_id, gateway_id, mi_name );
   
   // fill the entry with the driver-given data 
   AG_populate_md_entry_from_publish_info( &entry, &pub_info );
   
   // need parent name and ID to create
   entry.parent_id = parent_id;
   entry.parent_name = md_basename( parent_path, NULL );
   
   free( mi_name );
   free( parent_path );
   
   uint64_t new_file_id = 0;
   int64_t write_nonce = 0;
   char const* method = NULL;
   
   // publish to the MS 
   if( entry.type == MD_ENTRY_DIR ) {
      
      method = "ms_client_mkdir";
      rc = ms_client_mkdir( ag_fs->ms, &new_file_id, &write_nonce, &entry );
   }
   else {
      
      method = "ms_client_create";
      rc = ms_client_create( ag_fs->ms, &new_file_id, &write_nonce, &entry );
   }
   
   if( rc != 0 ) {
      errorf("%s(%s) rc = %d\n", method, path, rc );
      
      md_entry_free( &entry );
      return rc;
   }
   
   // cache the new metadata
   AG_fs_make_coherent( ag_fs, path, new_file_id, mi->file_version, mi->block_version, write_nonce, AG_map_info_make_deadline( mi->reval_sec ), NULL );
   
   md_entry_free( &entry );
   return rc;
}


// Publish a whole fs_map of entries.
// Publish them in a breadth-first order, so parent directories get created before files.
// NOTE: to_publish must contain all the parent directories for each file to publish!
// If continue_if_exists is true, then this method will try to publish subsequent entries even if one is found to exist already.
// if an error besides EEXIST is encountered
int AG_fs_publish_map( struct AG_fs* ag_fs, AG_fs_map_t* to_publish, bool continue_if_exists ) {
   
   int rc = 0;

   // publish in order by increasing path length.  This is becaues if to_publish has all of 
   // a file's parent directories, then they are guaranteed to be shorter than the file's path.
   
   // get the list of paths to publish 
   vector<string> paths;
   
   for( AG_fs_map_t::iterator itr = to_publish->begin(); itr != to_publish->end(); itr++ ) {
      
      paths.push_back( itr->first );
   }
   
   // comparator by string length 
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         return (s1.size() < s2.size());
      }
   };
   
   // sort them by increasing length 
   sort( paths.begin(), paths.end(), local_string_length_comparator::comp );
   
   // publish them all 
   for( unsigned int i = 0; i < paths.size(); i++ ) {
      
      char const* path = paths[i].c_str();
      struct AG_map_info* mi = (*to_publish)[paths[i]];
      
      // publish this!
      rc = AG_fs_publish( ag_fs, path, mi );
      if( rc != 0 ) {
         
         errorf("AG_fs_publish(%s) rc = %d\n", path, rc );
         
         if( continue_if_exists ) {
            if( rc != -EEXIST ) {
               // con't continue 
               return rc;
            }
            // CAN continue
         }
         else {
            // can't continue 
            return rc;
         }
      }
   }
   
   return 0;
}


// reversion a (path, map_info) via the driver.
// this updates the version field of the file, and will fail if it doesn't exist.
// optionally use the caller-given pubinfo, or generate new pubinfo from the driver.
int AG_fs_reversion( struct AG_fs* ag_fs, char const* path, struct AG_driver_publish_info* pubinfo ) {
   
   dbprintf("Reversion %s in %p\n", path, ag_fs );
   
   struct md_entry entry;
   memset( &entry, 0, sizeof(struct md_entry) );
   
   int rc = 0;
   
   // make sure the map_info has the requisite metadata from the MS 
   rc = AG_fs_refresh_path_metadata( ag_fs, path, false );
   if( rc != 0 ) {
      errorf("AG_fs_refresh_path_metadata(%s) rc = %d\n", path, rc );
      return rc;
   }
   
   // look up the map_info
   struct AG_map_info* mi = AG_fs_lookup_path( ag_fs, path );
   if( mi == NULL ) {
      errorf("No such entry at '%s'\n", path );
      return -ENOENT;
   }
   
   // old file version 
   int64_t file_version = mi->file_version;
   
   // look up the parent map_info 
   char* parent_path = md_dirname( path, NULL );
   struct AG_map_info* parent_mi = AG_fs_lookup_path( ag_fs, parent_path );
   free( parent_path );
   
   // get entry's revalidation time for reversioning
   int64_t mi_reval_sec = mi->reval_sec;
   
   // populate the entry 
   rc = AG_populate_md_entry( ag_fs->ms, &entry, path, mi, parent_mi, pubinfo, true );
   
   AG_map_info_free( mi );
   AG_map_info_free( parent_mi );
   
   // remember the driver 
   struct AG_driver* mi_driver = mi->driver;
   
   free( mi );
   free( parent_mi );
   
   if( rc != 0 ) {
      errorf("AG_populate_md_entry(%s) rc = %d\n", path, rc );
      
      md_entry_free( &entry );
      return rc;
   }
   
   // generate a new file and block version, randomly 
   entry.version = (int64_t)md_random64();
   int64_t block_version = (int64_t)md_random64();
   int64_t write_nonce = 0;
   
   // update 
   rc = ms_client_update( ag_fs->ms, &write_nonce, &entry );
   
   if( rc != 0 ) {
      errorf("ms_client_update(%s) rc = %d\n", path, rc );
      
      md_entry_free( &entry );
      return rc;
   }
   
   struct AG_map_info reversioned_mi;
   
   // update authoritative copy to keep it coherent
   AG_fs_make_coherent( ag_fs, path, entry.file_id, entry.version, block_version, write_nonce, AG_map_info_make_deadline( mi_reval_sec ), &reversioned_mi );
   
   md_entry_free( &entry );
   
   // evict cached blocks for this file 
   struct AG_state* state = AG_get_state();
   if( state != NULL ) {
      AG_cache_evict_file( state, path, file_version );
      
      AG_release_state( state );
   }
   
   // inform the driver that we reversioned
   rc = AG_driver_reversion( mi_driver, path, &reversioned_mi );
   if( rc != 0 ) {
      errorf("WARN: AG_driver_reversion(%s) rc = %d\n", path, rc );
   }
   
   AG_map_info_free( &reversioned_mi );
   
   return rc;
}


// reversion a whole map of map_infos
// if continue_on_failure is true, then continue to reversion subsequent entries even if reversioning one fails.
// reversion in order of parents to children, so permissions get applied in the right order
int AG_fs_reversion_map( struct AG_fs* ag_fs, AG_fs_map_t* to_reversion, bool continue_on_failure ) {
   
   int rc = 0;
   
   // order paths by length, starting with the shortest.
   vector<string> paths;
   
   for( AG_fs_map_t::iterator itr = to_reversion->begin(); itr != to_reversion->end(); itr++ ) {
      
      paths.push_back( itr->first );
   }
   
   // comparator by string length, longest > shortest
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         return (s1.size() < s2.size());
      }
   };
   
   // sort them by increasing length 
   sort( paths.begin(), paths.end(), local_string_length_comparator::comp );
   
   for( unsigned int i = 0; i < paths.size(); i++ ) {
      
      char const* path = paths[i].c_str();
      
      rc = AG_fs_reversion( ag_fs, path, NULL );
      if( rc != 0 ) {
         
         errorf("AG_fs_reversion(%s) rc = %d\n", path, rc );
         if( !continue_on_failure ) {
            return rc;
         }
      }
   }
   
   return 0;
}


// delete a (path, map_info) via the driver
// if delete_cont is not NULL, then call delete_cont after the entry gets deleted.
// NOTE: the return code from delete_cont will NOT be reported--the caller must handle errors internally
int AG_fs_delete( struct AG_fs* ag_fs, char const* path ) {
   
   dbprintf("Delete %s in %p\n", path, ag_fs );
   
   struct md_entry entry;
   memset( &entry, 0, sizeof(struct md_entry) );
   
   int rc = 0;
   
   // make sure the map_info has the requisite metadata from the MS 
   rc = AG_fs_refresh_path_metadata( ag_fs, path, false );
   if( rc != 0 ) {
      errorf("AG_fs_refresh_path_metadata(%s) rc = %d\n", path, rc );
      return rc;
   }
   
   // look up the map_info
   struct AG_map_info* mi = AG_fs_lookup_path( ag_fs, path );
   if( mi == NULL ) {
      errorf("No such entry at '%s'\n", path );
      return -ENOENT;
   }
   
   // old file version 
   int64_t file_version = mi->file_version;
   
   // look up the parent map_info 
   char* parent_path = md_dirname( path, NULL );
   struct AG_map_info* parent_mi = AG_fs_lookup_path( ag_fs, parent_path );
   free( parent_path );
   
   // populate the entry 
   rc = AG_populate_md_entry( ag_fs->ms, &entry, path, mi, parent_mi, NULL, false );
   
   AG_map_info_free( parent_mi );
   free( parent_mi );
   
   if( rc != 0 ) {
      errorf("AG_populate_md_entry(%s) rc = %d\n", path, rc );
      
      AG_map_info_free( mi );
      free( mi );
      
      md_entry_free( &entry );
      return rc;
   }
   
   // delete the entry 
   rc = ms_client_delete( ag_fs->ms, &entry );
   
   if( rc != 0 ) {
      errorf("ms_client_delete(%s) rc = %d\n", path, rc );
      
      AG_map_info_free( mi );
      free( mi );
      
      md_entry_free( &entry );
      return rc;
   }
   
   // purge it from the FS 
   rc = AG_fs_remove( ag_fs, path );
   if( rc != 0 ) {
      errorf("AG_fs_remove(%s) rc = %d\n", path, rc );
      
      AG_map_info_free( mi );
      free( mi );
      
      md_entry_free( &entry );
      return rc;
   }
   
   // evict cached blocks for this file 
   struct AG_state* state = AG_get_state();
   if( state != NULL ) {
      AG_cache_evict_file( state, path, file_version );
      
      AG_release_state( state );
   }
   
   md_entry_free( &entry );
   
   AG_map_info_free( mi );
   free( mi );
   
   return rc;
}


// delete a whole map of map_infos
// if continue_on_failure is true, then continue to delete subsequent entries even deleting one fails.
// delete in order of children to parents--a reverse breadth-first, if you will
int AG_fs_delete_map( struct AG_fs* ag_fs, AG_fs_map_t* to_delete, bool continue_on_failure ) {
   
   int rc = 0;
   
   // order paths by length, starting with the longest.
   vector<string> paths;
   
   for( AG_fs_map_t::iterator itr = to_delete->begin(); itr != to_delete->end(); itr++ ) {
      
      paths.push_back( itr->first );
   }
   
   // comparator by string length, longest < shortest
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         return (s1.size() > s2.size());
      }
   };
   
   // sort them by decreasing length 
   sort( paths.begin(), paths.end(), local_string_length_comparator::comp );
   
   for( unsigned int i = 0; i < paths.size(); i++ ) {
      
      char const* path = paths[i].c_str();
      
      rc = AG_fs_delete( ag_fs, path );
      if( rc != 0 ) {
         
         errorf("AG_fs_delete(%s) rc = %d\n", path, rc );
         if( !continue_on_failure ) {
            return rc;
         }
      }
   }
   
   return 0;
}


// is a path an immediate child of another?
// return true if child refers to an immediate child in parent; false if not
// parent and child must be normalized paths
bool AG_path_is_immediate_child( char const* parent, char const* child ) {
   
   char* child_parent = md_dirname( child, NULL );
   
   bool ret = (strcmp( child_parent, parent ) == 0);
   
   free( child_parent );
   
   return ret;
}


// find the set of entries that already exist on the MS.
// exisitng_fs must be empty.
int AG_download_existing_fs_map( struct ms_client* ms, AG_fs_map_t** ret_existing_fs, bool fail_fast ) {
   
   dbprintf("%s", "Begin downloading\n");
   
   int rc = 0;
   vector<string> frontier;
   
   struct AG_map_info* root = CALLOC_LIST( struct AG_map_info, 1 );
   
   rc = AG_map_info_get_root( ms, root );
   if( rc != 0 ) {
      errorf("AG_map_info_get_root rc = %d\n", rc );
      
      free( root );
      
      dbprintf("End downloading (failure, rc = %d)\n", rc);
      return rc;
   }
   
   AG_fs_map_t* existing_fs = new AG_fs_map_t();
   
   (*existing_fs)[ string("/") ] = root;
   
   // find the root and invalidate it 
   rc = AG_invalidate_path_metadata( existing_fs, "/" );
   if( rc != 0 ) {
      errorf("AG_invalidate_path_metadata(/) rc = %d\n", rc );
      
      delete existing_fs;
      dbprintf("End downloading (failure, rc = %d)\n", rc);
      return rc;
   }
   
   frontier.push_back( string("/") );
   
   while( frontier.size() > 0 ) {
      
      // next directory 
      string dir_path_s = frontier.front();
      frontier.erase( frontier.begin() );
      
      char const* dir_path = dir_path_s.c_str();
      
      dbprintf("Explore '%s'\n", dir_path );
         
      // next directory's listing 
      AG_fs_map_t dir_listing;
      
      // newly-discovered data 
      AG_fs_map_t new_info;
      
      // copy this path in 
      rc = AG_fs_map_clone_path( existing_fs, dir_path, &dir_listing );
      if( rc != 0 ) {
         errorf("AG_fs_map_clone_path(%s) rc = %d\n", dir_path, rc );
         break;
      }
      
      // read this directory, and its immediate children
      rc = AG_get_path_metadata( ms, dir_path, &dir_listing, &new_info, true );
      if( rc != 0 ) {
         errorf("AG_get_path_metadata(%s) rc = %d\n", dir_path, rc );
         
         AG_fs_map_free( &dir_listing );
         break;
      }
      
      // find unexplored info, and invalidate directories
      for( AG_fs_map_t::iterator itr = new_info.begin(); itr != new_info.end(); itr++ ) {
         
         char const* child_path = itr->first.c_str();
         struct AG_map_info* mi = itr->second;
         
         // ignore non-immediate childs of the current directory, and ignore root
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
      
      AG_fs_map_free( &dir_listing );
      
      // merge discovered data back in 
      rc = AG_fs_map_merge_tree( existing_fs, &new_info, true, NULL );
      
      if( rc != 0 ) {
         errorf("AG_fs_map_merge_tree(%s) rc = %d\n", dir_path, rc );
         break;
      }
   }
   
   if( rc == 0 ) {
      
      *ret_existing_fs = existing_fs;
      
      dbprintf("Downloaded file mapping %p:\n", existing_fs);
      AG_dump_fs_map( existing_fs );
      
      dbprintf("%s", "End downloading (success)\n");
   }
   else {

      AG_fs_map_free( existing_fs );
      delete existing_fs;
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
   
   // comparator by string length, shortest < longest
   struct local_string_length_comparator {
      
      static bool comp( const string& s1, const string& s2 ) {
         if( s1.size() != s2.size() ) {
            return (s1.size() < s2.size());
         }
         else {
            return s1 < s2;
         }
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
