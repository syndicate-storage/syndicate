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

#include "publish.h"
#include "core.h"
#include "cache.h"
#include "driver.h"
#include "map-info.h"

typedef bool (*AG_build_request_filter_t)( char const*, struct AG_map_info*, void* );
typedef int (*AG_make_request_func_t)( struct ms_client*, struct md_entry*, struct AG_map_info* mi, struct ms_client_request* );  
typedef int (*AG_error_handler_t)( struct ms_client_request*, struct ms_client_request_result* );

// fill an unpublished fs_map with MS metadata and AG metadata, for subsequent publishing.
// entries in to_publish that are marked as coherent will be skipped.
// return 0 on success
// return negative on error
int AG_fs_publish_generate_metadata( AG_fs_map_t* to_publish ) {
   
   int rc = 0;
   map< string, int > child_counts;
   
   rc = AG_fs_count_children( to_publish, &child_counts );
   if( rc != 0 ) {
      errorf("AG_fs_count_children rc = %d\n", rc );
      return rc;
   }
      
   for( AG_fs_map_t::iterator itr = to_publish->begin(); itr != to_publish->end(); itr++ ) {
      
      if( itr->second->cache_valid ) {
         // this entry already has fresh metadata
         continue;
      }
      
      uint64_t file_id = ms_client_make_file_id();
      int64_t file_version = md_random64();
      int64_t write_nonce = md_random64();
      int64_t block_version = md_random64();
      uint64_t num_children = child_counts[ itr->first ];
      int64_t generation = -1;
      int64_t capacity = 1L << (int64_t)(log2( (double)(num_children + 1) ) + 1);
      int64_t refresh_deadline = AG_map_info_make_deadline( itr->second->reval_sec );
      
      AG_map_info_make_coherent_with_MS_data( itr->second, file_id, file_version, write_nonce, num_children, generation, capacity );
      AG_map_info_make_coherent_with_AG_data( itr->second, block_version, refresh_deadline );
   }
   
   return 0;
}


// make a create request.  We'll need to fill in a file ID 
// NOTE: the mi must have the requisite MS metadata filled in (such as from AG_fs_publish_generate_metadata)
static int AG_make_create_async_request( struct ms_client* client, struct md_entry* ent, struct AG_map_info* mi, struct ms_client_request* req ) {
   
   ent->file_id = mi->file_id;
   ent->version = mi->file_version;
   ent->write_nonce = mi->write_nonce;
   
   int rc = ms_client_create_async_request( client, ent, req );
   
   return rc;
}


// make a mkdir request.  We'll need to fill in a file ID 
// NOTE: the mi must have the requisite MS metadata filled in (such as from AG_fs_publish_generate_metadata)
static int AG_make_mkdir_request( struct ms_client* client, struct md_entry* ent, struct AG_map_info* mi, struct ms_client_request* req ) {
   
   ent->file_id = mi->file_id;
   ent->version = mi->file_version;
   ent->write_nonce = mi->write_nonce;
   
   int rc = ms_client_mkdir_request( client, ent, req );
   
   return rc;
}


// make an update async request.
static int AG_make_update_async_request( struct ms_client* client, struct md_entry* ent, struct AG_map_info* mi, struct ms_client_request* req ) {
   
   return ms_client_update_async_request( client, ent, req );
}

// make a delete request 
static int AG_make_delete_request( struct ms_client* client, struct md_entry* ent, struct AG_map_info* mi, struct ms_client_request* req ) {
   
   return ms_client_delete_request( client, ent, req );
}


// filter (AG_build_request_filter_t) for selecting directories at a particular depth.
// cls points to an int that is the depth.
static bool AG_filter_dir_requests_at_depth( char const* path, struct AG_map_info* mi, void* cls ) {
   
   int depth = *(int*)cls;
   
   if( mi->type != MD_ENTRY_DIR ) {
      return false;
   }
   
   if( md_depth( path ) != depth ) {
      return false;
   }
   
   return true;
}


// filter (AG_build_request_filter_t) for selecting files at a particular depth 
// cls points to an int that is the depth
static bool AG_filter_file_requests_at_depth( char const* path, struct AG_map_info* mi, void* cls ) {
   
   int depth = *(int*)cls;
   
   if( mi->type != MD_ENTRY_FILE ) {
      return false;
   }
   
   if( md_depth( path ) != depth ) {
      return false;
   }
   
   return true;
}


// free a request, and the data the AG gave it 
static int AG_request_free( struct ms_client_request* request ) {
   
   if( request->ent ) {
      
      md_entry_free( request->ent );
      free( request->ent );
      request->ent = NULL;
   }
   
   if( request->cls != NULL ) {
      free( request->cls );       // will be the path
      request->cls = NULL;
   }
   
   return 0;
}


// free a list of requests 
static int AG_requests_free_all( struct ms_client_request* requests, size_t num_requests ) {
   
   for( unsigned int i = 0; i < num_requests; i++ ) {
      AG_request_free( &requests[i] );
   }
   
   free( requests );
   return 0;
}


// create a sequence of MS requests for a list of path entries, for the ones for which the given filter is true.
// associate with each request its absolute path, so we can merge the result back in.
// map_infos contains the data we know, and each element must have both the driver-given and MS-given metadata.
// request_infos contains the data to send to the MS, and each element must include at least the driver-given metadata.
// on success, *ret_requests will point to an array of *ret_num_requests requests
static int AG_build_requests( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* request_infos,
                              AG_build_request_filter_t filter, void* filter_cls, AG_make_request_func_t make_request, struct ms_client_request** ret_requests, size_t* ret_num_requests ) {
   
   int rc = 0;
   vector< struct ms_client_request > requests;
   
   for( AG_fs_map_t::iterator itr = request_infos->begin(); itr != request_infos->end(); itr++ ) {
      
      char const* path = itr->first.c_str();
      struct AG_map_info* mi = itr->second;
      char* parent_path = NULL;
      struct AG_map_info* parent_mi = NULL;
      AG_fs_map_t::iterator parent_itr;
      bool include = true;
      struct md_entry* ent = NULL;
      struct ms_client_request req;
      
      // filter?
      if( filter != NULL ) {
         include = (*filter)( path, mi, filter_cls );
      }
      
      if( !include ) {
         continue;
      }
      
      // find parent 
      parent_path = md_dirname( path, NULL );
      parent_itr = map_infos->find( string( parent_path ) );
      
      if( parent_itr == map_infos->end() ) {
         // incomplete 
         errorf("ERR: not found: '%s'\n", parent_path );
         
         free( parent_path );
         rc = -EINVAL;
         break;
      }
      
      free( parent_path );
      parent_mi = parent_itr->second;
      
      // make request 
      ent = CALLOC_LIST( struct md_entry, 1 );
      
      // populate from the basics 
      AG_populate_md_entry( client, ent, path, mi, parent_mi, AG_POPULATE_NO_DRIVER, &mi->pubinfo );
      
      memset( &req, 0, sizeof(struct ms_client_request) );
      (*make_request)( client, ent, mi, &req );
      
      // remember the path 
      ms_client_request_set_cls( &req, strdup(path) );
      
      requests.push_back( req );
   }
   
   if( rc == 0 ) {
      
      // success
      if( requests.size() > 0 ) {
         *ret_requests = CALLOC_LIST( struct ms_client_request, requests.size() );
      
         std::copy( requests.begin(), requests.end(), *ret_requests );
      }
      else {
         *ret_requests = NULL;
      }
      
      *ret_num_requests = requests.size();
   }
   else {
      
      // failure.  free memory 
      for( unsigned int i = 0; i < requests.size(); i++ ) {
         AG_request_free( &requests[i] );
      }
   }
   
   return rc;
}


// build a list of mkdir requests at a particular depth 
static int AG_build_mkdir_requests_at_depth( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* request_infos, int depth, struct ms_client_request** ret_requests, size_t* ret_num_requests ) {
   return AG_build_requests( client, map_infos, request_infos, AG_filter_dir_requests_at_depth, &depth, AG_make_mkdir_request, ret_requests, ret_num_requests );
}

// build a list of create_async requests at a particular depth
static int AG_build_create_async_requests_at_depth( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* request_infos, int depth, struct ms_client_request** ret_requests, size_t* ret_num_requests ) {
   return AG_build_requests( client, map_infos, request_infos, AG_filter_file_requests_at_depth, &depth, AG_make_create_async_request, ret_requests, ret_num_requests );
}

// build a list of update-async requests at a particular depth 
static int AG_build_update_async_requests_at_depth( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* request_infos, int depth, struct ms_client_request** ret_requests, size_t* ret_num_requests ) {
   return AG_build_requests( client, map_infos, request_infos, AG_filter_file_requests_at_depth, &depth, AG_make_update_async_request, ret_requests, ret_num_requests );
}

// build a list of unlink requests at a particular depth 
static int AG_build_delete_requests_at_depth( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* request_infos, int depth, struct ms_client_request** ret_requests, size_t* ret_num_requests ) {
   return AG_build_requests( client, map_infos, request_infos, AG_filter_file_requests_at_depth, &depth, AG_make_delete_request, ret_requests, ret_num_requests );
}

// build a list of unlink requests at a particular depth 
static int AG_build_rmdir_requests_at_depth( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* request_infos, int depth, struct ms_client_request** ret_requests, size_t* ret_num_requests ) {
   return AG_build_requests( client, map_infos, request_infos, AG_filter_dir_requests_at_depth, &depth, AG_make_delete_request, ret_requests, ret_num_requests );
}

// ignore -EEXIST on create
static int AG_create_error_handler( struct ms_client_request* request, struct ms_client_request_result* result ) {
   
   if( request->op == ms::ms_update::CREATE || request->op == ms::ms_update::CREATE_ASYNC ) {
      
      if( result->rc == -EEXIST ) {
         return 0;
      }
   }
   
   return result->rc;
}

// ignore -ENOENT on delete 
static int AG_delete_error_handler( struct ms_client_request* request, struct ms_client_request_result* result ) {
   
   if( request->op == ms::ms_update::DELETE || request->op == ms::ms_update::DELETE_ASYNC ) {
      
      if( result->rc == -ENOENT ) {
         return 0;
      }
   }
   
   return result->rc;
}

// run a batch of requests, all-or-nothing
// if we succeed, merge them into the given fs_map dest (all-or-nothing)
// return 0 if they all succeed.
// return nonzero if at least one failed 
static int AG_run_requests( struct ms_client* client, AG_fs_map_t* dest, struct ms_client_request* requests, size_t num_requests, AG_error_handler_t error_handler ) {
  
   int rc = 0;
   struct ms_client_request_result* results = NULL; 
   
   results = CALLOC_LIST( struct ms_client_request_result, num_requests );
   
   rc = ms_client_run_requests( client, requests, results, num_requests );
   
   if( rc == 0 ) {
      
      // no protocol errors.  Check for MS errors.
      for( unsigned int i = 0; i < num_requests; i++ ) {
         
         char* path = (char*)requests[i].cls;
         int ms_rc = results[i].rc;
         
         if( ms_rc != 0 && error_handler != NULL ) {
            
            // handle error 
            ms_rc = (*error_handler)( &requests[i], &results[i] );
         }
         
         if( ms_rc != 0 ) {
            
            // failed to handle error
            errorf("MS request %d on %s failed, rc = %d\n", requests[i].op, path, ms_rc );
            rc = ms_rc;
            break;
         }
      }
      
      if( rc == 0 ) {
         
         // no MS errors. Merge 
         for( unsigned int i = 0; i < num_requests; i++ ) {
            
            char* path = (char*)requests[i].cls;
            
            // sanity check--we did get a response, right?
            if( MS_CLIENT_OP_RETURNS_ENTRY( requests[i].op ) && results[i].ent == NULL ) {
               // shouldn't happen 
               errorf("Expected metadata in response to request %d (op %d on %s): operation rc = %d, request rc = %d\n", i, requests[i].op, path, results[i].rc, results[i].reply_error );
               rc = -ENODATA;
               break;
            }
            
            // do we have a response?
            if( results[i].ent != NULL ) {
               
               struct AG_map_info* mi = NULL;
               AG_fs_map_t::iterator itr;
               struct md_entry* ent = results[i].ent;
               
               itr = dest->find( string(path) );
               
               if( itr == dest->end() ) {
                  // this shouldn't happen--we earlier generated a request on this entry 
                  errorf("BUG: not found: %s\n", path );
                  rc = -EINVAL;
                  break;
               }
               
               mi = itr->second;
               
               // reload MS data
               AG_map_info_make_coherent_with_MS_data( mi, ent->file_id, ent->version, ent->write_nonce, ent->num_children, ent->generation, ent->capacity );
            }
         }
      }
   }
   
   ms_client_request_result_free_all( results, num_requests );
   
   return rc;
}


// shallow-copy entries from one AG_fs_map_t to another that are of the same depth.
// NOTE: duplicates are overwritten!
static int AG_fs_find_entries_at_depth( AG_fs_map_t* dest, AG_fs_map_t* source, int depth ) {
   
   for( AG_fs_map_t::iterator itr = source->begin(); itr != source->end(); itr++ ) {
      
      char const* path = itr->first.c_str();
      
      if( md_depth(path) != depth ) {
         continue;
      }
      
      (*dest)[ itr->first ] = itr->second;
   }
   
   return 0;
}

// Publish an fs_map of entries to the MS (to_publish).
// Each entry in to_publish needs to have its driver-given metadata.  It does not need MS metadata--that will be obtained.
// map_infos must contain the parents of everything in to_publish
// put the resulting MS metadata into to_publish on success.
int AG_fs_publish_all( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* to_publish ) {
   
   int depth = 0;
   int rc = 0;
   struct ms_client_request* requests = NULL;
   size_t num_requests = 0;
   int max_depth = AG_max_depth( to_publish );
   
   // have map_infos reference the parents of all entries for which we have data
   AG_fs_map_t parents;
   
   for( AG_fs_map_t::iterator itr = map_infos->begin(); itr != map_infos->end(); itr++ ) {
      parents[ itr->first ] = itr->second;
   }
   
   for( depth = 0; depth <= max_depth; depth++ ) {
      
      // make directory requests
      rc = AG_build_mkdir_requests_at_depth( client, &parents, to_publish, depth, &requests, &num_requests );
      if( rc != 0 ) {
         
         errorf("AG_build_mkdir_requests_at_depth(%d) rc = %d\n", depth, rc );
         break;
      }
      
      if( num_requests > 0 ) {
         // run directory requests 
         rc = AG_run_requests( client, to_publish, requests, num_requests, AG_create_error_handler );
         
         AG_requests_free_all( requests, num_requests );
         
         if( rc != 0 ) {
            
            errorf("ms_client_run_requests(mkdir, %d) rc = %d\n", depth, rc );
            break;
         }
      }
      
      // make file requests 
      rc = AG_build_create_async_requests_at_depth( client, &parents, to_publish, depth, &requests, &num_requests );
      if( rc != 0 ) {
         
         errorf("AG_build_create_async_requests_at_depth(%d) rc = %d\n", depth, rc );
         break;
      }
         
      if( num_requests > 0 ) {
         // run file requests 
         rc = AG_run_requests( client, to_publish, requests, num_requests, AG_create_error_handler );
      
         AG_requests_free_all( requests, num_requests );
         
         if( rc != 0 ) { 
            
            errorf("ms_client_run_requests(create_async, %d) rc = %d\n", depth, rc );
            break;
         }
      }
      
      // merge mkdir requests to parents, so we can look them up later when building the next generation of requests.
      AG_fs_find_entries_at_depth( &parents, to_publish, depth );
   }
   
   return rc;
}


// Update an fs_map of entries to the MS (to_update).
// Each entry in to_update needs to have its driver-given metadata and its MS-given metadata
// put the resulting MS metadata into to_update on success.
int AG_fs_update_all( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* to_update ) {
   
   int depth = 0;
   int rc = 0;
   struct ms_client_request* requests = NULL;
   size_t num_requests = 0;
   int max_depth = AG_max_depth( to_update );
   
   for( depth = 0; depth <= max_depth; depth++ ) {
      
      // make update requests
      rc = AG_build_update_async_requests_at_depth( client, map_infos, to_update, depth, &requests, &num_requests );
      if( rc != 0 ) {
         
         errorf("AG_build_update_async_requests_at_depth(%d) rc = %d\n", depth, rc );
         break;
      }
      
      if( num_requests == 0 ) {
         // we're done 
         continue;
      }
      
      // run directory requests 
      rc = AG_run_requests( client, to_update, requests, num_requests, NULL );
      
      AG_requests_free_all( requests, num_requests );
      
      if( rc != 0 ) {
         
         errorf("ms_client_run_requests(update_async, %d) rc = %d\n", depth, rc );
         break;
      }
   }
   
   return rc;
}


// Delete directories, retrying them if the fail with -ENOTEMPTY 
// return 0 on success
// return negative on error (-ENOTEMPTY if we failed even after retries)
static int AG_fs_delete_directories_at_depth( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* to_delete, int depth ) {

   int rc = 0;
   struct ms_client_request* requests = NULL;
   struct ms_client_request_result* results = NULL; 
   size_t num_requests = 0;             // number of current requests in the pipeline
   size_t num_alloced_requests = 0;     // size of the request buffer
   int num_retries = 0;
   int retry_head = 0;
   int attempt = 0;
   
   // make directory requests
   rc = AG_build_rmdir_requests_at_depth( client, map_infos, to_delete, depth, &requests, &num_requests );
   if( rc != 0 ) {
      
      errorf("AG_build_delete_requests_at_depth(%d) rc = %d\n", depth, rc );
      return rc;
   }
   
   num_alloced_requests = num_requests;
   
   // run directory requests, retrying them if they fail with -ENOTEMPTY
   for( attempt = 0; attempt < AG_REQUEST_MAX_RETRIES; attempt++ ) {
      
      // run requests and get results
      results = CALLOC_LIST( struct ms_client_request_result, num_requests );
      
      rc = ms_client_run_requests( client, requests, results, num_requests );
      
      if( rc != 0 ) {
         
         // network or protocol error 
         errorf("ms_client_run_requests(%d) rc = %d\n", depth, rc );
         break;
      }
      
      // which requests failed?
      // try them again, and compactify the request list
      num_retries = 0;
      retry_head = -1;
      
      for( unsigned int i = 0; i < num_requests; i++ ) {
         
         char* path = (char*)requests[i].cls;            
         
         if( results[i].rc == 0 ) {
            
            // this one succeeded
            AG_request_free( &requests[i] );
            memset( &requests[i], 0, sizeof(struct ms_client_request) );
            
            if( retry_head == -1 ) {
               
               // copy next ENOTEMPTY request here.
               retry_head = i;
            }
         }
         else if( results[i].rc == -ENOTEMPTY ) {
            
            // failed, but should be retried 
            if( retry_head >= 0 ) {
               
               // compactify--move this request to an empty request slot
               requests[retry_head] = requests[i];
               memset( &requests[i], 0, sizeof(struct ms_client_request) );
               
               // next insertion point 
               for( unsigned int j = retry_head; j <= i; j++ ) {
                  if( requests[i].op == 0 ) {
                     
                     // next vacant slot to which to copy the next ENOTEMPTY
                     retry_head = j;
                     break;
                  }
               }
            }
            
            num_retries++;
         }
         else if( results[i].rc != -ENOENT ) {
            
            // some other fatal error, which means we won't be able to proceed 
            errorf("ms_client_rmdir(%s) rc = %d\n", path, rc);
            break;
         }
      }
      
      ms_client_request_result_free_all( results, num_requests );
      
      num_requests = num_retries;
   }
   
   if( num_retries > 0 && attempt == AG_REQUEST_MAX_RETRIES ) {
      // failed 
      rc = -ENOTEMPTY;
   }
   
   AG_requests_free_all( requests, num_alloced_requests );
   
   return rc;
}


// Delete an fs_map of entries from the MS (to_delete).
// Each entry in to_delete needs to have its MS-given metadata.
// Retry directory deletes if they fail with -ENOTEMPTY, since the MS might just be catching up with us.
// As they are deleted from the MS, entries will be removed from to_delete.
int AG_fs_delete_all( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* to_delete ) {
   
   int depth = 0;
   int rc = 0;
   struct ms_client_request* requests = NULL; 
   size_t num_requests = 0;
   int max_depth = AG_max_depth( to_delete );
   
   for( depth = max_depth; depth >= 0; depth-- ) {
      
      // make file requests 
      rc = AG_build_delete_requests_at_depth( client, map_infos, to_delete, depth, &requests, &num_requests );
      if( rc != 0 ) {
         
         errorf("AG_build_delete_requests_at_depth(%d) rc = %d\n", depth, rc );
         break;
      }
      
      if( num_requests == 0 ) {
         // we're done with this depth
         continue;
      }
         
      // run file requests 
      rc = AG_run_requests( client, to_delete, requests, num_requests, AG_delete_error_handler );
      
      AG_requests_free_all( requests, num_requests );
      
      if( rc != 0 ) { 
         
         errorf("ms_client_run_requests(delete files, %d) rc = %d\n", depth, rc );
         break;
      }
      
      // remove directories from to_delete 
      rc = AG_fs_delete_directories_at_depth( client, map_infos, to_delete, depth );
      
      if( rc != 0 ) {
         
         errorf("AG_fs_delete_directories_at_depth(%d) rc = %d\n", depth, rc );
         break;
      }
   }
   
   return rc;
}

// publish a path in the MS, using the given path and driver publish info.
// NOTE: pubinfo must be given
int AG_fs_publish( struct AG_fs* ag_fs, char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pubinfo ) {
   
   dbprintf("Publish %s in %p\n", path, ag_fs );
   
   struct md_entry entry;
   memset( &entry, 0, sizeof(struct md_entry) );
   
   int rc = 0;
   
   // look up the parent map_info 
   char* parent_path = md_dirname( path, NULL );
   struct AG_map_info* parent_mi = AG_fs_lookup_path( ag_fs, parent_path );
   free( parent_path );
   
   if( parent_mi == NULL ) {
      errorf("No such parent entry at '%s'\n", parent_path );
      
      return -ENOENT;
   }
   
   rc = AG_populate_md_entry( ag_fs->ms, &entry, path, mi, parent_mi, 0, pubinfo );
   
   AG_map_info_free( parent_mi );
   
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
   uint64_t file_id = ms_client_make_file_id();
   
   // create 
   rc = ms_client_create( ag_fs->ms, &file_id, &write_nonce, &entry );
   
   if( rc != 0 ) {
      errorf("ms_client_create(%s) rc = %d\n", path, rc );
      
      md_entry_free( &entry );
      return rc;
   }
   
   struct AG_map_info tmp;
   memset( &tmp, 0, sizeof(struct AG_map_info) );
   
   AG_copy_metadata_to_map_info( &tmp, &entry );
   AG_map_info_make_coherent_with_AG_data( &tmp, block_version, AG_map_info_make_deadline( mi->reval_sec ) );
   
   // update authoritative copy to keep it coherent
   AG_fs_make_coherent( ag_fs, path, &tmp, mi );
   
   // insert it 
   AG_fs_map_insert( ag_fs, path, mi );
   
   // evict cached blocks for this file 
   struct AG_state* state = AG_get_state();
   if( state != NULL ) {
      AG_cache_evict_file( state, path, entry.version );
      
      AG_release_state( state );
   }
   
   md_entry_free( &entry );
   
   return rc;
}

// reversion a (path, map_info) via the driver.
// this updates the version field of the file, and will fail if it doesn't exist (either locally or on the MS)
// optionally use the caller-given opt_pubinfo, or generate new pubinfo from the driver.
// AG_fs must not be locked
int AG_fs_reversion( struct AG_fs* ag_fs, char const* path, struct AG_driver_publish_info* opt_pubinfo ) {
   
   dbprintf("Reversion %s in %p\n", path, ag_fs );
   
   struct md_entry entry;
   memset( &entry, 0, sizeof(struct md_entry) );
   
   int rc = 0;
   
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
   
   if( parent_mi == NULL ) {
      errorf("No such parent entry at '%s'\n", parent_path );
      
      AG_map_info_free( mi );
      free( mi );
      return -ENOENT;
   }
   
   // get entry's revalidation time for reversioning
   int64_t mi_reval_sec = mi->reval_sec;
   
   // populate the entry 
   int reversion_flags = 0;
   
   if( opt_pubinfo != NULL ) {
      // use caller-given publish data 
      reversion_flags = AG_POPULATE_NO_DRIVER;
   }
   
   rc = AG_populate_md_entry( ag_fs->ms, &entry, path, mi, parent_mi, reversion_flags, opt_pubinfo );
   
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
   memset( &reversioned_mi, 0, sizeof(struct AG_map_info) );
   
   struct AG_map_info tmp;
   memset( &tmp, 0, sizeof(struct AG_map_info) );
   
   AG_copy_metadata_to_map_info( &tmp, &entry );
   AG_map_info_make_coherent_with_AG_data( &tmp, block_version, AG_map_info_make_deadline( mi_reval_sec ) );
   
   // update authoritative copy to keep it coherent
   AG_fs_make_coherent( ag_fs, path, &tmp, &reversioned_mi );
   
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


// delete a path in the MS 
int AG_fs_delete( struct AG_fs* ag_fs, char const* path ) {
   
   dbprintf("Delete %s in %p\n", path, ag_fs );
   
   struct md_entry entry;
   memset( &entry, 0, sizeof(struct md_entry) );
   
   int rc = 0;
   
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
   
   if( parent_mi == NULL ) {
      errorf("No such parent entry at '%s'\n", parent_path );
      
      AG_map_info_free( mi );
      free( mi );
      return -ENOENT;
   }
   
   rc = AG_populate_md_entry( ag_fs->ms, &entry, path, mi, parent_mi, AG_POPULATE_SKIP_DRIVER_INFO, NULL );
   
   AG_map_info_free( mi );
   AG_map_info_free( parent_mi );
   
   if( rc != 0 ) {
      errorf("AG_populate_md_entry(%s) rc = %d\n", path, rc );
      
      md_entry_free( &entry );
      return rc;
   }
   
   // create 
   rc = ms_client_delete( ag_fs->ms, &entry );
   
   if( rc != 0 ) {
      errorf("ms_client_delete(%s) rc = %d\n", path, rc );
      
      md_entry_free( &entry );
      return rc;
   }
   
   // remove from fs 
   rc = AG_fs_map_remove( ag_fs, path, &mi );
   if( mi != NULL ) {
      
      AG_map_info_free( mi );
      free( mi );
      mi = NULL;
   }
   
   md_entry_free( &entry );
   
   // evict cached blocks for this file 
   struct AG_state* state = AG_get_state();
   if( state != NULL ) {
      AG_cache_evict_file( state, path, file_version );
      
      AG_release_state( state );
   }
   
   return rc;
}
