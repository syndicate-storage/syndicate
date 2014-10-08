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

// initialize a stage
// NOTE: this consumes request--stage becomes its owner
// NOTE: this consume parents--stage becomes its owner
int AG_request_stage_init( struct AG_request_stage* stage, int depth, int file_oper, int dir_oper, AG_request_list_t* file_reqs, AG_request_list_t* dir_reqs, AG_request_parent_map_t* parents, bool dirs_first ) {
   
   memset( stage, 0, sizeof(struct AG_request_stage) );
   
   stage->depth = depth;
   
   stage->file_oper = file_oper;
   stage->dir_oper = dir_oper;
   
   stage->file_reqs = file_reqs;
   stage->dir_reqs = dir_reqs;
   
   stage->dirs_first = dirs_first;
   
   stage->results = NULL;
   
   stage->parents = parents;
   
   return 0;
}

// clear a stage's results 
static int AG_request_stage_clear_results( struct AG_request_stage* stage ) {
   
   if( stage->results != NULL ) {
      ms_client_multi_result_free( stage->results );
      free( stage->results );
      stage->results = NULL;
   }

   return 0;
}


// free a stage
int AG_request_stage_free( struct AG_request_stage* stage ) {
   
   AG_request_list_t** reqs_to_free[] = {
      &stage->file_reqs,
      &stage->dir_reqs,
      NULL
   };
   
   for( int i = 0; reqs_to_free[i] != NULL; i++ ) {
      
      AG_request_list_t* reqs = *(reqs_to_free[i]);
      
      if( reqs != NULL ) {
         
         for( unsigned int j = 0; j < reqs->size(); j++ ) {
            
            ms_client_request_free( &reqs->at(j) );
         }
         
         reqs->clear();
         
         delete reqs;
         
         *(reqs_to_free[i]) = NULL;
      }
   }
   
   AG_request_stage_clear_results( stage );
   
   if( stage->parents ) {
      for( AG_request_parent_map_t::iterator itr = stage->parents->begin(); itr != stage->parents->end(); itr++ ) {
         
         if( itr->second != NULL ) {
            free( itr->second );
         }
      }
      
      stage->parents->clear();
      delete stage->parents;
      stage->parents = NULL;
   }
   
   return 0;
}


// free a list of stages
int AG_request_stage_list_free( AG_request_stage_list_t* stages ) {
   
   for( unsigned int i = 0; i < stages->size(); i++ ) {
      
      AG_request_stage_free( stages->at(i) );
      free( stages->at(i) );
      
   }
   
   stages->clear();
   return 0;
}

// find and remove all entries from sorted_paths of the same depth, and put them into directory_list
// sorted_paths must be sorted by increasing path depth.
static int AG_pop_dirs_by_depth( vector<string>* sorted_paths, vector<string>* directory_list ) {
   
   if( sorted_paths->size() == 0 ) {
      return -EINVAL;
   }
   
   int depth = 0;
   
   // what's the depth of the head?
   depth = md_depth( sorted_paths->at(0).c_str() );
   
   directory_list->push_back( sorted_paths->at(0) );
   
   if( sorted_paths->size() > 1 ) {
      // find all directories in sorted_paths that are have $depth.
      // they will all be at the head.
      for( unsigned int i = 1; i < sorted_paths->size(); i++ ) {
         
         int next_depth = md_depth( sorted_paths->at(i).c_str() );
         if( next_depth == depth ) {
            
            directory_list->push_back( sorted_paths->at(i) );
         }
         else {
            break;
         }
      }
   }
   
   // pop all pushed paths 
   sorted_paths->erase( sorted_paths->begin(), sorted_paths->begin() + directory_list->size() );
   
   return 0;
}


// find and remove all entries from sorted_paths that share the same parent, and put them into directory_list
// sorted_paths must be sorted alphanumerically
static int AG_pop_dirs_by_parent( vector<string>* sorted_paths, vector<string>* directory_list ) {
   
   if( sorted_paths->size() == 0 ) {
      return -EINVAL;
   }
   
   char* parent_dir = md_dirname( sorted_paths->at(0).c_str(), NULL );
   
   directory_list->push_back( sorted_paths->at(0) );
   
   if( sorted_paths->size() > 0 ) {
      // find all directories in sorted paths that have the parent directory $parent_dir 
      // they will all be at the head.
      for( unsigned int i = 1; i < sorted_paths->size(); i++ ) {
         
         if( AG_path_is_immediate_child( parent_dir, sorted_paths->at(i).c_str() ) ) {
            
            directory_list->push_back( sorted_paths->at(i) );
         }
         else {
            break;
         }
      }
   }
   
   free( parent_dir );
   
   // pop all pushed paths 
   sorted_paths->erase( sorted_paths->begin(), sorted_paths->begin() + directory_list->size() );
   
   return 0;
}

// given a list of paths that are all children of the same directory, and the operation to be performed with them, generate a stage containing the request.
// use the operation to perform sanity checks and to fill out the request properly.
// reference and directives contain the metadata for the paths in listing_paths.  They may overlap; go with the reference by default, but use directives instead of AG_REQUEST_USE_DIRECTIVES is set in flags.
// get metadata from the driver if AG_REQUEST_USE_DRIVER is set in flags.  Otherwise, use metadata already in reference/directives (regardless of whether or not it is coherent).
// NOTE: listing_paths will get *consumed* by this method!
static int AG_generate_stage( struct ms_client* client, int depth, vector<string>* listing_paths, AG_fs_map_t* directives, AG_fs_map_t* reference, int file_op, int dir_op, struct AG_request_stage* stage, int flags ) {
   
   if( listing_paths->size() == 0 ) {
      // nothing to do 
      return 0;
   }
   
   // group by parent directory--sort alphanumerically
   sort( listing_paths->begin(), listing_paths->end(), less<string>() );
   
   int rc = 0;
   uint64_t volume_id = ms_client_get_volume_id( client );
   
   AG_request_list_t* file_requests = new AG_request_list_t();
   AG_request_list_t* dir_requests = new AG_request_list_t();
   AG_request_parent_map_t* parents = new AG_request_parent_map_t();

   while( listing_paths->size() > 0 ) {
      
      // get the next batch that share a common parent 
      vector<string> next_batch;
      
      rc = AG_pop_dirs_by_parent( listing_paths, &next_batch );
      if( rc != 0 ) {
         errorf("AG_pop_dirs_by_parent rc = %d\n", rc );
         break;
      }
      
      // what's the parent?
      char* dirpath = md_dirname( next_batch[0].c_str(), NULL );
      
      // find the parent of the entries we'll generate 
      AG_fs_map_t::iterator parent_itr;
      AG_fs_map_t::iterator parent_itr_reference = reference->find( string(dirpath) );
      AG_fs_map_t::iterator parent_itr_directive = directives->find( string(dirpath) );
      
      if( parent_itr_reference == reference->end() && parent_itr_directive == directives->end() ) {
         
         errorf("Parent not found: %s\n", dirpath );
         free( stage );
         free( dirpath );
         rc = -ENOENT;
         break;
      }
      else if( parent_itr_reference != reference->end() && parent_itr_directive != directives->end() ) {
         
         // parent available in both reference and directives.
         if( (flags & AG_REQUEST_USE_DIRECTIVES) != 0 ) {
            
            parent_itr = parent_itr_directive;
         }
         else {
            
            parent_itr = parent_itr_reference;
         }
      }
      else if( parent_itr_reference != reference->end() ) {
         
         parent_itr = parent_itr_reference;
      }
      else {
         
         parent_itr = parent_itr_directive;
      }
      
      // parent data 
      struct AG_map_info* parent_mi = parent_itr->second;
      
      // process all children of the parent
      for( unsigned int i = 0; i < next_batch.size(); i++ ) {
         
         // find the map info as given in the directives map
         AG_fs_map_t::iterator itr = directives->find( next_batch.at(i) );
         if( itr == directives->end() ) {
            
            // not found; can't continue
            errorf("ERR: Not found in directives: %s\n", next_batch.at(i).c_str() );
            rc = -ENOENT;
            break;
         }
         
         // this child...
         char const* mi_path = itr->first.c_str();
         char* mi_name = md_basename( mi_path, NULL );
         
         struct AG_map_info* mi = itr->second;
         
         // entry to generate from the child
         struct md_entry ent;
         memset( &ent, 0, sizeof(struct md_entry) );
         
         if( flags & (AG_REQUEST_USE_DRIVER) ) {
            
            // get info from the driver for this entry
            struct AG_driver_publish_info pub_info;
            memset( &pub_info, 0, sizeof(struct AG_driver_publish_info) );
            
            // get driver info 
            rc = AG_get_publish_info( mi_path, mi, &pub_info );
            if( rc != 0 ) {
               errorf("AG_get_publish_info(%s) rc = %d\n", mi_path, rc );
               free( mi_name );
               break;
            }
            
            // fill the entry with the driver-given data 
            AG_populate_md_entry_from_publish_info( &ent, &pub_info );
         }
         
         // fill in the entry with our AG-specific data 
         AG_populate_md_entry_from_AG_info( &ent, mi, volume_id, client->owner_id, client->gateway_id, mi_name );
         free( mi_name );
         
         // fill in the entry with the data we'll send to the MS 
         AG_populate_md_entry_from_cached_MS_info( &ent, mi->file_id, mi->file_version, mi->write_nonce );
         
         // fill in parent information
         if( parent_mi != NULL ) {
            ent.parent_id = parent_mi->file_id;
            ent.parent_name = md_basename( dirpath, NULL );
         }
         
         // populate this request
         struct ms_client_request req;
         memset( &req, 0, sizeof(struct ms_client_request) );
         
         req.ent = CALLOC_LIST( struct md_entry, 1 );
         if( req.ent == NULL ) {
            rc = -ENOMEM;
            break;
         }
         
         *req.ent = ent;
         
         if( ent.type == MD_ENTRY_FILE ) {
            file_requests->push_back( req );
         }
         else if( ent.type == MD_ENTRY_DIR ) {
            dir_requests->push_back( req );
         }
         else {
            errorf("unknown type %d in entry %s (%" PRIX64 ")\n", ent.type, ent.name, ent.file_id );
            rc = -EINVAL;
            break;
         }
         
         // remember the parent!
         (*parents)[ req.ent->file_id ] = strdup(dirpath);
      }
      
      free( dirpath );
      
      if( rc != 0 ) {
         break;
      }
   }
   
   if( rc != 0 ) {
      
      // had an error.  revert and clean up
      for( unsigned int i = 0; i < file_requests->size(); i++ ) {
         
         ms_client_request_free( &file_requests->at(i) );
      }
      
      for( unsigned int i = 0; i < dir_requests->size(); i++ ) {
         
         ms_client_request_free( &dir_requests->at(i) );
      }
      
      for( AG_request_parent_map_t::iterator itr = parents->begin(); itr != parents->end(); itr++ ) {
         
         if( itr->second != NULL ) {
            free( itr->second );
         }
      }
      
      file_requests->clear();
      dir_requests->clear();
      parents->clear();
      
      delete file_requests;
      delete dir_requests;
      delete parents;
      
      return rc;
   }
   
   // set up the stage
   AG_request_stage_init( stage, depth, file_op, dir_op, file_requests, dir_requests, parents, (flags & AG_REQUEST_DIRS_FIRST) != 0 );
   
   return 0;
}


// dump stages to stdout, for debugging purposes 
static int AG_dump_stages( AG_request_stage_list_t* stages ) {
   
   dbprintf("Begin request stages %p\n", stages );
   for( unsigned int i = 0; i < stages->size(); i++ ) {
      
      struct AG_request_stage* stage = stages->at(i);
      
      dbprintf("   Stage %d, dirs_first = %d\n", stage->depth, stage->dirs_first );
      dbprintf("      Directories (operation=%d, count=%zu):\n", stage->dir_oper, stage->dir_reqs->size());
      
      for( unsigned int j = 0; j < stage->dir_reqs->size(); j++ ) {
         dbprintf("       %" PRIX64 " name=%s, parent=%" PRIX64 "\n", stage->dir_reqs->at(j).ent->file_id, stage->dir_reqs->at(j).ent->name, stage->dir_reqs->at(j).ent->parent_id );
      }
      
      dbprintf("      Files (operation=%d, count=%zu):\n", stage->file_oper, stage->file_reqs->size());
      
      for( unsigned int j = 0; j < stage->file_reqs->size(); j++ ) {
         dbprintf("       %" PRIX64 " name=%s, parent=%" PRIX64 "\n", stage->file_reqs->at(j).ent->file_id, stage->file_reqs->at(j).ent->name, stage->file_reqs->at(j).ent->parent_id );
      }
   }
   dbprintf("End request stages %p\n", stages );
   return 0;
}


// Given a list of paths and their associated data in reference, generate request stages that when evaluated step by step, will perform the given operation on the MS for the collection of entries they name.
// The request stage list is generated from the root of the fs, where files and directories are grouped by depth.
// The resulting request stage list is in pre-order traversal, so [root, (root's child directories, root's child files), (root's grandchild directories, root's grandchild files), ...]
static int AG_generate_stages( struct ms_client* client, AG_fs_map_t* directives, AG_fs_map_t* reference, int file_op, int dir_op, int stage_flags, AG_request_stage_list_t* stages ) {
   
   int rc = 0;
   int depth = 0;
   int expected_depth = 0;      // for checking for correctness
   vector<string> paths;        // set of paths in directives
   
   // sanity check 
   if( directives->size() == 0 ) {
      // empty list
      return 0;
   }
   
   for( AG_fs_map_t::iterator itr = directives->begin(); itr != directives->end(); itr++ ) {
      paths.push_back( itr->first );
   }
   
   // put the FS paths into breadth-first order.
   struct local_path_comparator {
      
      static bool comp_breadth_first( const string& s1, const string& s2 ) {
         // s1 comes before s2 if s1 is shallower than s2
         int depth_1 = md_depth( s1.c_str() );
         int depth_2 = md_depth( s2.c_str() );
         
         return depth_1 < depth_2;
      }
   };
   
   // breadth-first order
   sort( paths.begin(), paths.end(), local_path_comparator::comp_breadth_first );
   
   expected_depth = md_depth( paths.at(0).c_str() );
   
   while( paths.size() > 0 ) {
      
      vector<string> directory_list;
      struct AG_request_stage* stage = CALLOC_LIST( struct AG_request_stage, 1 );
      
      // get the next group of files and directories that are at the same depth
      AG_pop_dirs_by_depth( &paths, &directory_list );
      
      // group by parent directory: sort alphanumerically
      sort( directory_list.begin(), directory_list.end(), less<string>() );
      
      
      ////////////////////////////////////////////////////
      dbprintf("Stage at %d will have %zu items\n", expected_depth, directory_list.size() );
      
      /*
      for( unsigned int i = 0; i < directory_list.size(); i++ ) {
         dbprintf("   '%s'\n", directory_list[i].c_str() );
      }
      ////////////////////////////////////////////////////
      */
      
      // sanity check: what's the current depth?
      depth = md_depth( directory_list.at(0).c_str() );
      
      // sanity check: each iteration of this loop should consume all directories at a given depth, in increasing order
      if( depth != expected_depth ) {
         errorf("BUG: depth of %s is %d, but expected %d\n", directory_list.at(0).c_str(), depth, expected_depth );
         rc = -EINVAL;
         break;
      }
      
      // generate a stae for this depth 
      rc = AG_generate_stage( client, depth, &directory_list, directives, reference, file_op, dir_op, stage, stage_flags );
      
      if( rc != 0 ) {
         errorf("AG_generate_stage(depth=%d) rc = %d\n", depth, rc );
         free( stage );
         break;
      }
      
      stages->push_back( stage );
      
      expected_depth++;
   }
   
   if( rc != 0 ) {
      // clean up 
      AG_request_stage_list_free( stages );
   }
   /*
   else {
      AG_dump_stages( stages );
   }
   */
   return rc;
}


// generate a stage list that will create everything in the given to_publish mapping
// directories will be created by the MS synchronously.
// files will be created by the MS asynchronously.
// when building the stage list, get parent data from the directives over the reference if there's a conflict, since the parent doesn't exist
static int AG_generate_stages_create( struct ms_client* client, AG_fs_map_t* to_publish, AG_fs_map_t* reference, AG_request_stage_list_t* stages ) {
   return AG_generate_stages( client, to_publish, reference, ms::ms_update::CREATE_ASYNC, ms::ms_update::CREATE, AG_REQUEST_DIRS_FIRST | AG_REQUEST_USE_DIRECTIVES, stages );
}


// generate a stage list that will update everything in the given to_update mapping
// all updates will be processed by the MS asynchronously.
// when building the stage list, get parent data from the reference over the directives if there's a conflict, since the parent should already exist.
static int AG_generate_stages_update( struct ms_client* client, AG_fs_map_t* to_update, AG_fs_map_t* reference, AG_request_stage_list_t* stages ) {
   return AG_generate_stages( client, to_update, reference, ms::ms_update::UPDATE_ASYNC, ms::ms_update::UPDATE_ASYNC, AG_REQUEST_DIRS_FIRST | AG_REQUEST_USE_DRIVER, stages );
}


// generate a stage list that will delete everything in the given to_delete mapping 
// files and directories will be deleted synchronously.
static int AG_generate_stages_delete( struct ms_client* client, AG_fs_map_t* to_delete, AG_fs_map_t* reference, AG_request_stage_list_t* stages ) {
   
   int rc = AG_generate_stages( client, to_delete, reference, ms::ms_update::DELETE, ms::ms_update::DELETE, 0, stages );
   if( rc != 0 ) {
      errorf("AG_generate_stages(delete) rc = %d\n", rc );
      return rc;
   }
   
   // reverse the stages, since we have to delete in post-traversal order 
   reverse( stages->begin(), stages->end() );
   return 0;
}


// free up a batch request's data 
static int AG_batch_request_free( struct AG_batch_request* request ) {
   
   if( request->nctx != NULL ) {
      ms_client_network_context_free( request->nctx );
      free( request->nctx );
      request->nctx = NULL;
   }
   
   memset( request, 0, sizeof(struct AG_batch_request) );
   return 0;
}

// cancel a single batch request 
static int AG_batch_request_cancel( struct ms_client* client, struct md_download_set* dlset, struct AG_batch_request* request ) {
   
   struct ms_client_network_context* nctx = request->nctx;
   
   if( nctx == NULL ) {
      return 0;
   }
   
   // stop tracking
   if( dlset != NULL ) {
      md_download_set_clear( dlset, nctx->dlctx );  
   }
   
   // destroy the context 
   int rc = ms_client_multi_cancel( client, nctx );
   if( rc != 0 ) {
      // shouldn't happen
      errorf("BUG: ms_client_multi_cancel(%p) rc = %d\n", nctx->dlctx, rc );
   }
   
   return rc;
}

// clean up the network state of a list of batch requests
static int AG_batch_requests_cancel_and_free( struct ms_client* client, struct md_download_set* dlset, struct AG_batch_request* requests, int num_contexts ) {
   
   // failed to set up.  clean up
   for( int i = 0; i < num_contexts; i++ ) {
      
      AG_batch_request_cancel( client, dlset, &requests[i] );
      
      AG_batch_request_free( &requests[i] );
   }
   
   return 0;
}

// clean up all batches 
static int AG_batch_request_free_all( struct AG_batch_request* batches, int num_batches ) {
   
   for( int i = 0; i < num_batches; i++ ) {
      
      AG_batch_request_free( &batches[i] );
   }
   
   return 0;
}

// restart a single batch of operations 
// allocate the network context, if needed.
// return 0 on success, negative on error
static int AG_restart_operation( struct ms_client* client, struct AG_batch_request* batch, int oper, int flags, struct ms_client_request* reqs, int num_reqs, struct md_download_set* dlset ) {
   
   // sanity check
   if( batch->nctx != NULL && batch->nctx->started ) {
      return -EINVAL;
   }
   
   if( batch->nctx == NULL ) {
      
      batch->nctx = CALLOC_LIST( struct ms_client_network_context, 1 );
      
      if( batch->nctx == NULL ) {
         return -ENOMEM;
      }
   }
   
   // set up the request 
   batch->reqs = reqs;
   batch->num_reqs = num_reqs;
   batch->retries ++;
   
   dbprintf("(Re)start operations (oper=%d) for %p (%d requests)\n", oper, reqs, num_reqs );
   
   int rc = ms_client_multi_begin( client, oper, flags, reqs, num_reqs, batch->nctx, dlset );
   if( rc != 0 ) {
      errorf("ms_client_multi_begin(%p) rc = %d\n", batch->nctx->dlctx, rc );
   }
   
   return rc;
}

// start up to num_connections, with up to max_batch operations.
// track each batch's network context with the download_set
// return the number started, or negative on error.
// If this method fails, you should call AG_batch_request_free_all() on the batches to clean up
static int AG_start_operations( struct ms_client* client, struct AG_batch_request* batches, int num_connections, int oper, int flags, int max_batch,
                                AG_request_list_t* reqs, int req_offset,
                                struct md_download_set* dlset, int* ret_rc ) {
   
   int offset = 0;      // offset from req_offset in reqs where the next requests will be inserted for processing
   int rc = 0;
   int opened_connections = 0;
   
   // start batch operations 
   for( int i = 0; i < num_connections && (unsigned)(req_offset + offset) < reqs->size(); i++ ) {
      
      // find a free connection 
      if( batches[i].nctx != NULL && batches[i].nctx->started ) {
         continue;
      }
      
      size_t num_reqs = MIN( (unsigned)max_batch, reqs->size() - (req_offset + offset) );
      if( num_reqs == 0 ) {
         break;
      }
      
      // NOTE: vectors are guaranteed by C++ to be contiguous in memory
      struct ms_client_request* msreqs = &reqs->at( req_offset + offset );
      
      // set up the batch request 
      batches[i].retries = 0;
      
      // start the batch request
      rc = AG_restart_operation( client, &batches[i], oper, flags, msreqs, num_reqs, dlset );
      if( rc != 0 ) {
         errorf("AG_start_operation(%p) rc = %d\n", batches[i].nctx->dlctx, rc );
         break;
      }
      
      // next batch...
      offset += num_reqs;
      opened_connections++;
   }
   
   if( rc != 0 ) {
      *ret_rc = rc;
   }
   
   dbprintf("Opened %d connections for %d more requests (starting at %d)\n", opened_connections, offset, req_offset );
   
   return offset;
}


// finish an operation on a stage.  Return 0 on success; return -ENOMEM if out of memory; return an MS-given error if the requests failed.
static int AG_finish_operation( struct ms_client* client, struct AG_batch_request* req, struct AG_request_stage* stage ) {
   
   if( req->nctx == NULL ) {
      return -EINVAL;   
   }
   
   int rc = 0;
   struct ms_client_multi_result results;
   
   memset( &results, 0, sizeof(struct ms_client_multi_result) );
   
   // finish and get results
   rc = ms_client_multi_end( client, &results, req->nctx );
   if( rc != 0 ) {
      errorf("ms_client_multi_end(%p (stage=%d)) rc = %d\n", req->nctx->dlctx, stage->depth, rc );
      return rc;
   }
   
   dbprintf("Stage %d: got back %d results, %zu entries, reply_error = %d\n", stage->depth, results.num_processed, results.num_ents, results.reply_error );
   
   // make sure we have results
   if( stage->results == NULL ) {
      stage->results = CALLOC_LIST( struct ms_client_multi_result, 1 );  
      if( stage->results == NULL ) {
         return -ENOMEM;
      }
   }
   
   if( results.reply_error == 0 ) {
      // merge results into the stage's results
      ms_client_multi_result_merge( stage->results, &results );
   }
   else {
      // just remember the error 
      stage->results->reply_error = results.reply_error;
      ms_client_multi_result_free( &results );
   }
   
   dbprintf("Stage %d: %d results total (%zu entries)\n", stage->depth, stage->results->num_processed, stage->results->num_ents );
   
   return results.reply_error;
}


// run a list of requests for a stage, but don't open more than max_connections connections and don't send more than max_batch operations
// retry operations if they fail with -EAGAIN, up to client->conf->max_metadata_write_retry times.
// return 0 on success
// return negative on failure (i.e. due to persistent network failure, or MS operational failure)
// if an MS operational failure code is NOT listed in tolerated_operational_errors, this method fails fast on the first error encountered.
// otherwise, MS operational errors in tolerated_operational_errors will be masked (but will be recorded in stage->results->reply_error)
// if this method fails to process a request, stage->error will be set to the returned error code, and stage->failed_reqs will be set to the request list that was being procesed.
static int AG_run_stage_requests( struct ms_client* client, struct AG_request_stage* stage, bool dirs, int max_connections, int max_batch, AG_operational_error_set_t* tolerated_operational_errors ) {
   
   int rc = 0;
   int offset = 0;              // offset into reqs for unstarted requests
   int started = 0;             // number of requests started
   
   AG_request_list_t* reqs = NULL;
   int oper = 0;
   
   if( dirs ) {
      reqs = stage->dir_reqs;
      oper = stage->dir_oper;
   }
   else {
      reqs = stage->file_reqs;
      oper = stage->file_oper;
   }
   
   // sanity check 
   if( reqs->size() == 0 ) {
      // done!
      return 0;
   }
   
   // batch requests 
   struct AG_batch_request* batches = CALLOC_LIST( struct AG_batch_request, max_connections );
   if( batches == NULL ) {
      return -ENOMEM;
   }
   
   // download set for the batches 
   struct md_download_set dlset;
   md_download_set_init( &dlset );
   
   // start batch operations 
   started = AG_start_operations( client, batches, max_connections, oper, stage->flags, max_batch, reqs, 0, &dlset, &rc );
   
   if( rc != 0 ) {
      // failed to set up.  clean up
      AG_batch_request_free_all( batches, started );
      
      free( batches );
      md_download_set_free( &dlset );
      
      return rc;
   }
   
   dbprintf("Stage %d: started %d reqests\n", stage->depth, started );
   
   offset += started;
   
   // reap connections; start next operations
   while( true ) {
      
      // wait for directory operations to finish
      rc = md_download_context_wait_any( &dlset, 10000 );
      if( rc != 0 && rc != -ETIMEDOUT ) {
         
         // failed
         break;
      }
      
      else if( rc == -ETIMEDOUT ) {
         continue;
      }
      
      set<int> finished;
      int num_running = 0;
      
      // which operation(s) finished?
      for( int i = 0; i < max_connections; i++ ) {
         
         if( batches[i].nctx == NULL ) {
            // not in use
            continue;
         }
         
         struct md_download_context* dlctx = batches[i].nctx->dlctx;
         
         if( dlctx == NULL ) {
            // download not active
            continue;
         }
         
         // not done yet?
         if( !md_download_context_finalized( dlctx ) ) {
            
            num_running ++;
            continue;
         }
         
         // finished!  process it and keep its data around 
         rc = AG_finish_operation( client, &batches[i], stage );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // connetion timed out.  Try these requests again 
               if( batches[i].retries <= client->conf->max_metadata_write_retry ) {
                  
                  dbprintf("Retry batch %p (%zu requests)\n", batches[i].reqs, batches[i].num_reqs );
                  
                  // retry this request
                  rc = AG_restart_operation( client, &batches[i], oper, stage->flags, batches[i].reqs, batches[i].num_reqs, &dlset );
                  if( rc != 0 ) {
                     errorf("AG_restart_operation(%p stage=%d) rc = %d\n", batches[i].nctx->dlctx, stage->depth, rc );
                  }
                  else {
                     
                     // this one is running
                     num_running++;
                  }
               }
            }
            
            // mask this error?
            else if( tolerated_operational_errors != NULL && tolerated_operational_errors->count( rc ) > 0 ) {
               
               // this isn't considered to be an error by the caller 
               rc = 0;
               finished.insert(i);
            }
            
            if( rc != 0 ) {
               // Otherwise, remember where ths stage failed 
               stage->error = rc;
               
               if( dirs ) {
                  stage->failed_reqs = stage->dir_reqs;
               }
               else {
                  stage->failed_reqs = stage->file_reqs;
               }
               
               break;
            }
         }
         else {
            
            // mark as finished 
            finished.insert( i );
         }
         
         // start more downloads, if there are more left 
         if( offset < (signed)reqs->size() ) {
            
            started = AG_start_operations( client, batches, max_connections, oper, stage->flags, max_batch, reqs, offset, &dlset, &rc );
            if( rc != 0 ) {
               
               // failed to start 
               break;
            }
            else {
               
               int num_results = 0;
               if( stage->results != NULL ) {
                  num_results = stage->results->num_processed;
               }
               
               offset += started;
               
               dbprintf("Stage %d: started %d more requests (%d started, %d results, %zu total)\n", stage->depth, started, offset, num_results, reqs->size() );
               
               // discount all now-running batches from our finished set
               for( int j = 0; j < max_connections; j++ ) {
                  
                  if( batches[j].nctx != NULL && batches[j].nctx->started ) {
                     if( finished.count(j) > 0 ) {
                        finished.erase(j);
                        num_running++;
                     }
                  }
               }
            }
         }
      }
      
      if( rc != 0 ) {
         // fatal error 
         break;
      }
      
      // remove finished downloads from the download set
      for( set<int>::iterator itr = finished.begin(); itr != finished.end(); itr++ ) {
         
         int i = *itr;
         
         if( batches[i].nctx != NULL ) {
            md_download_set_clear( &dlset, batches[i].nctx->dlctx );
         }
      }
      
      // if there are no more downloads running, then we're done
      if( num_running == 0 ) {
         break;
      }
   }
   
   if( rc != 0 ) {
      // clean up on error from the operations loop
      AG_batch_requests_cancel_and_free( client, &dlset, batches, max_connections );
   }
   
   md_download_set_free( &dlset );
   
   AG_batch_request_free_all( batches, max_connections );
   free( batches );
   return rc;
}


// run a single stage
// return 0 on success, negative on error.
static int AG_run_stage( struct ms_client* client, struct AG_request_stage* stage, int max_connections, int max_batch, int max_async_batch, AG_operational_error_set_t* tolerated_operational_errors ) {
   
   int rc = 0;
   
   const bool* is_dir_opers = NULL;
   char const** req_names = NULL;
   const int* opers = NULL;
   
   if( stage->dirs_first ) {
      
      const bool d_is_dir_opers[] = {
         true,
         false
      };
      
      char const* d_req_names[] = {
         "dirs",
         "files"
      };
      
      const int d_opers[] = {
         stage->dir_oper,
         stage->file_oper
      };
      
      is_dir_opers = d_is_dir_opers;
      req_names = d_req_names;
      opers = d_opers;
   }
   else {
      
      const bool f_is_dir_opers[] = {
         false,
         true
      };
      
      char const* f_req_names[] = {
         "files",
         "dirs"
      };
      
      const int f_opers[] = {
         stage->file_oper,
         stage->dir_oper
      };
      
      is_dir_opers = f_is_dir_opers;
      req_names = f_req_names;
      opers = f_opers;
   }
   
   for( int i = 0; i < 2; i++ ) {
      
      // how many requests per connection?
      int num_requests_per_connection = 0;
      
      if( ms_client_is_async_operation( opers[i] ) ) {
         
         num_requests_per_connection = max_async_batch;
      }
      else {
         
         num_requests_per_connection = max_batch;
      }
      
      if( is_dir_opers[i] ) {
         dbprintf("Stage %d: Run %zu requests on %s (batch size = %d)\n", stage->depth, stage->dir_reqs->size(), req_names[i], num_requests_per_connection );
      }
      else {
         dbprintf("Stage %d: Run %zu requests on %s (batch size = %d)\n", stage->depth, stage->file_reqs->size(), req_names[i], num_requests_per_connection );
      }
      
      rc = AG_run_stage_requests( client, stage, is_dir_opers[i], max_connections, num_requests_per_connection, tolerated_operational_errors );
      if( rc != 0 ) {
         errorf("AG_run_stage_requests(%s, stage=%d) rc = %d\n", req_names[i], stage->depth, rc );
         return rc;
      }
   }
   
   return 0;
}


// verify that the MS response has exactly the replies we expected 
// return 0 on success 
// return -ENODATA if we're missing a reply 
// return -EBADMSG if we got a gratuitous reply
static int AG_validate_stage_MS_response_replies( struct AG_request_stage* stage, AG_request_list_t* reqs ) {

   int rc = 0;
   struct ms_client_multi_result* results = stage->results;
   
   if( (signed)(stage->dir_reqs->size() + stage->file_reqs->size()) != results->num_processed ) {
      return -EINVAL;
   }
   
   // make sure all entries we requested are present in the reply 
   // map file ID to requests and replies
   map<uint64_t, struct md_entry*> ms_replies;
   map<uint64_t, struct md_entry*> ag_requests;
   
   for( unsigned int i = 0; i < reqs->size(); i++ ) {
      
      struct ms_client_request* req = &reqs->at(i);
      struct md_entry* ent = &results->ents[i];
      
      ag_requests[req->ent->file_id] = req->ent;
      ms_replies[ent->file_id] = ent;
   }
   
   // check for missing replies
   for( unsigned int i = 0; i < reqs->size(); i++ ) {
      
      struct ms_client_request* req = &reqs->at(i);
      
      if( ms_replies.count( req->ent->file_id ) == 0 ) {
         errorf("Missing MS reply for %" PRIX64 " (%s)\n", req->ent->file_id, req->ent->name );
         rc = -ENODATA;
      }
   }
   
   if( rc != 0 ) {
      return rc;
   }
   
   // check for gratuitous replies 
   for( unsigned int i = 0; i < reqs->size(); i++ ) {
      
      struct md_entry* ent = &results->ents[i];
      
      if( ag_requests.count( ent->file_id ) == 0 ) {
         errorf("Gratuitous MS reply for %" PRIX64 " (%s)\n", ent->file_id, ent->name );
         rc = -EBADMSG;
      }
   }
   
   return rc;
}


// validate the MS response against the requests we made 
static int AG_validate_stage_MS_response( struct AG_request_stage* stage ) {
   
   int rc = 0;
   
   AG_request_list_t* file_reqs = stage->file_reqs;
   AG_request_list_t* dir_reqs = stage->dir_reqs;
   struct ms_client_multi_result* results = stage->results;
   
   if( results == NULL ) {
      errorf("No data received for stage %d\n", stage->depth );
      return -ENODATA;
   }
   
   AG_request_list_t* reqs_list[] = {
      dir_reqs,
      file_reqs,
      NULL
   };
   
   int reqs_types[] = {
      MD_ENTRY_DIR,
      MD_ENTRY_FILE,
      -1
   };
   
   // even if we got no data back, the MS should have processed all our entries
   if( (signed)(dir_reqs->size() + file_reqs->size()) != results->num_processed ) {
      errorf("Sent %zu requests, but the MS processed %d\n", dir_reqs->size() + file_reqs->size(), results->num_processed );
      return -EREMOTEIO;
   }
   
   // were we even expecting results to be sent back?
   int num_expected_dirs = ms_client_num_expected_reply_ents( dir_reqs->size(), stage->dir_oper );
   int num_expected_files = ms_client_num_expected_reply_ents( file_reqs->size(), stage->file_oper );
   
   int num_expected_replies[] = {
      num_expected_dirs,
      num_expected_files,
      -1
   };
   
   for( int j = 0; reqs_list[j] != NULL; j++ ) {
      
      // each request list...
      AG_request_list_t* reqs = reqs_list[j];
      
      if( num_expected_replies[j] == 0 ) {
         continue;
      }
      
      if( num_expected_replies[j] != (signed)reqs->size() ) {
         errorf("Expected %d replies for type %d, but got %zu\n", num_expected_replies[j], reqs_types[j], reqs->size() );
         return -EBADMSG;
      }
      
      rc = AG_validate_stage_MS_response_replies( stage, reqs );
      if( rc != 0 ) {
         errorf("AG_validate_stage_MS_response_replies(stage=%d) rc = %d\n", stage->depth, rc );
         break;
      }
   }
   
   return rc;
}


// given a request stage list, walk down it and send the updates to the MS 
// return 0 on succes, negative on failure.
static int AG_run_stages( struct ms_client* client, AG_request_stage_list_t* stages, int max_connections, int max_batch, int max_async_batch ) {
   
   int rc = 0;
   
   for( unsigned int i = 0; i < stages->size(); i++ ) {
      
      struct AG_request_stage* stage = stages->at(i);
      
      dbprintf("Running stage %d\n", stage->depth );
      
      rc = AG_run_stage( client, stage, max_connections, max_batch, max_async_batch, NULL );
      if( rc != 0 ) {
      
         // this stage failed
         errorf("AG_run_stage(depth=%d) rc = %d\n", stage->depth, rc );
         break;
      }
      
      
      dbprintf("Validating MS replies for stage %d\n", stage->depth );
      
      rc = AG_validate_stage_MS_response( stage );
      if( rc != 0 ) {
      
         // this stage fialed 
         errorf("AG_validate_stage_MS_response(depth=%d) rc = %d\n", stage->depth, rc );
         break;
      }
   }
   
   return rc;
}



// add a stage's worth of data into an fs_map (overwriting duplicates)
// NOTE: no validation occurs on the MS-given stage results (use AG_validate_stage_MS_response for that) beyond ensuring that we have N replies for N total requests.
// mi_reference will be used as a whitelist of entries to add, and will be used to ensure that the MS didn't 
static int AG_fs_add_stage_data( AG_fs_map_t* dest, AG_fs_map_t* mi_reference, struct AG_request_stage* stage ) {
   
   int rc = 0;
   
   AG_request_list_t* file_reqs = stage->file_reqs;
   AG_request_list_t* dir_reqs = stage->dir_reqs;
   struct ms_client_multi_result* results = stage->results;
   
   if( results == NULL ) {
      errorf("No data received for stage %d\n", stage->depth );
      return -ENODATA;
   }
   
   int num_expected_file_ents = ms_client_num_expected_reply_ents( file_reqs->size(), stage->file_oper );
   int num_expected_dir_ents = ms_client_num_expected_reply_ents( dir_reqs->size(), stage->dir_oper );
   
   // did all creates process?
   if( (unsigned)(num_expected_file_ents + num_expected_dir_ents) != results->num_ents ) {
      
      errorf("Not all operations succeeded: expected (%d files, %d directories); got %zu replies\n", num_expected_file_ents, num_expected_dir_ents, results->num_ents );
      return -EINVAL;
   }
   
   AG_request_list_t* reqs_list[] = {
      dir_reqs,
      file_reqs,
      NULL
   };
   
   int num_expected_reqs_list[] = {
      num_expected_dir_ents,
      num_expected_file_ents,
      0
   };
   
   for( int j = 0; reqs_list[j] != NULL; j++ ) {
      
      // each request list...
      AG_request_list_t* reqs = reqs_list[j];
      
      for( unsigned int i = 0; i < reqs->size() && i < (unsigned)num_expected_reqs_list[j]; i++ ) {
         
         struct md_entry* ent = &results->ents[i];
         
         // find the parent 
         AG_request_parent_map_t::iterator parent_itr = stage->parents->find( ent->file_id );
         if( parent_itr == stage->parents->end() ) {
            
            // very strange..this shouldn't happen 
            errorf("BUG: no parent name found for %" PRIX64 " (%s)\n", ent->file_id, ent->name );
            rc = -EINVAL;
            break;
         }
         
         char* dirpath = parent_itr->second;
         
         // make a duplicate of the map_info in mi_reference, and add it to dest
         char* path = md_fullpath( dirpath, ent->name, NULL );
         struct AG_map_info* mi = AG_fs_lookup_path_in_map( mi_reference, path );
         
         if( mi == NULL ) {
            
            errorf("Not found: %s\n", path );
            
            free( path );
            rc = -ENOENT;
            break;
         }
         
         // update with ent data
         AG_map_info_make_coherent_with_data( mi, path, ent->file_id, ent->version, md_random64(), ent->write_nonce, AG_map_info_make_deadline( mi->reval_sec ) );
         
         AG_fs_map_t::iterator itr = dest->find( path );
         if( itr != dest->end() ) {   
            
            AG_map_info_free( itr->second );
            free( itr->second );
            
            dest->erase( itr );
         }
         
         (*dest)[ string(path) ] = mi;
         
         free( path );
      }
      
      if( rc != 0 ) {
         // message failure 
         break;
      }
   }
   
   return rc;
}


// add a stage-list's worth of data to an fs_map.
// NOTE: no validation occurs on the data, use AG_validate_stage_MS_response for that.
// This method fails with -EEXIST if there is a collision.
static int AG_fs_add_stages( AG_fs_map_t* dest, AG_fs_map_t* mi_reference, AG_request_stage_list_t* stages ) {
   
   int rc = 0;
   
   for( unsigned int i = 0; i < stages->size(); i++ ) {
      
      rc = AG_fs_add_stage_data( dest, mi_reference, stages->at(i) );
      if( rc != 0 ) {
         errorf("AG_fs_add_stage_data(depth=%d) rc = %d\n", stages->at(i)->depth, rc );
         break;
      }
   }
   
   return rc;
}


// mark a mapping of entries (in mi_reference) as updated in dest.
// entries must exist in dest.
static int AG_fs_mark_all_updated( AG_fs_map_t* dest, AG_fs_map_t* mi_reference ) {
   
   int rc = 0;
   
   for( AG_fs_map_t::iterator itr = mi_reference->begin(); itr != mi_reference->end(); itr++ ) {
      
      char const* path = itr->first.c_str();
      
      AG_fs_map_t::iterator mi_itr = dest->find( string(path) );
      if( mi_itr == dest->end() ) {
         errorf("Not found: %s\n", path );
         rc = -ENOENT;
         break;
      }
      
      struct AG_map_info* mi = mi_itr->second;

      // mark as updated, so we refresh later
      mi->refresh_deadline = AG_map_info_make_deadline( mi->reval_sec );
   }
   
   return rc;
}

// remove files and directories from an fs_map, as listed by the given stage
static int AG_fs_remove_stage_data( AG_fs_map_t* dest, struct AG_request_stage* stage ) {
   
   int rc = 0;

   AG_request_list_t* file_reqs = stage->file_reqs;
   AG_request_list_t* dir_reqs = stage->dir_reqs;
   
   AG_request_list_t* reqs_list[] = {
      dir_reqs,
      file_reqs,
      NULL
   };
   
   for( int j = 0; reqs_list[j] != NULL; j++ ) {
      
      // each request list...
      AG_request_list_t* reqs = reqs_list[j];
      
      for( unsigned int i = 0; i < reqs->size(); i++ ) {
         
         struct ms_client_request* req = &reqs->at(i);
         
         // find the parent 
         AG_request_parent_map_t::iterator parent_itr = stage->parents->find( req->ent->file_id );
         if( parent_itr == stage->parents->end() ) {
            
            // very strange..this shouldn't happen 
            errorf("BUG: no parent name found for %" PRIX64 " (%s)\n", req->ent->file_id, req->ent->name );
            rc = -EINVAL;
            break;
         }
         
         char* dirpath = parent_itr->second;
         
         char* path = md_fullpath( dirpath, req->ent->name, NULL );
         
         // remove it from the fs, replacing an existing entry if need be.
         AG_fs_map_t::iterator itr = dest->find( string(path) );
         if( itr != dest->end() ) {
         
            // blow this one away 
            AG_map_info_free( itr->second );
            free( itr->second );
            
            dest->erase( itr );
         }
         
         free( path );
      }
      
      if( rc != 0 ) {
         // message failure 
         break;
      }
   }
   
   return rc;
}


// remove file and directories from an fs_map, as listed by the stages 
static int AG_fs_remove_stages( AG_fs_map_t* dest, AG_request_stage_list_t* stages ) {
   
   int rc = 0;
   
   for( unsigned int i = 0; i < stages->size(); i++ ) {
      
      rc = AG_fs_remove_stage_data( dest, stages->at(i) );
      if( rc != 0 ) {
         errorf("AG_fs_remove_stage_data(depth=%d) rc = %d\n", stages->at(i)->depth, rc );
         break;
      }
   }
   
   return rc;
}


// generate a list of stages with the requested method, run it, and validate the response.
// don't mask errors; fail fast if the MS barfs on any request
static int AG_fs_apply_stages( struct ms_client* client, AG_request_stage_generator_t stage_list_generator, AG_fs_map_t* directives, AG_fs_map_t* reference, AG_request_stage_list_t* stages ) {
   
   int rc = 0;
   
   // run the generator 
   rc = (*stage_list_generator)( client, directives, reference, stages );
   if( rc != 0 ) {
      errorf("Stage list generator rc = %d\n", rc );
      return rc;
   }
   
   // run it and validate it
   rc = AG_run_stages( client, stages, client->max_connections, client->max_request_batch, client->max_request_async_batch );
   if( rc != 0 ) {
      errorf("AG_run_stages rc = %d\n", rc );
      
      return rc;
   }
   
   return 0;
}


// create a mapping of data on the MS, and put any obtained data into the AG fs.
// current_mappings must have the AG-specific information for all paths to create, since 
// we're going to use its data plus the data the MS returns to map the paths in ag_fs.
int AG_fs_create_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_publish, AG_fs_map_t* current_mappings ) {
   
   AG_request_stage_list_t stages;
   int rc = 0;
   
   // build up a set of references for publishing.
   // * each entry in to_publish must have its parent data in either to_publish or in current_mappings
   // * each entry in to_publish must not have MS-given data.  We'll generate it.
   
   AG_fs_map_t merged_publish;
   AG_fs_map_dup( &merged_publish, to_publish );
   
   for( AG_fs_map_t::iterator itr = merged_publish.begin(); itr != merged_publish.end(); itr++ ) {
      
      char const* mi_path = itr->first.c_str();
      struct AG_map_info* mi = itr->second;
      
      // what's the parent?
      char* mi_parent = md_dirname( mi_path, NULL );
      
      AG_fs_map_t::iterator parent_itr = merged_publish.find( string(mi_parent) );
      if( parent_itr == merged_publish.end() ) {
         
         // parent is not to be published.  It must be in current_mappings then.
         parent_itr = current_mappings->find( string(mi_parent) );
         if( parent_itr == current_mappings->end() ) {
            
            // no parent data given!
            errorf("ERR: no parent data for %s\n", mi_path );
            rc = -ENOENT;
         }
         else {
            
            // must have MS-given data, since it's not to be published 
            if( !parent_itr->second->cache_valid ) {
               
               // no parent data available 
               errorf("ERR: reference parent data for %s is stale\n", mi_path );
               rc = -ESTALE;
            }
         }
      }
      
      if( rc == 0 ) {
         // publish data...
         mi->file_id = ms_client_make_file_id();
         mi->file_version = md_random64();
         mi->write_nonce = md_random64();
      }
      
      free( mi_parent );
   }
   
   if( rc != 0 ) {
      
      AG_fs_map_free( &merged_publish );
      return rc;
   }
   
   // get the stages 
   rc = AG_fs_apply_stages( client, AG_generate_stages_create, &merged_publish, current_mappings, &stages );
   
   AG_fs_map_free( &merged_publish );
   
   if( rc != 0 ) {
      errorf("AG_fs_apply_stages(create) rc = %d\n", rc );
      
      AG_request_stage_list_free( &stages );
      return rc;
   }
   
   // add everything.
   rc = AG_fs_add_stages( dest, to_publish, &stages );
   if( rc != 0 ) {
      errorf("AG_fs_add_stages() rc = %d\n", rc );
      
      AG_request_stage_list_free( &stages );
      
      return rc;
   }
   
   // success!
   AG_request_stage_list_free( &stages );
   return rc;
}


// update a mapping of data on the MS, and put new data into the AG fs.
// everything in to_update must be coherent
// current_mappings must have the AG-specific information for all paths to update, 
// since we're going to use its data plus the data the MS returns.
int AG_fs_update_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_update, AG_fs_map_t* current_mappings ) {
   
   AG_request_stage_list_t stages;
   int rc = 0;

   for( AG_fs_map_t::iterator itr = to_update->begin(); itr != to_update->end(); itr++ ) {
      
      struct AG_map_info* mi = itr->second;
      
      if( !mi->cache_valid ) {
         
         errorf("Not coherent: %s\n", itr->first.c_str() );
         rc = -ESTALE;
      }
   }
   
   if( rc != 0 ) {
      
      return rc;
   }
   
   // get the stages 
   rc = AG_fs_apply_stages( client, AG_generate_stages_update, to_update, current_mappings, &stages );
   
   if( rc != 0 ) {
      errorf("AG_fs_apply_stages(update) rc = %d\n", rc );
      
      AG_request_stage_list_free( &stages );
      return rc;
   }
   
   // mark everything as updated locally
   rc = AG_fs_mark_all_updated( dest, to_update );
   if( rc != 0 ) {
      errorf("AG_fs_mark_all_updated() rc = %d\n", rc );
      
      AG_request_stage_list_free( &stages );
      
      return rc;
   }
   
   // success!
   AG_request_stage_list_free( &stages );
   return rc;
}


// delete a mapping of data on the MS, and delete the corresponding entries in a destination fs_map
// current_mappings must have the AG-specific information for all paths to delete.
int AG_fs_delete_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_delete, AG_fs_map_t* current_mappings ) {
   
   AG_request_stage_list_t stages;
   AG_request_stage_list_t all_stages;
   
   int rc = 0;
   
   // make all the stages 
   rc = AG_generate_stages_delete( client, to_delete, current_mappings, &all_stages );
   if( rc != 0 ) {
      errorf("Stage list generator (delete) rc = %d\n", rc );
      return rc;
   }
   
   // keep shallow copies to all stages, and work with this instead
   for( unsigned int i = 0; i < all_stages.size(); i++ ) {
      stages.push_back( all_stages[i] );
   }
   
   // run the stages, but try a failed stage again if we fail with -ENOTEMPTY.
   // this is because sometimes entries don't disappear synchronously on delete, since 
   // the MS's query-processing logic is not guaranteed to be sequentially consistent.
   while( true ) {

      // run the stages that have not been processed yet
      rc = AG_run_stages( client, &stages, client->max_connections, client->max_request_batch, client->max_request_async_batch );
      
      if( rc == -ENOENT ) {
         
         // find the failed stage
         unsigned int failed_stage = 0;
         
         for( failed_stage = 0; failed_stage < stages.size(); failed_stage++ ) {
            if( stages[failed_stage]->error != 0 && stages[failed_stage]->failed_reqs != NULL ) {
               break;
            }
         }
         
         if( failed_stage == stages.size() ) {
            errorf("%s", "BUG: could not find failed stage\n");
            rc = -EIO;
            break;
         }
         
         // try again, but mask -ENOENT 
         dbprintf("WARN: stage %u failed; will retry while ignoring -ENOENT\n", failed_stage );
      
         // run this stage again, but mask -ENOENT, since we're retrying a deletion 
         AG_operational_error_set_t masked;
         masked.insert( -ENOENT );
         
         // try the failed stage again 
         rc = AG_run_stage( client, stages[failed_stage], client->max_connections, client->max_request_batch, client->max_request_async_batch, &masked );
         if( rc != 0 ) {
            errorf("AG_run_stage(depth=%d) rc = %d\n", stages[failed_stage]->depth, rc );
         }
         
         else {   
            // remove the successful stages, and try the rest 
            stages.erase( stages.begin(), stages.begin() + (failed_stage + 1) );
         }
      }
      else {
         
         // success, or some other error besides -ENOENT
         if( rc != 0 ) {
            errorf("AG_run_stages(delete) rc = %d\n", rc );
         }
         
         break;
      }
   }
   
   if( rc != 0 ) {
      // can't continue 
      AG_request_stage_list_free( &all_stages );
      return rc;
   }
   
   // remove everything
   rc = AG_fs_remove_stages( dest, &all_stages );
   if( rc != 0 ) {
      errorf("AG_fs_remove_stages rc = %d\n", rc );
      
      AG_request_stage_list_free( &all_stages );
      
      return rc;
   }
   
   // success!
   AG_request_stage_list_free( &all_stages );
   return rc;
}


// reversion a (path, map_info) via the driver.
// this updates the version field of the file, and will fail if it doesn't exist.
// optionally use the caller-given opt_pubinfo, or generate new pubinfo from the driver.
int AG_fs_reversion( struct AG_fs* ag_fs, char const* path, struct AG_driver_publish_info* opt_pubinfo ) {
   
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
   rc = AG_populate_md_entry( ag_fs->ms, &entry, path, mi, parent_mi, 0, opt_pubinfo );
   
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
