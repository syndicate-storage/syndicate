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

// initialize a DAG node
// NOTE: this consumes request--node becomes its owner
int AG_request_DAG_node_init( struct AG_request_DAG_node* node, char const* dirpath, int file_oper, int dir_oper, AG_request_list_t* file_reqs, AG_request_list_t* dir_reqs, bool dirs_first ) {
   
   memset( node, 0, sizeof(struct AG_request_DAG_node) );
   
   node->dir_path = strdup(dirpath );
   
   node->file_oper = file_oper;
   node->dir_oper = dir_oper;
   
   node->file_reqs = file_reqs;
   node->dir_reqs = dir_reqs;
   
   node->dirs_first = dirs_first;
   
   return 0;
}


// free a DAG node
int AG_request_DAG_node_free( struct AG_request_DAG_node* node ) {
   
   AG_request_list_t** reqs_to_free[] = {
      &node->file_reqs,
      &node->dir_reqs,
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
   
   if( node->dir_path != NULL ) {
      free( node->dir_path );
      node->dir_path = NULL;
   }
   
   ms_client_multi_result_free( &node->results );
   
   return 0;
}


// free a list of DAG nodes 
int AG_request_DAG_node_list_free( AG_request_DAG_node_list_t* dag ) {
   
   for( unsigned int i = 0; i < dag->size(); i++ ) {
      
      AG_request_DAG_node_free( dag->at(i) );
      free( dag->at(i) );
      
   }
   
   dag->clear();
   return 0;
}

// find and remove all entries from sorted_paths of the same depth, and put them into directory_list
// sorted_paths must be sorted by increasing path depth.
static int AG_pop_DAG_list( vector<string>* sorted_paths, vector<string>* directory_list ) {
   
   if( sorted_paths->size() == 0 ) {
      return -EINVAL;
   }
   
   int depth = 0;
   int num_pop = 0;
   
   // what's the depth of the head?
   depth = md_depth( sorted_paths->at(0).c_str() );
   
   directory_list->push_back( sorted_paths->at(0) );
   
   num_pop = 1;
   
   if( sorted_paths->size() > 1 ) {
      // find all directories in sorted_paths that are have $depth.
      // they will all be at the head.
      for( unsigned int i = 1; i < sorted_paths->size(); i++ ) {
         
         int next_depth = md_depth( sorted_paths->at(i).c_str() );
         if( next_depth == depth ) {
            
            directory_list->push_back( sorted_paths->at(i) );
            num_pop++;
         }
         else {
            break;
         }
      }
   }
   
   // pop all pushed paths 
   sorted_paths->erase( sorted_paths->begin(), sorted_paths->begin() + num_pop );
   
   return 0;
}

// given a list of paths that are all children of the same directory, and the operation to be performed with them, generate a DAG node containing the request
// use the operation to perform sanity checks and to fill out the request properly.
// directives containst the metadata for the paths in listing_paths to publish.
// if we're creating entries, then the paths in listing_paths don't have to be coherent (but they have to be for update and delete)
// always honor coherency requirements in coherency flags (abort with ESTALE if they are not met)
static int AG_generate_DAG_node( struct ms_client* client, char const* dirpath, struct AG_map_info* parent_mi, vector<string>* listing_paths, AG_fs_map_t* directives,
                                 int file_op, int dir_op, struct AG_request_DAG_node* node, int flags ) {
   
   if( listing_paths->size() == 0 ) {
      // nothing to do 
      return 0;
   }
   
   int rc = 0;
   uint64_t volume_id = ms_client_get_volume_id( client );
   
   AG_request_list_t* file_requests = new AG_request_list_t();
   AG_request_list_t* dir_requests = new AG_request_list_t();
   
   
   for( unsigned int i = 0; i < listing_paths->size(); i++ ) {
      
      // find the map info 
      AG_fs_map_t::iterator itr = directives->find( listing_paths->at(i) );
      if( itr == directives->end() ) {
         
         // not found; can't continue
         errorf("ERR: Not found in directives: %s\n", listing_paths->at(i).c_str() );
         rc = -ENOENT;
         break;
      }
      
      char const* mi_path = itr->first.c_str();
      char* mi_name = md_basename( mi_path, NULL );
      
      struct AG_map_info* mi = itr->second;
      
      // entry to generate 
      struct md_entry ent;
      memset( &ent, 0, sizeof(struct md_entry) );
      
      if( flags & (AG_DAG_USE_DRIVER) ) {
         
         // driver info 
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
      
      if( parent_mi != NULL ) {
         ent.parent_id = parent_mi->file_id;
         ent.parent_name = md_basename( dirpath, NULL );
      }
      
      // populate this request
      struct ms_client_request req;
      memset( &req, 0, sizeof(struct ms_client_request) );
      
      req.ent = CALLOC_LIST( struct md_entry, 1 );
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
   }
   
   if( rc != 0 ) {
      
      // had an error.  revert and clean up
      for( unsigned int i = 0; i < file_requests->size(); i++ ) {
         
         ms_client_request_free( &file_requests->at(i) );
      }
      
      for( unsigned int i = 0; i < dir_requests->size(); i++ ) {
         
         ms_client_request_free( &dir_requests->at(i) );
      }
      
      file_requests->clear();
      dir_requests->clear();
      
      delete file_requests;
      delete dir_requests;
      
      return rc;
   }
   
   // set up the DAG node 
   AG_request_DAG_node_init( node, dirpath, file_op, dir_op, file_requests, dir_requests, (flags & AG_DAG_DIRS_FIRST) != 0 );
   
   return 0;
}


// dump a DAG to stdout, for debugging purposes 
static int AG_dump_DAG( AG_request_DAG_node_list_t* dag ) {
   
   dbprintf("Begin DAG %p\n", dag );
   for( unsigned int i = 0; i < dag->size(); i++ ) {
      
      struct AG_request_DAG_node* node = dag->at(i);
      
      dbprintf("   DAG stage %u, dirs_first = %d\n", i, node->dirs_first );
      dbprintf("      Directories (operation=%d):\n", node->dir_oper);
      
      for( unsigned int j = 0; j < node->dir_reqs->size(); j++ ) {
         dbprintf("       %" PRIX64 " name=%s, parent=%" PRIX64 "\n", node->dir_reqs->at(j).ent->file_id, node->dir_reqs->at(j).ent->name, node->dir_reqs->at(j).ent->parent_id );
      }
      
      dbprintf("      Files (operation=%d):\n", node->file_oper);
      
      for( unsigned int j = 0; j < node->file_reqs->size(); j++ ) {
         dbprintf("       %" PRIX64 " name=%s, parent=%" PRIX64 "\n", node->file_reqs->at(j).ent->file_id, node->file_reqs->at(j).ent->name, node->file_reqs->at(j).ent->parent_id );
      }
   }
   dbprintf("End DAG %p\n", dag );
   return 0;
}


// Given a list of paths and their associated data in reference, generate a request DAG that when evaluated step by step, will perform the given operation on the MS for the collection of entries they name.
// The request DAG is generated from the root of the fs, where files and directories are grouped by depth.
// The resulting request DAG is in pre-order traversal, so [root, (root's child directories, root's child files), (root's grandchild directories, root's grandchild files), ...]
static int AG_generate_DAG( struct ms_client* client, AG_fs_map_t* directives, AG_fs_map_t* reference, int file_op, int dir_op, int node_flags, AG_request_DAG_node_list_t* dag ) {
   
   int rc = 0;
   int depth = 0;
   int expected_depth = 0;      // for checking for correctness
   vector<string> paths;        // set of paths in directives
   
   // sanity check 
   if( directives->size() == 0 ) {
      // empty DAG
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
      struct AG_request_DAG_node* node = CALLOC_LIST( struct AG_request_DAG_node, 1 );
      
      // get the next group of files and directories that belong together
      AG_pop_DAG_list( &paths, &directory_list );
      
      ////////////////////////////////////////////////////
      dbprintf("DAG node %d will have these %zu items:\n", expected_depth, directory_list.size() );
      
      for( unsigned int i = 0; i < directory_list.size(); i++ ) {
         dbprintf("   '%s'\n", directory_list[i].c_str() );
      }
      ////////////////////////////////////////////////////
      
      // what's the current depth?
      depth = md_depth( directory_list.at(0).c_str() );
      
      // sanity check: each iteration of this loop should consume all directories at a given depth, in increasing order
      if( depth != expected_depth ) {
         errorf("BUG: depth of %s is %d, but expected %d\n", directory_list.at(0).c_str(), depth, expected_depth );
         rc = -EINVAL;
         break;
      }
      
      char* dirpath = md_dirname( directory_list.at(0).c_str(), NULL );
      
      // find the parent of the entries we'll generate 
      AG_fs_map_t::iterator parent_itr;
      AG_fs_map_t::iterator parent_itr_reference = reference->find( string(dirpath) );
      AG_fs_map_t::iterator parent_itr_directive = directives->find( string(dirpath) );
      
      if( parent_itr_reference == reference->end() && parent_itr_directive == directives->end() ) {
         
         errorf("Parent not found: %s\n", dirpath );
         free( node );
         free( dirpath );
         rc = -ENOENT;
         break;
      }
      else if( parent_itr_reference != reference->end() && parent_itr_directive != directives->end() ) {
         
         // parent available in both reference and directives.
         if( (node_flags & AG_DAG_USE_DIRECTIVES) != 0 ) {
            
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
      
      struct AG_map_info* parent_mi = parent_itr->second;
      
      dbprintf("Generate DAG node %s\n", dirpath );
      
      // generate a DAG node for this depth 
      rc = AG_generate_DAG_node( client, dirpath, parent_mi, &directory_list, directives, file_op, dir_op, node, node_flags );
      
      if( rc != 0 ) {
         errorf("AG_generate_DAG_node(%s) rc = %d\n", dirpath, rc );
         free( node );
         free( dirpath );
         break;
      }
      
      free( dirpath );
      
      dag->push_back( node );
      
      expected_depth++;
   }
   
   if( rc != 0 ) {
      // clean up 
      AG_request_DAG_node_list_free( dag );
   }
   else {
      AG_dump_DAG( dag );
   }
   return rc;
}


// generate a DAG that will create everything in the given to_publish mapping
// directories will be created by the MS synchronously.
// files will be created by the MS asynchronously.
// when building the DAG, get parent data from the directives over the reference if there's a conflict, since the parent doesn't exist
static int AG_generate_DAG_create( struct ms_client* client, AG_fs_map_t* to_publish, AG_fs_map_t* reference, AG_request_DAG_node_list_t* dag ) {
   return AG_generate_DAG( client, to_publish, reference, ms::ms_update::CREATE_ASYNC, ms::ms_update::CREATE, AG_DAG_DIRS_FIRST | AG_DAG_USE_DIRECTIVES, dag );
}

// generate a DAG that will update everything in the given to_update mapping
// all updates will be processed by the MS asynchronously.
// when building the DAG, get parent data from the reference over the directives if there's a conflict, since the parent should already exist.
static int AG_generate_DAG_update( struct ms_client* client, AG_fs_map_t* to_update, AG_fs_map_t* reference, AG_request_DAG_node_list_t* dag ) {
   return AG_generate_DAG( client, to_update, reference, ms::ms_update::UPDATE_ASYNC, ms::ms_update::UPDATE_ASYNC, AG_DAG_DIRS_FIRST | AG_DAG_USE_DRIVER, dag );
}

// generate a DAG that will delete everything in the given to_delete mapping 
// files and directories will be deleted synchronously.
static int AG_generate_DAG_delete( struct ms_client* client, AG_fs_map_t* to_delete, AG_fs_map_t* reference, AG_request_DAG_node_list_t* dag ) {
   
   int rc = AG_generate_DAG( client, to_delete, reference, ms::ms_update::DELETE, ms::ms_update::DELETE, 0, dag );
   if( rc != 0 ) {
      errorf("AG_generate_DAG(delete) rc = %d\n", rc );
      return rc;
   }
   
   // reverse the DAG, since we have to delete in post-traversal order 
   reverse( dag->begin(), dag->end() );
   return 0;
}


// clean up a list of network contexts 
static int AG_network_contexts_cancel_and_free( struct ms_client* client, struct md_download_set* dlctx, struct ms_client_network_context* contexts, int num_contexts ) {
   // failed to set up.  clean up
   for( int i = 0; i < num_contexts; i++ ) {
      if( contexts[i].dlctx != NULL ) {
         ms_client_network_context_cancel( client, &contexts[i] );
         
         if( contexts[i].dlset != NULL ) {
            md_download_set_clear( contexts[i].dlset, contexts[i].dlctx );  
         }
      }
   }
   
   for( int i = 0; i < num_contexts; i++ ) {
      ms_client_network_context_free( &contexts[i] );
   }
   
   return 0;
}


// start up to num_connections, with up to max_batch operations.
// track network contexts with the download_set
static int AG_start_operations( struct ms_client* client, struct ms_client_network_context* contexts, int num_connections, int oper, int flags, int max_batch,
                                AG_request_list_t* reqs, int req_offset,
                                struct md_download_set* dlset, int* ret_rc ) {
   
   int offset = 0;      // offset from req_offset in reqs where the next requests will be inserted for processing
   int rc = 0;
   
   // start batch operations 
   for( int i = 0; i < num_connections && (unsigned)(req_offset + offset) < reqs->size(); i++ ) {
      
      // find a free connection 
      if( contexts[i].started ) {
         continue;
      }
      
      // attach this context's dlctx to this download set 
      ms_client_network_context_set( &contexts[i], dlset );
      
      
      size_t num_reqs = MIN( (unsigned)max_batch, reqs->size() - offset );
      if( num_reqs == 0 ) {
         break;
      }
      
      // NOTE: vectors are guaranteed by C++ to be contiguous in memory
      struct ms_client_request* msreqs = &reqs->at( req_offset + offset );
      
      // start running directories
      rc = ms_client_multi_begin( client, oper, flags, msreqs, num_reqs, &contexts[i] );
      if( rc != 0 ) {
         errorf("ms_client_multi_begin(%p) rc = %d\n", contexts[i].dlctx, rc );
         break;
      }
      
      // next batch...
      offset += num_reqs;
   }
   
   if( rc != 0 ) {
      *ret_rc = rc;
   }
   
   return offset;
}


// finish an operation on a node
static int AG_finish_operation( struct ms_client* client, struct ms_client_network_context* nctx, struct AG_request_DAG_node* node ) {
   
   int rc = 0;
   struct ms_client_multi_result results;
   
   memset( &results, 0, sizeof(struct ms_client_multi_result) );
   
   // finish and get results
   rc = ms_client_multi_end( client, &results, nctx );
   if( rc != 0 ) {
      errorf("ms_client_multi_end(%p (node=%s)) rc = %d\n", nctx->dlctx, node->dir_path, rc );
      return rc;
   }
   
   if( results.reply_error != 0 ) {
      errorf("Operational reply error %d, num_processed = %d\n", results.reply_error, results.num_processed );
      ms_client_multi_result_free( &results );
      return results.reply_error;
   }
   
   dbprintf("Node %s: got back %d results, %zu entries\n", node->dir_path, results.num_processed, results.num_ents );
   
   // merge results into the node's results
   ms_client_multi_result_merge( &node->results, &results );
   
   dbprintf("Node %s: %d results total (%zu entries)\n", node->dir_path, node->results.num_processed, node->results.num_ents );
   
   return 0;
}


// run a list of requests for a DAG node, but don't open more than max_connections connections and don't send more than max_batch operations
static int AG_run_DAG_node_requests( struct ms_client* client, struct AG_request_DAG_node* node, int oper, AG_request_list_t* reqs, int max_connections, int max_batch ) {
   
   // sanity check 
   if( reqs->size() == 0 ) {
      // done!
      return 0;
   }
   
   int rc = 0;
   int offset = 0;
   int started = 0;
   
   // network contexts 
   struct ms_client_network_context* contexts = CALLOC_LIST( struct ms_client_network_context, max_connections );
   
   // download set for the contexts 
   struct md_download_set dlset;
   md_download_set_init( &dlset );
   
   // start batch operations 
   started = AG_start_operations( client, contexts, max_connections, oper, node->flags, max_batch, reqs, 0, &dlset, &rc );
   
   if( rc != 0 ) {
      // failed to set up.  clean up
      AG_network_contexts_cancel_and_free( client, &dlset, contexts, started );
      
      free( contexts );
      md_download_set_free( &dlset );
      
      return rc;
   }
   
   dbprintf("Node %s: started %d connections (%d results so far)\n", node->dir_path, started, node->results.num_processed );
   
   offset += started;
   
   // reap connections; start next operations
   while( true ) {
      
      // wait for directory operations to finish
      rc = md_download_context_wait_any( &dlset, -1 );
      if( rc != 0 ) {
         
         // failed
         break;
      }
      
      set<int> finished;
      int num_running = 0;
      
      // which operation(s) finished?
      for( int i = 0; i < max_connections; i++ ) {
         
         struct md_download_context* dlctx = contexts[i].dlctx;
         
         if( dlctx == NULL ) {
            continue;
         }
         
         // not done yet?
         if( !md_download_context_finalized( dlctx ) ) {
            
            num_running ++;
            continue;
         }
         
         // finished!  process it and keep its data around 
         rc = AG_finish_operation( client, &contexts[i], node );
         if( rc != 0 ) {
            
            // failed to finish 
            break;
         }
         else {
            
            // mark as finished 
            finished.insert( i );
         }
         
         // start more downloads, if there are more left 
         if( offset < (signed)reqs->size() ) {
            
            started = AG_start_operations( client, contexts, oper, node->flags, max_connections, max_batch, reqs, offset, &dlset, &rc );
            if( rc != 0 ) {
               
               // failed to start 
               break;
            }
            else {
               
               dbprintf("Node %s: started %d more connections (%d total, %d results)\n", node->dir_path, started, offset, node->results.num_processed );
               
               offset += started;
               
               // discount all now-running contexts from our finished set
               for( int j = 0; j < num_running; j++ ) {
                  
                  if( contexts[j].started ) {
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
         
         if( contexts[i].dlctx != NULL ) {
            md_download_set_clear( &dlset, contexts[i].dlctx );
         }
      }
      
      // if there are no more downloads running, then we're done
      if( num_running == 0 ) {
         break;
      }
   }
   
   md_download_set_free( &dlset );
   
   if( rc != 0 ) {
      // clean up on error from the operations loop
      AG_network_contexts_cancel_and_free( client, &dlset, contexts, max_connections );
   }
   
   free( contexts );
   return rc;
}


// run a single DAG node.
// return 0 on success, negative on error.
static int AG_run_DAG_node( struct ms_client* client, struct AG_request_DAG_node* node, int max_connections, int max_batch ) {
   
   int rc = 0;
   
   AG_request_list_t** reqs = NULL;
   char const** req_names = NULL;
   const int* opers = NULL;
   
   if( node->dirs_first ) {
      
      const AG_request_list_t* d_reqs[] = {
         node->dir_reqs,
         node->file_reqs
      };
      
      char const* d_req_names[] = {
         "dirs",
         "files"
      };
      
      const int d_opers[] = {
         node->dir_oper,
         node->file_oper
      };
      
      reqs = (AG_request_list_t**)d_reqs;
      req_names = d_req_names;
      opers = d_opers;
   }
   else {
      
      const AG_request_list_t* f_reqs[] = {
         node->file_reqs,
         node->dir_reqs
      };
      
      char const* f_req_names[] = {
         "files",
         "dirs"
      };
      
      const int f_opers[] = {
         node->file_oper,
         node->dir_oper
      };
      
      reqs = (AG_request_list_t**)f_reqs;
      req_names = f_req_names;
      opers = f_opers;
   }
   
   for( int i = 0; i < 2; i++ ) {
        
      dbprintf("Node %s: Run %zu requests on %s\n", node->dir_path, reqs[i]->size(), req_names[i] );
      
      rc = AG_run_DAG_node_requests( client, node, opers[i], reqs[i], max_connections, max_batch );
      if( rc != 0 ) {
         errorf("AG_run_DAG_node_requests(%s, node=%s) rc = %d\n", req_names[i], node->dir_path, rc );
         return rc;
      }
   }
   
   return 0;
}

// validate the MS response against the requests we made 
static int AG_validate_DAG_MS_response( struct AG_request_DAG_node* node ) {
   
   int rc = 0;
   
   AG_request_list_t* file_reqs = node->file_reqs;
   AG_request_list_t* dir_reqs = node->dir_reqs;
   struct ms_client_multi_result* results = &node->results;
   
   
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
   if( (signed)(dir_reqs->size() + file_reqs->size()) != node->results.num_processed ) {
      errorf("Sent %zu requests, but the MS processed %d\n", dir_reqs->size() + file_reqs->size(), node->results.num_processed );
      return -EREMOTEIO;
   }
   
   // were we even expecting results to be sent back?
   int num_expected_dirs = ms_client_num_expected_reply_ents( dir_reqs->size(), node->dir_oper );
   int num_expected_files = ms_client_num_expected_reply_ents( file_reqs->size(), node->file_oper );
   
   int num_expected_replies[] = {
      num_expected_dirs,
      num_expected_files,
      -1
   };
   
   for( int j = 0; reqs_list[j] != NULL; j++ ) {
      
      // each request list...
      AG_request_list_t* reqs = reqs_list[j];
      int expected_type = reqs_types[j];
      
      if( num_expected_replies[j] == 0 ) {
         continue;
      }
      
      if( num_expected_replies[j] != (signed)reqs->size() ) {
         errorf("Expected %d replies for type %d, but got %zu\n", num_expected_replies[j], reqs_types[j], reqs->size() );
         return -EBADMSG;
      }
      
      for( unsigned int i = 0; i < reqs->size(); i++ ) {
         
         struct ms_client_request* req = &reqs->at(i);
         struct md_entry* ent = &results->ents[i];
         
         // The MS *should* return entires that are of the same type 
         if( expected_type != ent->type ) {
            errorf("Invalid MS data: entry %d should have type %d, but the MS replied %d\n", i, expected_type, ent->type );
            rc = -EBADMSG;
            break;
         }
         
         // The MS *should* return entries in the same order as we requested them
         if( strcmp( ent->name, req->ent->name ) != 0 ) {
            errorf("Invalid MS data: entry %d (type %d) should be '%s', but the MS replied '%s'\n", i, ent->type, ent->name, req->ent->name );
            rc = -EBADMSG;
            break;
         }
      }
   }
   
   return rc;
}


// given a request DAG, walk down it and send the updates to the MS 
// return 0 on succes, negative on failure.
static int AG_run_DAG( struct ms_client* client, AG_request_DAG_node_list_t* dag, int max_connections, int max_batch ) {
   
   int rc = 0;
   
   for( unsigned int i = 0; i < dag->size(); i++ ) {
      
      struct AG_request_DAG_node* node = dag->at(i);
      
      dbprintf("Running DAG node %d\n", i );
      
      rc = AG_run_DAG_node( client, node, max_connections, max_batch );
      if( rc != 0 ) {
      
         // this DAG stage failed
         errorf("AG_run_DAG_node(%s, depth=%d) rc = %d\n", node->dir_path, i, rc );
         break;
      }
      
      
      dbprintf("Validating MS replies for DAG node %s (depth=%d)\n", node->dir_path, i );
      
      rc = AG_validate_DAG_MS_response( node );
      if( rc != 0 ) {
      
         // this DAG stage fialed 
         errorf("AG_validate_DAG_MS_response(%s, depth=%d) rc = %d\n", node->dir_path, i, rc );
         break;
      }
   }
   
   return rc;
}



// add a node's worth of data into an fs_map (overwriting duplicates)
// NOTE: no validation occurs on the MS-given node results (use AG_validate_DAG_MS_response for that) beyond ensuring that we have N replies for N total requests.
// mi_reference will be used as a whitelist of entries to add, and will be used to ensure that the MS didn't 
static int AG_fs_add_DAG_node_data( AG_fs_map_t* dest, AG_fs_map_t* mi_reference, struct AG_request_DAG_node* node ) {
   
   int rc = 0;
   
   AG_request_list_t* file_reqs = node->file_reqs;
   AG_request_list_t* dir_reqs = node->dir_reqs;
   struct ms_client_multi_result* results = &node->results;
   
   int num_expected_file_ents = ms_client_num_expected_reply_ents( file_reqs->size(), node->file_oper );
   int num_expected_dir_ents = ms_client_num_expected_reply_ents( dir_reqs->size(), node->dir_oper );
   
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
         
         // make a duplicate of the map_info in mi_reference, and add it to dest
         char* path = md_fullpath( node->dir_path, ent->name, NULL );
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


// add a DAG's worth of data to an fs_map.
// NOTE: no validation occurs on the data, use AG_validate_DAG_MS_response for that.
// This method fails with -EEXIST if there is a collision.
static int AG_fs_add_DAG( AG_fs_map_t* dest, AG_fs_map_t* mi_reference, AG_request_DAG_node_list_t* dag ) {
   
   int rc = 0;
   
   for( unsigned int i = 0; i < dag->size(); i++ ) {
      
      rc = AG_fs_add_DAG_node_data( dest, mi_reference, dag->at(i) );
      if( rc != 0 ) {
         errorf("AG_fs_add_DAG_node_data(depth=%d) rc = %d\n", i, rc );
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

// remove files and directories from an fs_map, as listed by the given dag node
static int AG_fs_remove_DAG_node_data( AG_fs_map_t* dest, struct AG_request_DAG_node* node ) {
   
   int rc = 0;

   AG_request_list_t* file_reqs = node->file_reqs;
   AG_request_list_t* dir_reqs = node->dir_reqs;
   
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
         
         char* path = md_fullpath( node->dir_path, req->ent->name, NULL );
         
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


// remove file and directories from an fs_map, as listed by the dag 
static int AG_fs_remove_DAG( AG_fs_map_t* dest, AG_request_DAG_node_list_t* dag ) {
   
   int rc = 0;
   
   for( unsigned int i = 0; i < dag->size(); i++ ) {
      
      rc = AG_fs_remove_DAG_node_data( dest, dag->at(i) );
      if( rc != 0 ) {
         errorf("AG_fs_remove_DAG_node_data(depth=%d) rc = %d\n", i, rc );
         break;
      }
   }
   
   return rc;
}


// generate a dag with the requested method, run it, and validate the response.
static int AG_fs_apply_DAG( struct ms_client* client, int (*DAG_generator)( struct ms_client*, AG_fs_map_t*, AG_fs_map_t*, AG_request_DAG_node_list_t* ),
                            AG_fs_map_t* directives, AG_fs_map_t* reference,
                            AG_request_DAG_node_list_t* dag ) {
   
   int rc = 0;
   
   // run the generator 
   rc = (*DAG_generator)( client, directives, reference, dag );
   if( rc != 0 ) {
      errorf("DAG generator rc = %d\n", rc );
      return rc;
   }
   
   // run it and validate it
   // TODO: get these constants from somewhere 
   rc = AG_run_DAG( client, dag, 10, 3 );
   if( rc != 0 ) {
      errorf("AG_run_DAG rc = %d\n", rc );
      
      AG_request_DAG_node_list_free( dag );
      return rc;
   }
   
   return 0;
}


// create a mapping of data on the MS, and put any obtained data into the AG fs.
// current_mappings must have the AG-specific information for all paths to create, since 
// we're going to use its data plus the data the MS returns to map the paths in ag_fs.
int AG_fs_create_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_publish, AG_fs_map_t* current_mappings ) {
   
   AG_request_DAG_node_list_t dag;
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
   
   // get the DAG 
   rc = AG_fs_apply_DAG( client, AG_generate_DAG_create, &merged_publish, current_mappings, &dag );
   
   AG_fs_map_free( &merged_publish );
   
   if( rc != 0 ) {
      errorf("AG_fs_apply_DAG(create) rc = %d\n", rc );
      
      return rc;
   }
   
   // add everything.
   rc = AG_fs_add_DAG( dest, to_publish, &dag );
   if( rc != 0 ) {
      errorf("AG_fs_add_DAG() rc = %d\n", rc );
      
      AG_request_DAG_node_list_free( &dag );
      
      return rc;
   }
   
   // success!
   AG_request_DAG_node_list_free( &dag );
   return rc;
}


// update a mapping of data on the MS, and put new data into the AG fs.
// everything in to_update must be coherent
// current_mappings must have the AG-specific information for all paths to update, 
// since we're going to use its data plus the data the MS returns.
int AG_fs_update_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_update, AG_fs_map_t* current_mappings ) {
   
   AG_request_DAG_node_list_t dag;
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
   
   // get the DAG 
   rc = AG_fs_apply_DAG( client, AG_generate_DAG_update, to_update, current_mappings, &dag );
   
   if( rc != 0 ) {
      errorf("AG_fs_apply_DAG(update) rc = %d\n", rc );
      
      return rc;
   }
   
   // mark everything as updated locally
   rc = AG_fs_mark_all_updated( dest, to_update );
   if( rc != 0 ) {
      errorf("AG_fs_mark_all_updated() rc = %d\n", rc );
      
      AG_request_DAG_node_list_free( &dag );
      
      return rc;
   }
   
   // success!
   AG_request_DAG_node_list_free( &dag );
   return rc;
}


// delete a mapping of data on the MS, and delete the corresponding entries in a destination fs_map
// current_mappings must have the AG-specific information for all paths to delete.
int AG_fs_delete_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_delete, AG_fs_map_t* current_mappings ) {
   
   AG_request_DAG_node_list_t dag;
   int rc = 0;
   
   // get the DAG 
   rc = AG_fs_apply_DAG( client, AG_generate_DAG_delete, to_delete, current_mappings, &dag );
   if( rc != 0 ) {
      errorf("AG_fs_apply_DAG(delete) rc = %d\n", rc );
      
      return rc;
   }
   
   // remove everything
   rc = AG_fs_remove_DAG( dest, &dag );
   if( rc != 0 ) {
      errorf("AG_fs_remove_DAG rc = %d\n", rc );
      
      AG_request_DAG_node_list_free( &dag );
      
      return rc;
   }
   
   // success!
   AG_request_DAG_node_list_free( &dag );
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
