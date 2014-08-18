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

#include "libsyndicate/download.h"

static void* md_downloader_main( void* arg );
int md_downloader_finalize_download_context( struct md_download_context* dlctx, int curl_rc );

// locks around the downloading contexts 
int md_downloader_downloading_rlock( struct md_downloader* dl ) {
   return pthread_rwlock_rdlock( &dl->downloading_lock );
}

int md_downloader_downloading_wlock( struct md_downloader* dl ) {
   return pthread_rwlock_wrlock( &dl->downloading_lock );
}

int md_downloader_downloading_unlock( struct md_downloader* dl ) {
   return pthread_rwlock_unlock( &dl->downloading_lock );
}

// locks around the pending contexts 
int md_downloader_pending_rlock( struct md_downloader* dl ) {
   return pthread_rwlock_rdlock( &dl->pending_lock );
}

int md_downloader_pending_wlock( struct md_downloader* dl ) {
   return pthread_rwlock_wrlock( &dl->pending_lock );
}

int md_downloader_pending_unlock( struct md_downloader* dl ) {
   return pthread_rwlock_unlock( &dl->pending_lock );
}

// locks around the cancelling contexts 
int md_downloader_cancelling_rlock( struct md_downloader* dl ) {
   return pthread_rwlock_rdlock( &dl->cancelling_lock );
}

int md_downloader_cancelling_wlock( struct md_downloader* dl ) {
   return pthread_rwlock_wrlock( &dl->cancelling_lock );
}

int md_downloader_cancelling_unlock( struct md_downloader* dl ) {
   return pthread_rwlock_unlock( &dl->cancelling_lock );
}


// set up a downloader 
int md_downloader_init( struct md_downloader* dl, char const* name ) {
   memset( dl, 0, sizeof(struct md_downloader) );
   
   dl->name = strdup( name );
   dl->downloading = new md_downloading_map_t();
   dl->pending = new md_pending_set_t();
   dl->cancelling = new md_pending_set_t();
   
   pthread_rwlock_init( &dl->downloading_lock, NULL );
   pthread_rwlock_init( &dl->pending_lock, NULL );
   pthread_rwlock_init( &dl->cancelling_lock, NULL );
   
   dl->curlm = curl_multi_init();
   
   return 0;
}

// start up a downloader 
int md_downloader_start( struct md_downloader* dl ) {
   dl->running = true;
   
   dl->thread = md_start_thread( md_downloader_main, dl, false );
   if( dl->thread == (pthread_t)(-1) ) {
      errorf("%s: failed to start\n", dl->name);
      return -1;
   }
   
   return 0;
}

// stop a downloader 
int md_downloader_stop( struct md_downloader* dl ) {
   dl->running = false;
   
   int rc = pthread_join( dl->thread, NULL );
   if( rc != 0 ) {
      errorf("%s: pthread_join rc = %d\n", dl->name, rc );
      return rc;
   }
   
   return 0;
}

// signal every element of a pending_set 
static int md_signal_pending_set( md_pending_set_t* ps ) {
   
   // signal each waiting thread 
   for( md_pending_set_t::iterator itr = ps->begin(); itr != ps->end(); itr++ ) {
      
      struct md_download_context* dlctx = *itr;
      
      sem_post( &dlctx->sem );
   }
   
   return 0;
}


// shut down a downloader.
// stops all CURL transfers abruptly.
int md_downloader_shutdown( struct md_downloader* dl ) {
   if( dl->running ) {
      // not stopped yet 
      return -EINVAL;
   }
   
   // destroy downloading
   md_downloader_downloading_wlock( dl );
   
   if( dl->downloading ) {
      
      // remove each running download and signal the waiting threads 
      for( md_downloading_map_t::iterator itr = dl->downloading->begin(); itr != dl->downloading->end(); itr++ ) {
         
         struct md_download_context* dlctx = itr->second;
         
         curl_multi_remove_handle( dl->curlm, dlctx->curl );
         
         sem_post( &dlctx->sem );
      }
      
      dl->downloading->clear();
      
      delete dl->downloading;
      dl->downloading = NULL;
   }
   
   if( dl->curlm ) {
      curl_multi_cleanup( dl->curlm );
      dl->curlm = NULL;
   }
   
   md_downloader_downloading_unlock( dl );
   
   // destroy pending
   md_downloader_pending_wlock( dl );
   
   if( dl->pending ) {
      
      // signal each waiting thread 
      md_signal_pending_set( dl->pending );
      
      dl->pending->clear();
      delete dl->pending;
      dl->pending = NULL;
   }
   
   md_downloader_pending_unlock( dl );
   
   md_downloader_cancelling_wlock( dl );
   
   // destroy cancelling 
   if( dl->cancelling ) {
      
      // signal each waiting thread 
      md_signal_pending_set( dl->cancelling );
    
      dl->cancelling->clear();
      delete dl->cancelling;
      dl->cancelling = NULL;
   }
   
   md_downloader_cancelling_unlock( dl );
   
   // misc
   if( dl->name ) {
      free( dl->name );
      dl->name = NULL;
   }
   
   pthread_rwlock_destroy( &dl->downloading_lock );
   pthread_rwlock_destroy( &dl->pending_lock );
   pthread_rwlock_destroy( &dl->cancelling_lock );
   
   memset( dl, 0, sizeof(struct md_downloader) );
   
   return 0;
}


// insert a pending context 
int md_downloader_insert_pending( struct md_downloader* dl, struct md_download_context* dlctx ) {
   
   md_downloader_pending_wlock( dl );
   
   if( !dl->running ) {
      md_downloader_pending_unlock( dl );
      return -EPERM;
   }
   
   if( dlctx->finalized ) {
      md_downloader_pending_unlock( dl );
      return -EINVAL;
   }
   
   if( dlctx->pending || dlctx->cancelling ) {
      md_downloader_pending_unlock( dl );
      return -EINVAL;
   }
   
   dlctx->pending = true;
   dl->pending->insert( dlctx );
   
   md_downloader_pending_unlock( dl );
   
   dl->has_pending = true;
   
   dbprintf("Start context %p\n", dlctx );
   
   return 0;
}


// insert a cancelling context 
int md_downloader_insert_cancelling( struct md_downloader* dl, struct md_download_context* dlctx ) {
   md_downloader_cancelling_wlock( dl );
   
   if( !dl->running ) {
      md_downloader_cancelling_unlock( dl );
      return -EPERM;
   }
   
   if( dlctx->finalized ) {
      md_downloader_cancelling_unlock( dl );
      return -EINVAL;
   }
   
   if( dlctx->pending || dlctx->cancelling ) {
      md_downloader_cancelling_unlock( dl );
      return -EINVAL;
   }
   
   dlctx->cancelling = true;
   
   dl->cancelling->insert( dlctx );
   
   md_downloader_cancelling_unlock( dl );
   
   dl->has_cancelling = true;
   
   dbprintf("Cancel context %p\n", dlctx );
   
   return 0;
}

// add all pending to downloading 
// dl->downloading_lock MUST BE WRITE LOCKED
int md_downloader_start_all_pending( struct md_downloader* dl ) {
   if( dl->has_pending ) {
         
      md_downloader_pending_wlock( dl );
      
      for( md_pending_set_t::iterator itr = dl->pending->begin(); itr != dl->pending->end(); itr++ ) {
         
         struct md_download_context* dlctx = *itr;
         
         curl_multi_add_handle( dl->curlm, dlctx->curl );
         dlctx->pending = false;
         
         (*dl->downloading)[ dlctx->curl ] = dlctx;
      }
      
      dl->pending->clear();
      
      md_downloader_pending_unlock( dl );
      
      dl->has_pending = false;
   }
   
   return 0;
}

// remove all cancelling downloads from downloading.
// dl->downloading_lock MUST BE WRITE LOCKED 
int md_downloader_end_all_cancelling( struct md_downloader* dl ) {
   if( dl->has_cancelling ) {
         
      md_downloader_cancelling_wlock( dl );
      
      for( md_pending_set_t::iterator itr = dl->cancelling->begin(); itr != dl->cancelling->end(); itr++ ) {
         
         struct md_download_context* dlctx = *itr;
         
         if( dlctx == NULL )
            continue;
         
         curl_multi_remove_handle( dl->curlm, dlctx->curl );
         
         dl->downloading->erase( dlctx->curl );
         
         // update state
         dlctx->cancelling = false;
         dlctx->cancelled = true;
         
         // finalize, with -EAGAIN
         md_downloader_finalize_download_context( dlctx, -EAGAIN );
      }
      
      dl->cancelling->clear();
   
      md_downloader_cancelling_unlock( dl );
      
      dl->has_cancelling = false;
   }
   
   return 0;
}


// download data to a response buffer
size_t md_get_callback_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {
   response_buffer_t* rb = (response_buffer_t*)user_data;

   size_t realsize = size * count;
   char* buf = CALLOC_LIST( char, realsize );
   memcpy( buf, stream, realsize );
   
   rb->push_back( buffer_segment_t( buf, realsize ) );

   return realsize;
}

// download to a bound response buffer
size_t md_get_callback_bound_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {
   struct md_bound_response_buffer* brb = (struct md_bound_response_buffer*)user_data;
   
   //dbprintf("size = %zu, count = %zu, max_size = %ld, size = %ld\n", size, count, brb->max_size, brb->size );
   
   off_t realsize = size * count;
   if( brb->max_size >= 0 && (off_t)(brb->size + realsize) > brb->max_size ) {
      realsize = brb->max_size - brb->size;
   }
   
   char* buf = CALLOC_LIST( char, realsize );
   memcpy( buf, stream, realsize );
   
   brb->rb->push_back( buffer_segment_t( buf, realsize ) );
   brb->size += realsize;
   
   return realsize;
}


// download data to a buffer
size_t md_default_get_callback_ram(void *stream, size_t size, size_t count, void *user_data) {
   struct md_download_buf* dlbuf = (struct md_download_buf*)user_data;
   
   size_t realsize = size * count;
   
   int new_size = realsize + dlbuf->len;
   
   if( dlbuf->data_len > 0 ) {
      // have an upper bound on how much data to copy
      if( dlbuf->len + realsize > (size_t)dlbuf->data_len ) {
         realsize = dlbuf->data_len - dlbuf->len;
      }
      dbprintf("receive %zu to offset %zd of %zd\n", realsize, dlbuf->len, dlbuf->data_len);
      memcpy( dlbuf->data + dlbuf->len, stream, realsize );
      dlbuf->len += realsize;
      return realsize;
   }
   else {
      // expand   
      char* new_buf = (char*)realloc( dlbuf->data, new_size );
      if( new_buf == NULL ) {
         free( dlbuf->data );
         dlbuf->data = NULL;
         dbprintf("out of memory for %p\n", user_data );
         return 0;      // out of memory
      }

      else {
         dlbuf->data = new_buf;
         memcpy( dlbuf->data + dlbuf->len, stream, realsize );
         dlbuf->len = new_size;
         return realsize;
      }
   }
   
   return 0;
}

// initialize a download context.  Takes a CURL handle from the client.
// The only things it sets in the CURL handle are:
// * CURLOPT_WRITEDATA
// * CURLOPT_WRITEFUNCTION
int md_download_context_init( struct md_download_context* dlctx, CURL* curl, md_cache_connector_func cache_func, void* cache_func_cls, off_t max_len ) {
   memset( dlctx, 0, sizeof(struct md_download_context) );
   
   dlctx->brb.max_size = max_len;
   dlctx->brb.size = 0;
   dlctx->brb.rb = new response_buffer_t();
   
   dlctx->cache_func = cache_func;
   dlctx->cache_func_cls = cache_func_cls;
   
   dlctx->curl = curl;
   
   sem_init( &dlctx->sem, 0, 0 );
   
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEDATA, (void*)&dlctx->brb );
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );
   
   return 0;
}


// reset a download context.
int md_download_context_reset( struct md_download_context* dlctx, CURL* new_curl ) {
   
   response_buffer_free( dlctx->brb.rb );
   dlctx->brb.size = 0;
   
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEDATA, (void*)&dlctx->brb );
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );
   
   dlctx->curl_rc = 0;
   dlctx->http_status = 0;
   dlctx->transfer_errno = 0;
   dlctx->cancelled = false;
   dlctx->finalized = false;
   dlctx->pending = false;
   dlctx->cancelling = false;
   
   if( dlctx->effective_url ) {
      free( dlctx->effective_url );
      dlctx->effective_url = NULL;
   }
   
   if( new_curl ) {
      dlctx->curl = new_curl;
   }
   
   return 0;
}

// free a download context
// this will return -EAGAIN if the download context is queued to be inserted or cancelled.
// if this download context was finalized, then it's guaranteed to be freed
int md_download_context_free( struct md_download_context* dlctx, CURL** curl ) {
   // safe to free?
   if( dlctx->pending || dlctx->cancelling ) {
      return -EAGAIN;
   }
   
   dbprintf("Free context %p\n", dlctx );
   
   if( dlctx->brb.rb ) {
      response_buffer_free( dlctx->brb.rb );
      delete dlctx->brb.rb;
      dlctx->brb.rb = NULL;
      dlctx->brb.size = 0;
   }
   
   if( curl ) {
      *curl = dlctx->curl;
   }
   
   if( dlctx->effective_url ) {
      free( dlctx->effective_url );
      dlctx->effective_url = NULL;
   }
   
   dlctx->curl = NULL;
   
   sem_destroy( &dlctx->sem );
   
   memset( dlctx, 0, sizeof(struct md_download_context));
   
   return 0;
}

// wrapper around sem_wait and sem_trywait.
// if timeout_ms < 0, then use sem_wait.  Otherwise, use sem_trywait +timeout_ms seconds into the future.
int md_download_sem_wait( sem_t* sem, int64_t timeout_ms ) {
   
   int rc = 0;
   
   // do we timeout the wait?
   if( timeout_ms > 0 ) {
      
      struct timespec abs_ts;
      clock_gettime( CLOCK_REALTIME, &abs_ts );
      abs_ts.tv_sec += timeout_ms / 1000L;
      abs_ts.tv_nsec += timeout_ms / 1000000L;
      
      if( abs_ts.tv_nsec >= 1000000000L) {
         abs_ts.tv_nsec %= 1000000000L;
         abs_ts.tv_sec ++;
      }
      
      while( true ) {
         rc = sem_timedwait( sem, &abs_ts );
         if( rc == 0 ) {
            break;
         }
         else if( errno != EINTR ) {
            rc = -errno;
            errorf("sem_timedwait errno = %d\n", rc );
            break;
         }
         
         // otherwise, try again if interrupted
      }
   }
   else {
      while( true ) {
         rc = sem_wait( sem );
         if( rc == 0 ) {
            break;
         }
         else if( errno != EINTR ) {
            rc = -errno;
            errorf("sem_wait errno = %d\n", rc );
            break;
         }
         
         // otherwise, try again if interrupted
      }
   }
   
   return rc;
}


// wait for a download to finish, either in error or not 
// return the result of waiting, NOT the result of the download 
int md_download_context_wait( struct md_download_context* dlctx, int64_t timeout_ms ) {
   
   int rc = md_download_sem_wait( &dlctx->sem, timeout_ms );
   
   if( rc != 0 ) {
      errorf("md_download_sem_wait rc = %d\n", rc );
   }
   return rc;
}


// wait for a download to finish within a download set, either in error or not.
// return the result of waiting, NOT the result of the download 
int md_download_context_wait_any( struct md_download_set* dlset, int64_t timeout_ms ) {
   
   int rc = 0;
   
   // wait for at least one of them to finish 
   rc = md_download_sem_wait( &dlset->sem, timeout_ms );
   
   if( rc != 0 ) {
      errorf("md_download_sem_wait rc = %d\n", rc );
   }
   
   return rc;
}

// set up a download set 
int md_download_set_init( struct md_download_set* dlset ) {
   dlset->waiting = new md_pending_set_t();
   
   sem_init( &dlset->sem, 0, 0 );
   
   return 0;
}


// free a download set 
int md_download_set_free( struct md_download_set* dlset ) {
   
   if( dlset->waiting ) {
      delete dlset->waiting;
      dlset->waiting = NULL;
   }
   
   sem_destroy( &dlset->sem );
   
   memset( dlset, 0, sizeof( struct md_download_set ) );
   
   return 0;
}


// add a download context to a download set 
int md_download_set_add( struct md_download_set* dlset, struct md_download_context* dlctx ) {
   
   md_pending_set_t::iterator itr = dlset->waiting->find( dlctx );
   if( itr == dlset->waiting->end() ) {
      dlset->waiting->insert( dlctx );
      
      dlctx->dlset = dlset;
   }
   
   return 0;
}


// remove a download context from a download set by iterator
int md_download_set_clear_itr( struct md_download_set* dlset, const md_download_set_iterator& itr ) {
   
   struct md_download_context* dlctx = *itr;
   dlset->waiting->erase( itr );
   dlctx->dlset = NULL;
   
   return 0;
}

// remove a download context from a download set by value
// don't do this in e.g. a for() loop where you're iterating over download sets
int md_download_set_clear( struct md_download_set* dlset, struct md_download_context* dlctx ) {
   
   dlset->waiting->erase( dlctx );
   dlctx->dlset = NULL;
   
   return 0;
}


// how many items in a download set?
size_t md_download_set_size( struct md_download_set* dlset ) {
   
   return dlset->waiting->size();
}


// iterate: begin 
md_download_set_iterator md_download_set_begin( struct md_download_set* dlset ) {
   return dlset->waiting->begin();
}

// iterate: end 
md_download_set_iterator md_download_set_end( struct md_download_set* dlset ) {
   return dlset->waiting->end();
}

// iterate: deref 
struct md_download_context* md_download_set_iterator_get_context( const md_download_set_iterator& itr ) {
   return *itr;
}


// connect a donwload context to the caches 
static int md_download_context_connect_cache( struct md_downloader* dl, struct md_download_context* dlctx, struct md_closure* cache_closure, char const* base_url ) {
   
   int rc = 0;
   
   // connect to the cache 
   if( dlctx->cache_func ) {
      // no closure?
      if( cache_closure == NULL ) {
         return -EINVAL;
      }
      
      // connect to the cache
      rc = (*dlctx->cache_func)( cache_closure, dlctx->curl, base_url, dlctx->cache_func_cls );
      if( rc != 0 ) {
         errorf("%s: cache connect on %s rc = %d\n", dl->name, base_url, rc );
         return rc;
      }
   }
   
   return rc;
}


// begin downloading something 
int md_download_context_start( struct md_downloader* dl, struct md_download_context* dlctx, struct md_closure* cache_closure, char const* base_url ) {
   
   int rc = md_download_context_connect_cache( dl, dlctx, cache_closure, base_url );
   if( rc != 0 ) {
      errorf("%s: md_download_context_connect_cache(%s) rc = %d\n", dl->name, base_url, rc );
      return rc;
   }
   
   // enqueue the context into the downloader 
   md_downloader_insert_pending( dl, dlctx );
   return 0;
}

// cancel downloading something 
int md_download_context_cancel( struct md_downloader* dl, struct md_download_context* dlctx ) {
   if( dlctx->cancelled || dlctx->finalized )
      return 0;
   
   md_downloader_insert_cancelling( dl, dlctx );
   
   return md_download_context_wait( dlctx, -1 );
}


// release a waiting context set, given one of its now-finished entries.
int md_download_set_wakeup( struct md_download_context* dlctx ) {
   
   int rc = 0;
   
   if( dlctx->dlset != NULL ) {
      sem_post( &dlctx->dlset->sem );
   }
   
   return rc;
}

// run multiple downloads for a bit
// dl->downloading_lock MUST BE WRITE-LOCKED
int md_downloader_run_multi( struct md_downloader* dl ) {
   
   int still_running = 0;
   int rc = 0;
   struct timeval timeout;

   fd_set fdread;
   fd_set fdwrite;
   fd_set fdexcep;
   
   FD_ZERO( &fdread );
   FD_ZERO( &fdwrite );
   FD_ZERO( &fdexcep );

   int maxfd = -1;

   long curl_timeo = -1;
   
   // download for a bit
   curl_multi_perform( dl->curlm, &still_running );

   // don't wait more than 5ms
   timeout.tv_sec = 0;
   timeout.tv_usec = 5000;      // 5ms

   curl_multi_timeout( dl->curlm, &curl_timeo );
   
   if( curl_timeo > 0 ) {
      timeout.tv_sec = curl_timeo / 1000;
      if( timeout.tv_sec > 0 ) {
         timeout.tv_sec = 0;
      }
      
      // no more than 5ms
      timeout.tv_usec = MIN( (curl_timeo % 1000) * 1000, 5000 );
   }

   // get fd set
   rc = curl_multi_fdset( dl->curlm, &fdread, &fdwrite, &fdexcep, &maxfd );
   if( rc != 0 ) {
      errorf("%s: curl_multi_fdset rc = %d\n", dl->name, rc );
      return rc;
   }

   // select on them
   rc = select( maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout );
   if( rc < 0 ) {
      rc = -errno;
      errorf("%s: select errno = %d\n", dl->name, rc );
      return rc;
   }
   else {
      rc = 0;
   }
   return rc;
}


// finalize a download context
int md_downloader_finalize_download_context( struct md_download_context* dlctx, int curl_rc ) {
   int rc = 0;
   
   // check HTTP code
   long http_status = 0;
   long os_errno = 0;
   char* url = NULL;
   
   rc = curl_easy_getinfo( dlctx->curl, CURLINFO_RESPONSE_CODE, &http_status );
   if( rc != 0 ) {
      errorf("curl_easy_getinfo rc = %d\n", rc );
      http_status = -1;
   }
   
   // check error code 
   if( rc != 0 ) {
      rc = curl_easy_getinfo( dlctx->curl, CURLINFO_OS_ERRNO, &os_errno );
         
      if( rc != 0 ) {
         errorf("curl_easy_getinfo rc = %d\n", rc );
         os_errno = EIO;
      }
   }
   
   // get URL 
   rc = curl_easy_getinfo( dlctx->curl, CURLINFO_EFFECTIVE_URL, &url );
   
   if( rc != 0 ) {
      errorf("curl_easy_getinfo rc = %d\n", rc );
      os_errno = EIO;
   }
   
   dlctx->curl_rc = curl_rc;
   dlctx->http_status = (int)http_status;
   dlctx->transfer_errno = (int)os_errno;
   dlctx->finalized = true;
   
   if( url != NULL ) {
      dlctx->effective_url = strdup( url );
   }
   
   // release waiting thread
   sem_post( &dlctx->sem );
   
   return rc;
}


// finalize all finished downloads 
// dl->downloading_lock MUST BE WRITE-LOCKED
int md_downloader_finalize_download_contexts( struct md_downloader* dl ) {
   CURLMsg* msg = NULL;
   int msgs_left = 0;
   int rc = 0;
   
   do {
      msg = curl_multi_info_read( dl->curlm, &msgs_left );

      if( msg == NULL )
         break;

      if( msg->msg == CURLMSG_DONE ) {
         // a transfer finished.  Find out which one
         md_downloading_map_t::iterator itr = dl->downloading->find( msg->easy_handle );
         if( itr != dl->downloading->end() ) {
            // found!
            struct md_download_context* dlctx = itr->second;
            
            // get this now, before removing it from the curlm handle
            int result = msg->data.result;
            
            // remove from the downloader 
            dl->downloading->erase( itr );
            
            if( dlctx == NULL ) {
               errorf("WARN: no download context for curl handle %p\n", msg->easy_handle);
               
               curl_multi_remove_handle( dl->curlm, msg->easy_handle );
               continue;
            }
            
            if( dlctx->curl == NULL ) {
               errorf("BUG: curl handle of download context %p is NULL\n", dlctx );
               
               curl_multi_remove_handle( dl->curlm, msg->easy_handle );
            }
            else {
               curl_multi_remove_handle( dl->curlm, dlctx->curl );
            }
            
            // finalize the download context
            rc = md_downloader_finalize_download_context( dlctx, result );
            if( rc != 0 ) {
               errorf("%s: md_downloader_finalize_download_context rc = %d\n", dl->name, rc );
            }
            
            // wake up the set waiting on this dlctx 
            md_download_set_wakeup( dlctx );
         }
      } 
      
   } while( msg != NULL );
   
   return rc;
}


// main downloader loop
static void* md_downloader_main( void* arg ) {
   struct md_downloader* dl = (struct md_downloader*)arg;
   
   dbprintf("%s: starting\n", dl->name );
   
   int rc = 0;
   
   while( dl->running ) {
      md_downloader_downloading_wlock( dl );
      
      // add all pending downloads to this downloader 
      rc = md_downloader_start_all_pending( dl );
      if( rc != 0 ) {
         errorf("%s: md_downloader_start_all_pending rc = %d\n", dl->name, rc );
      }
      
      // remove all cancelled downloads from this downloader 
      rc = md_downloader_end_all_cancelling( dl );
      if( rc != 0 ) {
         errorf("%s: md_downloader_end_all_cancelling rc = %d\n", dl->name, rc );
      }
      
      // download for a bit 
      rc = md_downloader_run_multi( dl );
      if( rc != 0 ) {
         errorf("%s: md_downloader_run_multi rc = %d\n", dl->name, rc );
      }
      
      // finalize any completed downloads 
      md_downloader_finalize_download_contexts( dl );
      if( rc != 0 ) {
         errorf("%s: md_downloader_finalize_download_contexts rc = %d\n", dl->name, rc );
      }
      
      md_downloader_downloading_unlock( dl );
      
      // give the md_downloader_stop() method a chance to preempt the main method
   }
   
   dbprintf("%s: exiting\n", dl->name );
   return NULL;
}


// consolidate and write back the buffer 
int md_download_context_get_buffer( struct md_download_context* dlctx, char** buf, off_t* buf_len ) {
   *buf = response_buffer_to_string( dlctx->brb.rb );
   *buf_len = response_buffer_size( dlctx->brb.rb );
   return 0;
}

// get the http status
int md_download_context_get_http_status( struct md_download_context* dlctx ) {
   if( !dlctx->finalized )
      return -EAGAIN;
   
   return dlctx->http_status;
}

// get the errno 
int md_download_context_get_errno( struct md_download_context* dlctx ) {
   if( !dlctx->finalized )
      return -EAGAIN;
   
   return dlctx->transfer_errno;
}

// get the curl rc
int md_download_context_get_curl_rc( struct md_download_context* dlctx ) {
   if( !dlctx->finalized )
      return -EAGAIN;
   
   return dlctx->curl_rc;
}

// get the effective URL 
int md_download_context_get_effective_url( struct md_download_context* dlctx, char** url ) {
   if( !dlctx->finalized )
      return -EAGAIN;
   
   if( dlctx->effective_url == NULL )
      *url = NULL;
   else 
      *url = strdup( dlctx->effective_url );
   
   return 0;
}

// get the cache cls (only in reference)
void* md_download_context_get_cache_cls( struct md_download_context* dlctx ) {
   return dlctx->cache_func_cls;
}

// did a download context work?
bool md_download_context_succeeded( struct md_download_context* dlctx, int desired_HTTP_status ) {
   return (dlctx->curl_rc == 0 && dlctx->transfer_errno == 0 && dlctx->http_status == desired_HTTP_status); 
}

// is a download finalized?
bool md_download_context_finalized( struct md_download_context* dlctx ) {
   return dlctx->finalized;
}

// initialze a curl handle
void md_init_curl_handle( struct md_syndicate_conf* conf, CURL* curl_h, char const* url, time_t query_timeout ) {
   md_init_curl_handle2( curl_h, url, query_timeout, conf->verify_peer );
}

// initialze a curl handle
void md_init_curl_handle2( CURL* curl_h, char const* url, time_t query_timeout, bool ssl_verify_peer ) {
   curl_easy_setopt( curl_h, CURLOPT_NOPROGRESS, 1L );   // no progress bar
   curl_easy_setopt( curl_h, CURLOPT_USERAGENT, "Syndicate-Gateway/1.0");
   curl_easy_setopt( curl_h, CURLOPT_URL, url );
   curl_easy_setopt( curl_h, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( curl_h, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( curl_h, CURLOPT_CONNECTTIMEOUT, query_timeout );
   curl_easy_setopt( curl_h, CURLOPT_FILETIME, 1L );
   
   if( url != NULL && strncasecmp( url, "https", 5 ) == 0 ) {
      curl_easy_setopt( curl_h, CURLOPT_USE_SSL, CURLUSESSL_ALL );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, ssl_verify_peer ? 1L : 0L );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYHOST, 2L );
   }
   else {
      curl_easy_setopt( curl_h, CURLOPT_USE_SSL, CURLUSESSL_NONE );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, 0L );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYHOST, 0L );
   }
   
   //curl_easy_setopt( curl_h, CURLOPT_VERBOSE, 1L );
}


// download straight from an existing curl handle
off_t md_download_file2( CURL* curl_h, char** buf, off_t max_len ) {
   struct md_download_buf dlbuf;
   dlbuf.len = 0;
   
   if( max_len > 0 ) {
      dbprintf("Download max %zd\n", max_len );
      dlbuf.data = CALLOC_LIST( char, max_len );
      dlbuf.data_len = max_len;
   }
   else {
      dlbuf.data = CALLOC_LIST( char, 1 );
      dlbuf.data_len = -1;
   }

   if( dlbuf.data == NULL ) {
      return -1;
   }

   curl_easy_setopt( curl_h, CURLOPT_WRITEDATA, (void*)&dlbuf );
   curl_easy_setopt( curl_h, CURLOPT_WRITEFUNCTION, md_default_get_callback_ram );
   int rc = curl_easy_perform( curl_h );

   if( rc != 0 ) {
      dbprintf("curl_easy_perform rc = %d\n", rc);
      free( dlbuf.data );
      dlbuf.data = NULL;
      return -abs(rc);
   }

   *buf = dlbuf.data;
   return dlbuf.len;
}

// wrapper around md_download_file2
off_t md_download_file( CURL* curl_h, char** buf ) {
   return md_download_file2( curl_h, buf, -1 );
}


// download data from one or more CDNs.
// cache_func will initialize the curl handle.
// return 0 on success.
// return negative on error.
// fill in the HTTP satus code
int md_download_from_caches( struct md_downloader* dl, struct md_closure* closure, CURL* curl, char const* base_url, char** bits, off_t* ret_len, off_t max_len, int* status_code,
                             md_cache_connector_func cache_func, void* cache_func_cls ) {
   int rc = 0;

   struct md_download_context dlctx;
   md_download_context_init( &dlctx, curl, cache_func, cache_func_cls, max_len );
   
   // connect to cache...
   rc = md_download_context_connect_cache( dl, &dlctx, closure, base_url );
   if( rc != 0 ) {
      errorf("%s: md_download_context_connect_cache(%s) rc = %d\n", dl->name, base_url, rc );
      md_download_context_free( &dlctx, NULL );
      return rc;
   }
   
   // run the download 
   rc = curl_easy_perform( curl );
   md_downloader_finalize_download_context( &dlctx, rc );
   
   // check for errors 
   rc = md_download_context_get_curl_rc( &dlctx );
   if( rc != 0 ) {
      int errsv = md_download_context_get_errno( &dlctx );
      
      errorf("%s: md_download_context_wait(%s) CURL rc = %d, errno = %d\n", dl->name, base_url, rc, errsv );
      
      if( errsv == 0 ) {
         if( rc == CURLE_COULDNT_CONNECT ) {
            errsv = -ECONNREFUSED;
         }
         else {
            errsv = -EINVAL;       // CURL was not properly set up
         }
      }
      md_download_context_free( &dlctx, NULL );
      return errsv;
   }
   
   // give back the data 
   rc = md_download_context_get_buffer( &dlctx, bits, ret_len );
   if( rc != 0 ) {
      errorf("%s: md_download_context_get_buffer(%s) rc = %d\n", dl->name, base_url, rc );
      
      md_download_context_free( &dlctx, NULL );
      return -ENODATA;
   }
   
   *status_code = md_download_context_get_http_status( &dlctx );
   
   md_download_context_free( &dlctx, NULL );
   
   return 0;
}


// download data from one or more CDNs, and then fall back to a direct download if that fails.
// return 0 on success.
// return negative on error.
// fill in the HTTP status code 
int md_download( struct md_syndicate_conf* conf, struct md_downloader* dl, struct md_closure* closure, CURL* curl, char const* base_url, char** bits, off_t* ret_len, off_t max_len, int* status_code,
                 md_cache_connector_func cache_func, void* cache_func_cls ) {
   int rc = 0;
   
   if( cache_func ) {
      rc = md_download_from_caches( dl, closure, curl, base_url, bits, ret_len, max_len, status_code, cache_func, cache_func_cls );
      if( rc == 0 ) {
         return 0;
      }
      else {
         errorf("WARN: md_download_from_caches(%s) rc = %d, falling back to direct download\n", base_url, rc);
      }
   }
   
   // download directly if we get here...
   md_init_curl_handle( conf, curl, base_url, conf->connect_timeout );
   
   rc = md_download_from_caches( dl, NULL, curl, base_url, bits, ret_len, max_len, status_code, NULL, NULL );
   if( rc != 0 ) {
      errorf("md_download_from_caches(%s) rc = %d, HTTP status = %d\n", base_url, rc, *status_code );
   }
   
   return rc;
}


// translate an HTTP status code into the approprate error code.
// return the code if no error could be determined.
static int md_HTTP_status_code_to_error_code( int status_code ) {
   if( status_code == GATEWAY_HTTP_TRYAGAIN )
      return -EAGAIN;
   
   if( status_code == GATEWAY_HTTP_EOF )
      return 0;
   
   if( status_code == 500 )
      return -EREMOTEIO;
   
   if( status_code == 404 )
      return -ENOENT;
   
   return status_code;
}

// download a manifest and parse it.
// Do not attempt to check it for errors, or verify its authenticity
int md_download_manifest( struct md_syndicate_conf* conf, struct md_downloader* dl, struct md_closure* closure,
                          CURL* curl, char const* manifest_url, Serialization::ManifestMsg* mmsg,
                          md_cache_connector_func cache_func, void* cache_func_cls,
                          md_manifest_processor_func manifest_func, void* manifest_func_cls ) {

   char* manifest_data = NULL;
   int status_code = 0;
   off_t manifest_data_len = 0;
   int rc = 0;

   rc = md_download( conf, dl, closure, curl, manifest_url, &manifest_data, &manifest_data_len, SYNDICATE_MAX_MANIFEST_LEN, &status_code, cache_func, cache_func_cls );
   
   if( rc != 0 ) {
      errorf( "md_download(%s) rc = %d\n", manifest_url, rc );
      return rc;
   }
   
   if( status_code != 200 ) {
      // bad HTTP status
      errorf( "md_download(%s) HTTP status %d\n", manifest_url, status_code );
      
      if( manifest_data )
         free( manifest_data );
      
      int err = md_HTTP_status_code_to_error_code( status_code );
      if( err == 0 || err == status_code )
         return -EREMOTEIO;
      else
         return err;
   }
   
   // process the manifest...
   if( manifest_func ) {
      char* processed_manifest_data = NULL;
      size_t processed_manifest_data_len = 0;
      
      rc = (*manifest_func)( closure, manifest_data, manifest_data_len, &processed_manifest_data, &processed_manifest_data_len, manifest_func_cls );
      if( rc != 0 ) {
         errorf("manifest_func rc = %d\n", rc );
         
         free( manifest_data );
         return rc;
      }
      
      if( processed_manifest_data != NULL && processed_manifest_data != manifest_data ) {
         // driver transformed the data
         free( manifest_data );
         manifest_data = NULL;
      
         manifest_data = processed_manifest_data;
         manifest_data_len = processed_manifest_data_len;
      }
   }

   rc = md_parse< Serialization::ManifestMsg >( mmsg, manifest_data, manifest_data_len );
   if( rc != 0 ) {
      errorf("md_parse rc = %d\n", rc );
      
      if( manifest_data )
         free( manifest_data );
      
      return -ENODATA;
   }
   
   if( manifest_data )
      free( manifest_data );

   return rc;
}
