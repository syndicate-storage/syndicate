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
// return 0 on success
// return -ENOMEM on OOM 
int md_downloader_init( struct md_downloader* dl, char const* name ) {
   
   int rc = 0;
   
   memset( dl, 0, sizeof(struct md_downloader) );
   
   rc = pthread_rwlock_init( &dl->downloading_lock, NULL );
   if( rc != 0 ) {
      
      return -rc;
   }
   
   rc = pthread_rwlock_init( &dl->pending_lock, NULL );
   if( rc != 0 ) {
      
      pthread_rwlock_destroy( &dl->downloading_lock );
      return -rc;
   }
   
   rc = pthread_rwlock_init( &dl->cancelling_lock, NULL );
   if( rc != 0 ) {
      
      pthread_rwlock_destroy( &dl->downloading_lock );
      pthread_rwlock_destroy( &dl->pending_lock );
      return -rc;
   }
   
   dl->curlm = curl_multi_init();
   
   dl->name = SG_strdup_or_null( name );
   dl->downloading = SG_safe_new( md_downloading_map_t() );
   dl->pending = SG_safe_new( md_pending_set_t() );
   dl->cancelling = SG_safe_new( md_pending_set_t() );
   
   if( dl->name == NULL || dl->downloading == NULL || dl->pending == NULL || dl->cancelling == NULL || dl->curlm == NULL ) {
      
      SG_safe_free( dl->name );
      SG_safe_free( dl->downloading );
      SG_safe_free( dl->pending );
      SG_safe_free( dl->cancelling );
      
      if( dl->curlm != NULL ) {
         curl_multi_cleanup( dl->curlm );
      }
      
      pthread_rwlock_destroy( &dl->downloading_lock );
      pthread_rwlock_destroy( &dl->pending_lock );
      pthread_rwlock_destroy( &dl->cancelling_lock );
      
      return -ENOMEM;
   }
   
   dl->inited = true;
   return 0;
}

// start up a downloader 
// return 0 on success
// return -1 if we failed to start the thread, or if we're already running 
int md_downloader_start( struct md_downloader* dl ) {
   
   if( !dl->running ) {
      
      dl->running = true;
      
      dl->thread = md_start_thread( md_downloader_main, dl, false );
      if( dl->thread == (pthread_t)(-1) ) {
         
         SG_error("%s: failed to start\n", dl->name);
         dl->running = false;
         return -1;
      }
   }
   
   return 0;
}

// stop a downloader 
// return 0 on success 
// return negative if we failed to join with the downloader thread
int md_downloader_stop( struct md_downloader* dl ) {
   
   if( dl->running ) {
      dl->running = false;
      
      int rc = pthread_join( dl->thread, NULL );
      if( rc != 0 ) {
         SG_error("%s: pthread_join rc = %d\n", dl->name, rc );
         return rc;
      }
   }
   
   return 0;
}

// signal every element of a pending_set by incrementing its semaphore
// always succeeds
static int md_signal_pending_set( md_pending_set_t* ps ) {
   
   // signal each waiting thread 
   for( md_pending_set_t::iterator itr = ps->begin(); itr != ps->end(); itr++ ) {
      
      struct md_download_context* dlctx = *itr;
      
      if( dlctx != NULL ) {
         SG_debug("Wakeup %p\n", dlctx);
         sem_post( &dlctx->sem );
      }
   }
   
   return 0;
}


// shut down a downloader.  it must be stopped first.
// stops all CURL transfers abruptly.
// return 0 on success 
// return -EINVAL if the downloader was running, or if the downloader was never initialized
int md_downloader_shutdown( struct md_downloader* dl ) {
   
   if( dl->running ) {
      // not stopped yet 
      return -EINVAL;
   }
   
   if( !dl->inited ) {
      // not initialized
      return -EINVAL;
   }
   
   // destroy downloading
   md_downloader_downloading_wlock( dl );
   
   dl->inited = false;
   
   if( dl->downloading != NULL ) {
      
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
   
   if( dl->curlm != NULL ) {
      curl_multi_cleanup( dl->curlm );
      dl->curlm = NULL;
   }
   
   md_downloader_downloading_unlock( dl );
   
   // destroy pending
   md_downloader_pending_wlock( dl );
   
   if( dl->pending != NULL ) {
      
      // signal each waiting thread 
      md_signal_pending_set( dl->pending );
      
      dl->pending->clear();
      delete dl->pending;
      dl->pending = NULL;
   }
   
   md_downloader_pending_unlock( dl );
   
   md_downloader_cancelling_wlock( dl );
   
   // destroy cancelling 
   if( dl->cancelling != NULL ) {
      
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


// insert a pending context.
// return 0 on success, and mark it as pending and unsafe to free on success
// return -EPERM if the downloader is not running 
// return -EINVAL if the download context is finalized, already pending, or already cancelling
// return -ENOMEM if OOM
int md_downloader_insert_pending( struct md_downloader* dl, struct md_download_context* dlctx ) {
   
   int rc = 0;
   
   md_downloader_pending_wlock( dl );
   
   if( !dl->running ) {
      md_downloader_pending_unlock( dl );
      return -EPERM;
   }
   
   pthread_mutex_lock( &dlctx->finalize_lock );
   
   if( dlctx->finalized ) {
      md_downloader_pending_unlock( dl );
      
      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EINVAL;
   }
   
   if( dlctx->pending || dlctx->cancelling ) {
      md_downloader_pending_unlock( dl );
      
      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EINVAL;
   }
   
   dlctx->pending = true;
   dlctx->safe_to_free = false;
   
   try {
      dl->pending->insert( dlctx );
   }
   catch( bad_alloc& ba ) {
      
      dlctx->pending = false;
      dlctx->safe_to_free = true;
      rc = -ENOMEM;
   }
   
   pthread_mutex_unlock( &dlctx->finalize_lock );
   
   md_downloader_pending_unlock( dl );
   
   if( rc == 0 ) {
      dl->has_pending = true;
   
      SG_debug("Start download context %p\n", dlctx );
   }
   
   return rc;
}


// insert a context to cancel 
// return 0 on success, and mark the download context as cancelling 
// return 0 if the download context is already cancelling
// return -EPERM if the downloader was not running 
// return -EINPROGRESS if the download context is already cancelling 
// return -ENOMEM on OOM
int md_downloader_insert_cancelling( struct md_downloader* dl, struct md_download_context* dlctx ) {
   
   int rc = 0;
   SG_debug("Cancel download context %p\n", dlctx );
   
   md_downloader_cancelling_wlock( dl );
   
   if( !dl->running ) {
      md_downloader_cancelling_unlock( dl );
      return -EPERM;
   }
   
   pthread_mutex_lock( &dlctx->finalize_lock );
   
   if( dlctx->finalized ) {
      md_downloader_cancelling_unlock( dl );
      pthread_mutex_unlock( &dlctx->finalize_lock );
      
      SG_error("WARN: Download context %p is already finalized\n", dlctx );
      return 0;
   }
   
   if( dlctx->cancelling ) {
      md_downloader_cancelling_unlock( dl );
      pthread_mutex_unlock( &dlctx->finalize_lock );
      
      SG_error("WARN: Download context %p is already cancelling\n", dlctx );
      return -EINPROGRESS;
   }
   
   dlctx->cancelling = true;
   
   try {
      if( !dlctx->pending ) {
         dl->cancelling->insert( dlctx );
      }
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
      dlctx->cancelling = false;
   }
   
   pthread_mutex_unlock( &dlctx->finalize_lock );
   
   dl->has_cancelling = true;
   
   md_downloader_cancelling_unlock( dl );
   
   return rc;
}

// add all pending to downloading 
// the downloader must be write-locked for downloading
// return 0 on success 
// return -EPERM if we failed to insert the curl handle
// return -ENOMEM on OOM 
int md_downloader_start_all_pending( struct md_downloader* dl ) {
   
   int rc = 0;
   
   if( dl->has_pending ) {
         
      md_downloader_pending_wlock( dl );
      
      for( md_pending_set_t::iterator itr = dl->pending->begin(); itr != dl->pending->end(); itr++ ) {
         
         struct md_download_context* dlctx = *itr;
         
         if( dlctx == NULL ) {
            continue;
         }
         
         pthread_mutex_lock( &dlctx->finalize_lock );
         
         if( md_download_context_finalized( dlctx ) ) {
            
            pthread_mutex_unlock( &dlctx->finalize_lock );
            continue;
         }
         
         if( dlctx->cancelling ) {
            // got cancelled quickly after insertion 
            
            dlctx->cancelled = true;
            dlctx->cancelling = false;
            
            pthread_mutex_unlock( &dlctx->finalize_lock );
            
            md_downloader_finalize_download_context( dlctx, -EAGAIN );
            
            continue;
         }
         
         // add the handle
         rc = curl_multi_add_handle( dl->curlm, dlctx->curl );
         if( rc != 0 ) {
            rc = -EPERM;
            
            SG_error("curl_multi_add_handle( %p ) rc = %d\n", dlctx, rc );
            
            pthread_mutex_unlock( &dlctx->finalize_lock );
            break;
         }
         
         dlctx->running = true;
         dlctx->pending = false;
         dlctx->safe_to_free = false;
         
         pthread_mutex_unlock( &dlctx->finalize_lock );
         
         try {
            (*dl->downloading)[ dlctx->curl ] = dlctx;
         }
         catch( bad_alloc& ba ) {
            rc = -ENOMEM;
            break;
         }
      }
      
      dl->pending->clear();
      
      dl->has_pending = false;
      
      md_downloader_pending_unlock( dl );
   }
   
   return rc;
}


// remove all cancelling downloads from downloading.
// the downloader must be write-locked for downloading 
// return 0 on success 
// return -EPERM if we fail to remove the curl handle
// return -ENOMEM for OOM 
int md_downloader_end_all_cancelling( struct md_downloader* dl ) {
   
   int rc = 0;
   
   if( dl->has_cancelling ) {
         
      md_downloader_cancelling_wlock( dl );
      
      for( md_pending_set_t::iterator itr = dl->cancelling->begin(); itr != dl->cancelling->end(); itr++ ) {
         
         struct md_download_context* dlctx = *itr;
         
         if( dlctx == NULL ) {
            continue;
         }
         
         rc = curl_multi_remove_handle( dl->curlm, dlctx->curl );
         if( rc != 0 ) {
            
            rc = -EPERM;
            SG_error("curl_multi_remove_handle( %p ) rc = %d\n", dlctx, rc );
            
            break;
         }
         
         try {
            dl->downloading->erase( dlctx->curl );
         }
         catch( bad_alloc& ba ) {
            rc = -ENOMEM;
            break;
         }
         
         pthread_mutex_lock( &dlctx->finalize_lock );
         
         // update state
         dlctx->cancelled = true;
         dlctx->cancelling = false;
         
         if( !md_download_context_finalized( dlctx ) ) {
            
            pthread_mutex_unlock( &dlctx->finalize_lock );
            
            // finalize, with -EAGAIN
            md_downloader_finalize_download_context( dlctx, -EAGAIN );
         }
         
         else {
            pthread_mutex_unlock( &dlctx->finalize_lock );
         }
      }
      
      dl->cancelling->clear();
      
      dl->has_cancelling = false;
      
      md_downloader_cancelling_unlock( dl );
   }
   
   return 0;
}


// download data to a response buffer
// return size * count on success 
// return 0 on OOM
size_t md_get_callback_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {
   
   md_response_buffer_t* rb = (md_response_buffer_t*)user_data;

   size_t realsize = size * count;
   char* buf = SG_CALLOC( char, realsize );
   
   if( buf == NULL ) {
      // OOM
      return 0;
   }
   
   memcpy( buf, stream, realsize );
   
   try {
      rb->push_back( md_buffer_segment_t( buf, realsize ) );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( buf );
      return 0;
   }

   return realsize;
}

// download to a bound response buffer
// return size * count on success 
// return 0 on OOM
size_t md_get_callback_bound_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {
   
   struct md_bound_response_buffer* brb = (struct md_bound_response_buffer*)user_data;
   
   //SG_debug("size = %zu, count = %zu, max_size = %ld, size = %ld\n", size, count, brb->max_size, brb->size );
   
   off_t realsize = size * count;
   if( brb->max_size >= 0 && (off_t)(brb->size + realsize) > brb->max_size ) {
      realsize = brb->max_size - brb->size;
   }
   
   char* buf = SG_CALLOC( char, realsize );
   if( buf == NULL ) {
      return 0;
   }
   
   memcpy( buf, stream, realsize );
   
   try {
      brb->rb->push_back( md_buffer_segment_t( buf, realsize ) );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( buf );
      return 0;
   }
   
   brb->size += realsize;
   
   return realsize;
}

// initialize a download context.  Takes a CURL handle from the client.
// The only things it sets in the CURL handle are:
// * CURLOPT_WRITEDATA
// * CURLOPT_WRITEFUNCTION
// return 0 on success 
// return -ENOMEM on OOM 
// return -errno on failure to initialize a pthread lock 
int md_download_context_init( struct md_download_context* dlctx, CURL* curl, md_cache_connector_func cache_func, void* cache_func_cls, off_t max_len ) {
   
   SG_debug("Initialize download context %p\n", dlctx );
   
   int rc = 0;
   memset( dlctx, 0, sizeof(struct md_download_context) );
   
   rc = pthread_mutex_init( &dlctx->finalize_lock, NULL );
   if( rc != 0 ) {
      return -rc;
   }
   
   rc = md_bound_response_buffer_init( &dlctx->brb, max_len );
   if( rc != 0 ) {
      
      pthread_mutex_destroy( &dlctx->finalize_lock );
      return rc;
   }
   
   dlctx->cache_func = cache_func;
   dlctx->cache_func_cls = cache_func_cls;
   
   dlctx->curl = curl;
   
   sem_init( &dlctx->sem, 0, 0 );
   
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEDATA, (void*)&dlctx->brb );
   curl_easy_setopt( dlctx->curl, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );
   
   dlctx->dlset = NULL;
   
   dlctx->safe_to_free = true;
   
   return 0;
}


// reset a download context.
// don't call this until it's finalized
// return 0 on success
// return -EAGAIN if the donload context is not finalized
int md_download_context_reset( struct md_download_context* dlctx, CURL* new_curl ) {
   
   SG_debug("Reset download context %p\n", dlctx );
   
   pthread_mutex_lock( &dlctx->finalize_lock );
   
   if( !md_download_context_finalized( dlctx ) ) {
      SG_error("Download %p not yet finalized\n", dlctx );
      
      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EAGAIN;
   }
   
   md_response_buffer_free( dlctx->brb.rb );
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
   dlctx->running = false;
   dlctx->safe_to_free = true;
   
   if( dlctx->effective_url != NULL ) {
      
      SG_safe_free( dlctx->effective_url );
   }
   
   if( dlctx->__downloader_url != NULL ) {
   
      SG_safe_free( dlctx->__downloader_url );
   }
   
   if( new_curl != NULL ) {
      dlctx->curl = new_curl;
   }
   
   pthread_mutex_unlock( &dlctx->finalize_lock );
   
   return 0;
}

// free a download context
// this will return -EAGAIN if the download context is queued to be inserted or cancelled.
// if this download context was finalized, then it's guaranteed to be freed
// return 0 on success
// return -EINVAL if the download context is referenced by a download set
int md_download_context_free( struct md_download_context* dlctx, CURL** curl ) {
   
   pthread_mutex_lock( &dlctx->finalize_lock );
   
   // safe to free?
   if( !dlctx->safe_to_free ) {
      SG_error("Download context %p pending=%d, cancelling=%d, running=%d, safe_to_free=%d\n", dlctx, dlctx->pending, dlctx->cancelling, dlctx->running, dlctx->safe_to_free );
      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EAGAIN;
   }
   
   // make sure we're not referenced by anyone 
   if( dlctx->dlset != NULL ) {
      SG_error("Download context %p is still attached to download set %p\n", dlctx, dlctx->dlset );
      pthread_mutex_unlock( &dlctx->finalize_lock );
      return -EINVAL;
   }
   
   SG_debug("Free download context %p\n", dlctx );
   
   md_bound_response_buffer_free( &dlctx->brb );
   
   if( curl != NULL ) {
      *curl = dlctx->curl;
   }
   
   if( dlctx->effective_url != NULL ) {
      
      SG_safe_free( dlctx->effective_url );
   }
   
   if( dlctx->__downloader_url != NULL ) {
      
      SG_safe_free( dlctx->__downloader_url );
   }
   
   dlctx->curl = NULL;
   
   sem_destroy( &dlctx->sem );
   
   pthread_mutex_unlock( &dlctx->finalize_lock );
   pthread_mutex_destroy( &dlctx->finalize_lock );
   
   memset( dlctx, 0, sizeof(struct md_download_context));
   
   return 0;
}


// if a download context is part of a download set, remove it 
// always succeeds
int md_download_context_clear_set( struct md_download_context* dlctx ) {

   if( dlctx != NULL && dlctx->dlset != NULL ) {
      md_download_set_clear( dlctx->dlset, dlctx );
      dlctx->dlset = NULL;
   }
   
   return 0;
}

// wait for a semaphore to be signaled, up to a given number of milliseconds.
// if timeout_ms is less than or equal to 0, then wait indefinitely.
// this method masks -EINTR.
// if timeout_ms < 0, then use sem_wait.  Otherwise, use sem_trywait +timeout_ms seconds into the future.
// return 0 on success
// return -ETIMEDOUT if we timed out waiting 
// return -errno if sem_timedwait(2) or sem_wait(2) failed for some other reason
int md_download_sem_wait( sem_t* sem, int64_t timeout_ms ) {
   
   int rc = 0;
   
   // do we timeout the wait?
   if( timeout_ms > 0 ) {
      
      struct timespec abs_ts;
      clock_gettime( CLOCK_REALTIME, &abs_ts );
      abs_ts.tv_sec += timeout_ms / 1000L;
      abs_ts.tv_nsec += timeout_ms / 1000000L;
      
      if( abs_ts.tv_nsec >= 1000000000L) {
         
         // wrap around
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
            SG_error("sem_timedwait errno = %d\n", rc );
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
            SG_error("sem_wait errno = %d\n", rc );
            break;
         }
         
         // otherwise, try again if interrupted
      }
   }
   
   return rc;
}


// wait for a download to finish, either in error or not 
// return the result of waiting, NOT the result of the download 
// (see md_download_sem_wait for possible return codes)
// return 0 on success
int md_download_context_wait( struct md_download_context* dlctx, int64_t timeout_ms ) {
   
   SG_debug("Wait on download context %p\n", dlctx );
   
   int rc = md_download_sem_wait( &dlctx->sem, timeout_ms );
   
   if( rc != 0 ) {
      SG_error("md_download_sem_wait rc = %d\n", rc );
   }
   return rc;
}


// wait for a download to finish within a download set, either in error or not.
// return the result of waiting, NOT the result of the download 
// (see md_downlaod_sem_wait for possible return codes)
// return 0 on success
// return -EINVAL if the download set does not have any waiting contexts
int md_download_context_wait_any( struct md_download_set* dlset, int64_t timeout_ms ) {

   if( dlset->waiting == NULL ) {
      return -EINVAL;
   }
   
   if( dlset->waiting->size() == 0 ) {
      return 0;
   }
   
   SG_debug("Wait on download set %p (%zu contexts)\n", dlset, dlset->waiting->size() );
   
   int rc = 0;
   
   // wait for at least one of them to finish 
   rc = md_download_sem_wait( &dlset->sem, timeout_ms );
   
   if( rc != 0 ) {
      SG_error("md_download_sem_wait rc = %d\n", rc );
   }
   
   return rc;
}

// set up a download set 
// return 0 on success
// return -ENOMEM on OOM
int md_download_set_init( struct md_download_set* dlset ) {
   
   SG_debug("Initialize download set %p\n", dlset );
   
   dlset->waiting = SG_safe_new( md_pending_set_t() );
   if( dlset->waiting == NULL ) {
      return -ENOMEM;
   }
   
   sem_init( &dlset->sem, 0, 0 );
   
   return 0;
}


// free a download set 
// return 0 on success
int md_download_set_free( struct md_download_set* dlset ) {
   
   SG_debug("Free download set %p\n", dlset );
   
   if( dlset->waiting ) {
      
      SG_safe_delete( dlset->waiting );
   }
   
   sem_destroy( &dlset->sem );
   
   memset( dlset, 0, sizeof( struct md_download_set ) );
   
   return 0;
}


// add a download context to a download set.
// do this before starting the download.
// return 0 on success
// return -ENOMEM on OOM
int md_download_set_add( struct md_download_set* dlset, struct md_download_context* dlctx ) {
   
   int rc = 0;
   
   try {
      md_pending_set_t::iterator itr = dlset->waiting->find( dlctx );
      if( itr == dlset->waiting->end() ) {
         
         dlset->waiting->insert( dlctx );
         
         dlctx->dlset = dlset;
         
         SG_debug("Add download context %p to download set %p\n", dlctx, dlset );
      }
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
   }
   
   return rc;
}


// remove a download context from a download set by iterator
// don't do this in e.g. a for() loop where you're iterating over download contexts
// return 0 on success 
// return -ENOMEM on OOM
int md_download_set_clear_itr( struct md_download_set* dlset, const md_download_set_iterator& itr ) {
   
   try {
      struct md_download_context* dlctx = *itr;
      dlset->waiting->erase( itr );
      dlctx->dlset = NULL;
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}

// remove a download context from a download set by value
// don't do this in e.g. a for() loop where you're iterating over download contexts
// return 0 on success 
// return -ENOMEM on OOM 
int md_download_set_clear( struct md_download_set* dlset, struct md_download_context* dlctx ) {

   try {
      if( dlctx != NULL && dlset->waiting != NULL ) {
         dlset->waiting->erase( dlctx );
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   if( dlctx != NULL ) {
      dlctx->dlset = NULL;
   }
   
   return 0;
}


// how many items in a download set?
// return the number of items
size_t md_download_set_size( struct md_download_set* dlset ) {
   
   return dlset->waiting->size();
}


// iterate: begin 
// return an iterator to the waiting set
md_download_set_iterator md_download_set_begin( struct md_download_set* dlset ) {
   return dlset->waiting->begin();
}

// iterate: end 
// return an interator to the waiting set
md_download_set_iterator md_download_set_end( struct md_download_set* dlset ) {
   return dlset->waiting->end();
}

// iterate: deref 
// return a pointer to the download context
struct md_download_context* md_download_set_iterator_get_context( const md_download_set_iterator& itr ) {
   return *itr;
}


// connect a donwload context to the caches.
// this is a no-op if cache_closure == NULL and the dlctx's cache_func is NULL as well
// return 0 on success
// return non-zero if the cache_func is non-NULL and it failed
static int md_download_context_connect_cache( struct md_download_context* dlctx, struct md_closure* cache_closure, char const* base_url ) {
   
   int rc = 0;
   
   // connect to the cache 
   if( dlctx->cache_func != NULL && cache_closure != NULL ) {
      
      // connect to the cache
      rc = (*dlctx->cache_func)( cache_closure, dlctx->curl, base_url, dlctx->cache_func_cls );
      if( rc != 0 ) {
         
         SG_error("cache connect on %s rc = %d\n", base_url, rc );
         return rc;
      }
   }
   else if( dlctx->cache_func != NULL ) {
      
      SG_warn("CDN closure is NULL, but cache_func = %p\n", dlctx->cache_func );
   }
   
   return rc;
}


// begin downloading something 
// return 0 on success
// return non-zero if we failed to connect to the cache 
// return non-zero if we failed to insert the download into the downloader 
int md_download_context_start( struct md_downloader* dl, struct md_download_context* dlctx, struct md_closure* cache_closure, char const* base_url ) {
   
   int rc = md_download_context_connect_cache( dlctx, cache_closure, base_url );
   if( rc != 0 ) {
      
      SG_error("%s: md_download_context_connect_cache('%s') rc = %d\n", dl->name, base_url, rc );
      return rc;
   }
   
   // enqueue the context into the downloader 
   rc = md_downloader_insert_pending( dl, dlctx );
   if( rc != 0 ) {
      
      SG_error("%s: md_downloader_insert_pending('%s') rc = %d\n", dl->name, base_url, rc );
      return rc;
   }
   
   return 0;
}

// cancel downloading something 
// return 0 on successful insert, or if the download is already cancelled or finalized
// return -EPERM if the downloader was not running 
// return negative if we failed to insert (but mask EINPROGRESS)
// return negative if we failed to wait for the download to get cancelled
int md_download_context_cancel( struct md_downloader* dl, struct md_download_context* dlctx ) {
   
   if( !dl->running ) {
      return -EPERM;
   }
   
   if( dlctx->cancelled || dlctx->finalized ) {
      
      SG_debug("already cancelled %p\n", dlctx );
      return 0;
   }
   
   int rc = md_downloader_insert_cancelling( dl, dlctx );
   
   if( rc != 0 && rc != -EINPROGRESS ) {
      SG_error("md_downloader_insert_cancelling(%p) rc = %d\n", dlctx, rc );
      return rc;
   }
   else {
      
      // EINPROGRESS is okay
      rc = 0;
   }
   
   rc = md_download_context_wait( dlctx, -1 );
   if( rc != 0 ) {
      SG_error("md_download_context_wait(%p) rc = %d\n", dlctx, rc );
   }
   else {
      SG_debug("cancelled %p\n", dlctx );
   }
   
   return rc;
}


// release a waiting context set, given one of its now-finished entries.
// return 0 on success
// return -EINVAL if the download set is NULL;
int md_download_set_wakeup( struct md_download_set* dlset ) {
   
   SG_debug("Wake up download set %p\n", dlset );
   
   int rc = 0;
   
   if( dlset == NULL ) {
      return -EINVAL;
   }
   
   sem_post( &dlset->sem );
   
   return rc;
}

// run multiple downloads for a bit
// dl must be write-locked for downloading 
// return positive non-zero if curl failed somehow 
// return negative if select() failed
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
   rc = curl_multi_perform( dl->curlm, &still_running );
   if( rc != 0 ) {
      
      SG_error("%s: curl_multi_perform rc = %d\n", dl->name, rc );
      return rc;
   }

   // don't wait more than 5ms
   timeout.tv_sec = 0;
   timeout.tv_usec = 5000;      // 5ms

   rc = curl_multi_timeout( dl->curlm, &curl_timeo );
   if( rc != 0 ) {
      
      SG_error("%s: curl_multi_timeout rc = %d\n", dl->name, rc );
      return rc;
   }
   
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
      
      SG_error("%s: curl_multi_fdset rc = %d\n", dl->name, rc );
      return rc;
   }

   // select on them
   rc = select( maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout );
   if( rc < 0 ) {
      
      rc = -errno;
      SG_error("%s: select rc = %d\n", dl->name, rc );
      return rc;
   }
   else {
      rc = 0;
   }
   return rc;
}


// finalize a download context. On success, populate it with the curl rc, the HTTP status, the transfer OS errno, and the effective URL
// http_status == -1 means that we couldn't get the HTTP status 
// os_errno == EIO means that we couldn't get the OS errno or the URL
// return 0 on success, or if the download was already finalized (if the latter, then the fields in dlctx are untouched)
// return -ENOMEM on OOM
int md_downloader_finalize_download_context( struct md_download_context* dlctx, int curl_rc ) {
   
   pthread_mutex_lock( &dlctx->finalize_lock );
   
   // sanity check 
   if( md_download_context_finalized( dlctx ) ) {
      
      pthread_mutex_unlock( &dlctx->finalize_lock );
      return 0;
   }
   
   int rc = 0;
   
   // check HTTP code
   long http_status = 0;
   long os_errno = 0;
   char* url = NULL;
   char* url_dup = NULL;
   
   rc = curl_easy_getinfo( dlctx->curl, CURLINFO_RESPONSE_CODE, &http_status );
   if( rc != 0 ) {
      
      SG_error("curl_easy_getinfo(%p) rc = %d\n", dlctx, rc );
      http_status = -1;
   }
   
   // check error code 
   if( rc != 0 ) {
      rc = curl_easy_getinfo( dlctx->curl, CURLINFO_OS_ERRNO, &os_errno );
         
      if( rc != 0 ) {
         SG_error("curl_easy_getinfo(%p) rc = %d\n", dlctx, rc );
         os_errno = EIO;
      }
   }
   
   // get URL 
   rc = curl_easy_getinfo( dlctx->curl, CURLINFO_EFFECTIVE_URL, &url );
   
   if( rc != 0 || url == NULL ) {
      SG_error("curl_easy_getinfo(%p) rc = %d\n", dlctx, rc );
      os_errno = EIO;
   }
   
   if( url != NULL ) {
      url_dup = SG_strdup_or_null( url );
      if( url_dup == NULL ) {
         
         pthread_mutex_unlock( &dlctx->finalize_lock );
         return -ENOMEM;
      }
   }
   
   dlctx->curl_rc = curl_rc;
   dlctx->http_status = (int)http_status;
   dlctx->transfer_errno = (int)os_errno;
   dlctx->effective_url = NULL;
   
   if( url_dup != NULL ) {
      dlctx->effective_url = url_dup;
      
      SG_debug("Finalized download context %p (%s)\n", dlctx, dlctx->effective_url );
   }
   else {
      SG_debug("Finalized download context %p\n", dlctx );
   }
   
   dlctx->finalized = true;
   dlctx->running = false;
   
   // release waiting thread
   sem_post( &dlctx->sem );
   
   dlctx->safe_to_free = true;
   
   pthread_mutex_unlock( &dlctx->finalize_lock );
   
   return rc;
}


// finalize all finished downloads 
// try to remove all downloads, even if we fail to remove some.
// dl must be write-locked for downloads 
// return 0 on success
// return -ENOMEM on OOM
// return positive if curl did something fatal
int md_downloader_finalize_download_contexts( struct md_downloader* dl ) {
   CURLMsg* msg = NULL;
   int msgs_left = 0;
   int rc = 0;
   
   do {
      msg = curl_multi_info_read( dl->curlm, &msgs_left );

      if( msg == NULL ) {
         // no messages
         break;  
      }

      if( msg->msg == CURLMSG_DONE ) {
         // a transfer finished.  Find out which one
         md_downloading_map_t::iterator itr = dl->downloading->find( msg->easy_handle );
         if( itr != dl->downloading->end() ) {
            
            // found!
            struct md_download_context* dlctx = itr->second;
            
            // get this now, before removing it from the curlm handle
            int result = msg->data.result;
            
            try {
               // remove from the downloader 
               dl->downloading->erase( itr );
            }
            catch( bad_alloc& ba ) {
               rc = -ENOMEM;
               break;
            }
            
            if( dlctx == NULL ) {
               
               SG_warn("no download context for curl handle %p\n", msg->easy_handle);
               
               rc = curl_multi_remove_handle( dl->curlm, msg->easy_handle );
               if( rc != 0 ) {
                  
                  SG_error("curl_multi_remove_handle( %p ) rc = %d\n", msg->easy_handle, rc );
                  rc = 0;
               }
               
               continue;
            }
            
            if( dlctx->curl == NULL ) {
               SG_error("BUG: curl handle of download context %p is NULL\n", dlctx );
               
               rc = curl_multi_remove_handle( dl->curlm, msg->easy_handle );
            }
            else {
               rc = curl_multi_remove_handle( dl->curlm, dlctx->curl );
            }
            
            if( rc != 0 ) {
               SG_error("curl_multi_remove_handle( %p ) rc = %d\n", msg->easy_handle, rc );
               rc = 0;
               continue;
            }
            
            // get the download set from this dlctx, so we can awaken it later
            struct md_download_set* dlset = dlctx->dlset;
            
            // finalize the download context
            rc = md_downloader_finalize_download_context( dlctx, result );
            if( rc != 0 ) {
               
               SG_error("%s: md_downloader_finalize_download_context rc = %d\n", dl->name, rc );
               rc = 0;
               continue;
            }
            
            // wake up the set waiting on this dlctx 
            if( dlset != NULL ) {
               
               rc = md_download_set_wakeup( dlset );
               if( rc != 0 ) {
                  
                  SG_error("md_download_set_wakeup( %p ) rc = %d\n", dlset, rc );
                  rc = 0;
                  continue;
               }
            }
         }
      } 
      
   } while( msg != NULL );
   
   return rc;
}


// main downloader loop
// return NULL
static void* md_downloader_main( void* arg ) {
   
   struct md_downloader* dl = (struct md_downloader*)arg;
   
   SG_debug("%s: starting\n", dl->name );
   
   int rc = 0;
   
   while( dl->running ) {
      md_downloader_downloading_wlock( dl );
      
      // add all pending downloads to this downloader 
      rc = md_downloader_start_all_pending( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_start_all_pending rc = %d\n", dl->name, rc );
      }
      
      // remove all cancelled downloads from this downloader 
      rc = md_downloader_end_all_cancelling( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_end_all_cancelling rc = %d\n", dl->name, rc );
      }
      
      // download for a bit 
      rc = md_downloader_run_multi( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_run_multi rc = %d\n", dl->name, rc );
      }
      
      // finalize any completed downloads 
      md_downloader_finalize_download_contexts( dl );
      if( rc != 0 ) {
         SG_error("%s: md_downloader_finalize_download_contexts rc = %d\n", dl->name, rc );
      }
      
      md_downloader_downloading_unlock( dl );
      
      // give the md_downloader_stop() method a chance to preempt the main method
   }
   
   SG_debug("%s: exiting\n", dl->name );
   return NULL;
}


// consolidate and write back the buffer 
// return 0 on success, and set *buf and *buf_len accordingly
// return -ENOMEM on OOM
int md_download_context_get_buffer( struct md_download_context* dlctx, char** buf, off_t* buf_len ) {
   *buf = md_response_buffer_to_string( dlctx->brb.rb );
   *buf_len = md_response_buffer_size( dlctx->brb.rb );
   
   if( *buf == NULL ) {
      return -ENOMEM;
   }
   
   return 0;
}

// get the http status
// return the positive HTTP status on success
// return -EAGAIN if the download context was not finalized 
int md_download_context_get_http_status( struct md_download_context* dlctx ) {
   if( !dlctx->finalized ) {
      return -EAGAIN;
   }
   return dlctx->http_status;
}

// get the errno 
// return the transfer error code (non-negative number) on sucess
// return -EAGAIN if the download context was not finalized
int md_download_context_get_errno( struct md_download_context* dlctx ) {
   if( !dlctx->finalized ) {
      return -EAGAIN;
   }
   
   return dlctx->transfer_errno;
}

// get the curl rc
// return the non-negative return code on success
// return -EAGAIN if the download was not finalized
int md_download_context_get_curl_rc( struct md_download_context* dlctx ) {
   if( !dlctx->finalized ) {
      return -EAGAIN;
   }
   
   return dlctx->curl_rc;
}

// get the effective URL 
// return 0 on success, and set *url to a malloc'ed copy of the URL (if there was a URL to begin with)
// return -ENOMEM on OOM 
// return -EAGAIN if the download was not finalized
int md_download_context_get_effective_url( struct md_download_context* dlctx, char** url ) {
   if( !dlctx->finalized ) {
      return -EAGAIN;
   }
   
   if( dlctx->effective_url == NULL ) {
      *url = NULL;
   }
   else {
      
      *url = SG_strdup_or_null( dlctx->effective_url );
      if( *url == NULL ) {
         return -ENOMEM;
      }
   }
   
   return 0;
}

// get the download handle's curl handle 
// return the curl handle
CURL* md_download_context_get_curl( struct md_download_context* dlctx ) {
   return dlctx->curl;
}

// get the cache cls (only in reference)
// return a pointer to the cache function closure 
void* md_download_context_get_cache_cls( struct md_download_context* dlctx ) {
   return dlctx->cache_func_cls;
}

// did a download context work?
// return true if the download's curl rc is 0, its transfer errno is 0, and its HTTP status is the desired HTTP status
// return false otherwise
bool md_download_context_succeeded( struct md_download_context* dlctx, int desired_HTTP_status ) {
   return (dlctx->curl_rc == 0 && dlctx->transfer_errno == 0 && dlctx->http_status == desired_HTTP_status); 
}

// is a download finalized?
// return the finalization flag
bool md_download_context_finalized( struct md_download_context* dlctx ) {
   return dlctx->finalized;
}

// is a download running?
// return the running flag
bool md_download_context_running( struct md_download_context* dlctx ) {
   return dlctx->running;
}

// is a download pending?
// return the pending flag
bool md_download_context_pending( struct md_download_context* dlctx ) {
   return dlctx->pending;
}

// is a download cancelled?
// return the cancelled flag
bool md_download_context_cancelled( struct md_download_context* dlctx ) {
   return dlctx->cancelled;
}

// run a single download context 
// dlctx *cannot* be locked
// return 0 on successful finalization, even if the download failed
// return negative otherwise (see md_downloader_finalize_download_context)
int md_download_context_run( struct md_download_context* dlctx ) {
   
   int rc = 0;
   
   dlctx->safe_to_free = false;
   dlctx->running = true;
   
   rc = curl_easy_perform( dlctx->curl );
   if( rc != 0 ) {
      
      SG_error("curl_easy_perform( %p ) rc = %d\n", dlctx, rc );
   }
   
   rc = md_downloader_finalize_download_context( dlctx, rc );
   
   return rc;
}

// initialze a curl handle
// always succeeds
void md_init_curl_handle( struct md_syndicate_conf* conf, CURL* curl_h, char const* url, time_t query_timeout ) {
   md_init_curl_handle2( curl_h, url, query_timeout, conf->verify_peer );
}

// initialze a curl handle
void md_init_curl_handle2( CURL* curl_h, char const* url, time_t query_timeout, bool ssl_verify_peer ) {
   
   curl_easy_setopt( curl_h, CURLOPT_NOPROGRESS, 1L );   // no progress bar
   curl_easy_setopt( curl_h, CURLOPT_USERAGENT, "Syndicate-Gateway/1.0");
   
   if( url != NULL ) {
      curl_easy_setopt( curl_h, CURLOPT_URL, url );
   }
   
   curl_easy_setopt( curl_h, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( curl_h, CURLOPT_MAXREDIRS, 10L );
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
// return 0 on success
// return non-negative curl error code on failure
// return negative on 
int md_download_file2( CURL* curl_h, char** buf, off_t max_len, off_t* ret_size ) {
   
   struct md_bound_response_buffer dlbuf;
   memset( &dlbuf, 0, sizeof(struct md_bound_response_buffer) );
   int rc = 0;
   
   rc = md_bound_response_buffer_init( &dlbuf, max_len );
   if( rc != 0 ) {
      
      return rc;
   }
   
   curl_easy_setopt( curl_h, CURLOPT_WRITEDATA, (void*)&dlbuf );
   curl_easy_setopt( curl_h, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );
   
   rc = curl_easy_perform( curl_h );

   if( rc != 0 ) {
      
      SG_debug("curl_easy_perform rc = %d\n", rc);
      
      md_bound_response_buffer_free( &dlbuf );
      
      return rc;
   }

   *buf = md_response_buffer_to_string( dlbuf.rb );
   *ret_size = md_response_buffer_size( dlbuf.rb );
   
   md_bound_response_buffer_free( &dlbuf );
   
   return 0;
}

// wrapper around md_download_file2
int md_download_file( CURL* curl_h, char** buf, off_t* size ) {
   return md_download_file2( curl_h, buf, -1, size );
}


// start downloading data from zero or more CDNs
// pass NULL for cache_closure, cache_func, and cache_func_cls if you want to avoid CDNs
// return 0 on success
// return -ENOMEM on OOM
// return negative on error
int md_download_begin( struct md_syndicate_conf* conf,
                       struct md_downloader* dl,
                       char const* url, off_t max_len,
                       struct md_closure* cache_closure, md_cache_connector_func cache_func, void* cache_func_cls,
                       struct md_download_context* dlctx ) {
   
   int rc = 0;
   
   // TODO: connection pool 
   CURL* curl = curl_easy_init();
   if( curl == NULL ) {
      return -ENOMEM;
   }
   
   md_init_curl_handle( conf, curl, url, conf->connect_timeout );
   
   rc = md_download_context_init( dlctx, curl, cache_func, cache_func_cls, max_len );
   if( rc != 0 ) {
      
      curl_easy_cleanup( curl );
      return rc;
   }
   
   // start downloading 
   rc = md_download_context_start( dl, dlctx, cache_closure, url );
   if( rc != 0 ) {
      
      SG_error("md_download_context_start(%s) rc = %d\n", url, rc );
      
      // TODO: connection pool 
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      
      return rc;
   }
   
   return 0;
}


// finish downloading data, freeing up the download context
// return 0 on success, and allocate and fill bits, ret_len, and http_status
// return -ENOMEM on OOM 
// return -ENODATA if we didn't get any data 
// return -EPERM if the downloader is not running
// return negative on error 
// dlctx *cannot* be locked
int md_download_end( struct md_downloader* dl, struct md_download_context* dlctx, int timeout, int* http_status, char** bits, size_t* ret_len ) {
   
   int rc = 0;
   char* base_url = NULL;
   
   // wait for download
   rc = md_download_context_wait( dlctx, timeout );
   if( rc != 0 ) {
      
      SG_error("md_download_context_wait( %p ) rc = %d\n", dlctx, rc );
      
      rc = md_download_context_cancel( dl, dlctx );
      
      if( rc != 0 ) {
         
         if( rc == -EPERM ) {
            // downloader isn't running.  Try again 
            return rc;
         }
         
         else if( rc == -EINPROGRESS ) {
            // already in the process of getting cancelled.  Wait for it
            SG_debug("Waiting for %p to get cancelled\n", dlctx );
            md_download_context_wait( dlctx, -1 );
            
            rc = 0;
         }
         else if( rc == -EINVAL ) {
            
            // otherwise, it was already finalized
            rc = 0;
         }
         else {
            // unknown error 
            SG_error("md_download_context_cancel(%p) rc = %d\n", dlctx, rc );
            return rc;
         }
      }
   }
   
   else {
      
      // check for errors 
      rc = md_download_context_get_effective_url( dlctx, &base_url );
      if( rc != 0 ) {
         return rc;
      }
      
      rc = md_download_context_get_curl_rc( dlctx );
      
      if( rc != 0 ) {
         
         // curl failed
         int errsv = md_download_context_get_errno( dlctx );
      
         SG_error("md_download_context_get_errno('%s') CURL rc = %d, errno = %d\n", base_url, rc, errsv );
      
         if( errsv == 0 ) {
            
            // translate curl error to OS error
            if( rc == CURLE_COULDNT_CONNECT ) {
               errsv = -ECONNREFUSED;
            }
            else {
               errsv = -EINVAL;       // CURL was not properly set up
            }
         }
      }
      
      else {
         
         // give back the data 
         off_t len = 0;
         
         rc = md_download_context_get_buffer( dlctx, bits, &len );
         if( rc != 0 || len < 0 ) {
            
            SG_error("md_download_context_get_buffer('%s') rc = %d, len = %jd\n", base_url, rc, len );
            
            if( rc != -ENOMEM ) {
               rc = -ENODATA;
            }
         }
         
         else {
            
            // success! get the status
            *ret_len = (size_t)len;
            *http_status = md_download_context_get_http_status( dlctx );
         }
      }
   }
   
   // clean up
   // TODO: connection pool 
   CURL* old_curl = NULL;
   md_download_context_free( dlctx, &old_curl );
   
   curl_easy_cleanup( old_curl );
   
   SG_safe_free( base_url );
   
   return rc;
}


// download data from one or more CDNs, and then fall back to a direct download if that fails.
// return 0 on success (which means "the server responded with some data")
// return negative on error.
// in all cases, set *http_code to the HTTP result if the download succeeded
// NOTE: the http code can be an error code (i.e. 40x, 50x), which the caller should check
// TODO: refactor to avoid thread context switch to downloader
int md_download( struct md_syndicate_conf* conf,
                 struct md_downloader* dl,
                 char const* base_url, off_t max_len,
                 struct md_closure* closure, md_cache_connector_func cache_func, void* cache_func_cls,
                 int* http_code, char** bits, off_t* ret_len ) {
   
   int rc = 0;
   
   struct md_download_context dlctx;
   memset( &dlctx, 0, sizeof(struct md_download_context) );
   
   // start the download
   rc = md_download_begin( conf, dl, base_url, max_len, closure, cache_func, cache_func_cls, &dlctx );
   if( rc != 0 ) {
      
      SG_error( "md_download_begin(%s) rc = %d\n", base_url, rc );
      return rc;
   }
   
   // finish the download 
   size_t len = 0;
   rc = md_download_end( dl, &dlctx, conf->transfer_timeout * 1000, http_code, bits, &len );
   if( rc != 0 ) {
      SG_error("md_download_end(%s) rc = %d\n", base_url, rc );
      return rc;
   }
   
   *ret_len = len;
   
   return rc;
}


// translate an HTTP status code into the approprate error code.
// return the code if no error could be determined.
int md_HTTP_status_code_to_error_code( int status_code ) {
   if( status_code == SG_HTTP_TRYAGAIN ) {
      return -EAGAIN;
   }
   
   if( status_code == 500 ) {
      return -EREMOTEIO;
   }
   
   if( status_code == 404 ) {
      return -ENOENT;
   }
   
   return status_code;
}


// start downloading a manifest 
// return 0 on success
// return negative on error (see md_download_begin)
int md_download_manifest_begin( struct md_syndicate_conf* conf,
                                struct md_downloader* dl,
                                char const* manifest_url, 
                                struct md_closure* cache_closure, md_cache_connector_func cache_func, void* cache_func_cls,
                                struct md_download_context* dlctx ) {
   
   int rc = md_download_begin( conf, dl, manifest_url, SG_MAX_MANIFEST_LEN, cache_closure, cache_func, cache_func_cls, dlctx );
   
   if( rc != 0) {
      SG_error("md_download_begin(%s) rc = %d\n", manifest_url, rc );
   }
   
   return rc;
}

// finish donwloading a manifest, and parse it.
// return 0 on success, and fill in mmsg with the parsed data
// return negative on error.
// NOTE: no authenticity checks will be performed on the manifest!  The caller *MUST* do this itself!
int md_download_manifest_end( struct md_syndicate_conf* conf,
                              struct md_downloader* dl,
                              Serialization::ManifestMsg* mmsg,
                              struct md_closure* closure, md_manifest_processor_func manifest_func, void* manifest_func_cls,
                              struct md_download_context* dlctx ) {

   int rc = 0;
   int http_status = 0;
   char* manifest_bits = NULL;
   size_t manifest_len = 0;
   
   // finish up 
   rc = md_download_end( dl, dlctx, conf->transfer_timeout * 1000, &http_status, &manifest_bits, &manifest_len );
   if( rc != 0 ) {
      SG_error("md_download_end rc = %d\n", rc );
      
      SG_safe_free( manifest_bits );
      return rc;
   }
   
   if( http_status != 200 ) {
      SG_error("md_download_end HTTP status = %d\n", http_status );
      
      if( manifest_bits != NULL ) {
         SG_safe_free( manifest_bits );
      }
      
      return md_HTTP_status_code_to_error_code( http_status );
   }
   
   if( manifest_bits == NULL ) {
      SG_error("No data received from download context %p\n", dlctx );
      
      return -ENODATA;
   }
   
   // post-processing
   if( manifest_func != NULL ) {
      
      char* processed_manifest_data = NULL;
      size_t processed_manifest_data_len = 0;
      
      rc = (*manifest_func)( closure, manifest_bits, manifest_len, &processed_manifest_data, &processed_manifest_data_len, manifest_func_cls );
      if( rc != 0 ) {
         
         SG_error("manifest_func rc = %d\n", rc );
         
         SG_safe_free( manifest_bits );
         return rc;
      }
      
      // move data over
      if( manifest_bits != NULL ) {
         SG_safe_free( manifest_bits );
      }
      
      manifest_bits = processed_manifest_data;
      manifest_len = processed_manifest_data_len;
   }
   
   // parse it
   rc = md_parse< Serialization::ManifestMsg >( mmsg, manifest_bits, manifest_len );
   if( rc != 0 ) {
      
      SG_error("md_parse rc = %d\n", rc );
      
      SG_safe_free( manifest_bits );
      return rc;
   }
   
   SG_safe_free( manifest_bits );
   return rc;
}


// synchronously download and parse a manifest 
// return 0 on success, and fill in mmsg
// return neative negative on error
// NOTE: does not check the authenticity!
int md_download_manifest( struct md_syndicate_conf* conf,
                          struct md_downloader* dl, 
                          char const* manifest_url,
                          struct md_closure* closure, md_cache_connector_func cache_func, void* cache_func_cls,
                          Serialization::ManifestMsg* mmsg, md_manifest_processor_func manifest_func, void* manifest_func_cls ) {
                          
   int rc = 0;
   struct md_download_context dlctx;
   
   memset( &dlctx, 0, sizeof(struct md_download_context) );
   
   rc = md_download_manifest_begin( conf, dl, manifest_url, closure, cache_func, cache_func_cls, &dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_manifest_begin(%s) rc = %d\n", manifest_url, rc );
      return rc;
   }
   
   rc = md_download_manifest_end( conf, dl, mmsg, closure, manifest_func, manifest_func_cls, &dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_manifest_end(%s) rc = %d\n", manifest_url, rc );
      return rc;
   }
   
   return 0;
}

// default curl generator
// generate and return a curl handle
static CURL* md_download_default_curl_generator( void* ignored ) {
   
   CURL* curl = curl_easy_init();
   
   return curl;
}

// default curl release 
// clean up the curl handle
static void md_download_default_curl_release( CURL* curl, void* ignored ) {
   curl_easy_cleanup( curl );
}

// default post-processor 
// no-op; always succeeds
static int md_download_default_postprocess( struct md_download_context* dlctx, void* ignored ) {
   return 0;
}

// fill in a download config with defaults (no-ops)
// always succeeds
static int md_download_config_defaults( struct md_download_config* dlconf ) {
   
   if( dlconf->max_downloads <= 0 ) {
      dlconf->max_downloads = MD_DOWNLOAD_DEFAULT_MAX_DOWNLOADS;
   }
   
   if( dlconf->max_len <= 0 ) {
      dlconf->max_len = -1;             // unlimited
   }
   
   if( dlconf->curl_generator == NULL ) {
      dlconf->curl_generator = md_download_default_curl_generator;
      dlconf->curl_generator_cls = NULL;
   }
   
   if( dlconf->curl_release == NULL ) {
      dlconf->curl_release = md_download_default_curl_release;
      dlconf->curl_release_cls = NULL;
   }
   
   if( dlconf->postprocess_func == NULL ) {
      dlconf->postprocess_func = md_download_default_postprocess;
      dlconf->postprocess_func_cls = NULL;
   }
   
   return 0;
}

// allocate downloads 
// return 0 on success 
// return -ENOMEM on error
static int md_download_context_alloc_all( struct md_download_context*** ret_downloads, int max_downloads ) {
   
   int rc = 0;
   
   // allocate downloads...
   struct md_download_context** downloads = SG_CALLOC( struct md_download_context*, max_downloads );
   
   if( downloads == NULL ) {
      return -ENOMEM;
   }
   
   for( int i = 0; i < max_downloads; i++ ) {
      
      struct md_download_context* dlctx = SG_CALLOC( struct md_download_context, 1 );
      if( dlctx == NULL ) {
         rc = -ENOMEM;
         break;
      }
      
      downloads[i] = dlctx;
   }
   
   if( rc != 0 ) {
      
      SG_FREE_LIST( downloads, SG_safe_free );
      return rc;
   }
   
   *ret_downloads = downloads;
   
   return 0;
}
   

// cancel downloads, and on successsful cancellation, call the canceller function
// try to cancel all downloads, even if some fail
// none of the downloads can be locked
// return 0 on success 
// return negative on error (see md_download_context_cancel); the last error to occur
static int md_download_context_cancel_all( struct md_downloader* dl, struct md_download_context** downloads, int num_downloads, struct md_download_config* dlconf ) {
   
   int rc = 0;
   
   SG_debug("cancel %d downloads\n", num_downloads );
   
   for( int i = 0; i < num_downloads; i++ ) {
      
      rc = md_download_context_cancel( dl, downloads[i] );
      if( rc != 0 ) {
         
         SG_error("md_download_context_cancel(%p) rc = %d\n", downloads[i], rc );
      }
      
      else if( dlconf->canceller_func != NULL ) {
         // user-specific cancellation logic 
         (*dlconf->canceller_func)( downloads[i], dlconf->canceller_func_cls );
      }
   }
   
   return rc;
}

// free up and deallocate downloads
// if dlset is not NULL, then detach the downloads from it.
// none of the downloads can be locked.
// always succeeds, even if it has to spin
static int md_download_context_free_all( struct md_download_set* dlset, struct md_download_context** downloads, int num_downloads, struct md_download_config* dlconf ) {
   
   int rc = 0;
   
   for( int i = 0; i < num_downloads; i++ ) {
      
      if( downloads[i] == NULL ) {
         continue;
      }
      
      if( dlset != NULL ) {
         md_download_set_clear( dlset, downloads[i] );
      }
      else {
         md_download_context_clear_set( downloads[i] );
      }
      
      CURL* curl = NULL;
      
      rc = -EAGAIN;
      while( rc == -EAGAIN ) {
         
         rc = md_download_context_free( downloads[i], &curl );
         if( rc != 0 ) {
            
            if( rc == -EINVAL ) {
               // somehow got added to a download set???
               md_download_context_clear_set( downloads[i] );
               rc = md_download_context_free( downloads[i], &curl );
               
               if( rc == 0 ) {
                  break;
               }
            }
         
            // let some other work get done
            sched_yield();
         }
      }
      
      (*dlconf->curl_release)( curl, dlconf->curl_release_cls );
      
      SG_safe_free( downloads[i] );
   }
   
   free( downloads );
   
   return 0;
}

// initialize download contexts, using a function to generate curl handles
// return 0 on success
// return -ENOTCONN if we can't generate a curl handle 
// return negative on failure to initialize a download context (see md_download_context_init)
static int md_download_context_init_all( struct md_download_context** downloads, int max_downloads, struct md_download_config* dlconf ) {
   
   int rc = 0;
   
   md_download_curl_generator_func curl_generator = dlconf->curl_generator;
   void* curl_generator_cls = dlconf->curl_generator_cls;
   
   md_cache_connector_func cache_func = dlconf->cache_func;
   void* cache_func_cls = dlconf->cache_func_cls;
   
   off_t max_len = dlconf->max_len;
   
   // initialize downloads
   for( int i = 0; i < max_downloads; i++ ) {
      
      // next CURL handle 
      CURL* next_curl = (*curl_generator)( curl_generator_cls );
      if( next_curl == NULL ) {
         
         rc = -ENOTCONN;
         break;
      }
      
      rc = md_download_context_init( downloads[i], next_curl, cache_func, cache_func_cls, max_len );
      if( rc != 0 ) {
         
         SG_error("md_download_context_init(%p) rc = %d\n", downloads[i], rc );
         
         curl_easy_cleanup( next_curl );
         break;
      }
   }
   
   return rc;
}

// start downloads, tracking them with the given download set.
// return the number started on success.
// return negative on the first failure to start a download (see md_download_context_start)
static int md_download_context_start_all( struct md_downloader* dl, struct md_download_set* dlset, struct md_download_context** downloads, int max_downloads, struct md_download_config* dlconf ) {
   
   int num_running = 0;
   int rc = 0;
   
   md_download_url_generator_func url_generator = dlconf->url_generator;
   void* url_generator_cls = dlconf->url_generator_cls;
   struct md_closure* cache_closure = dlconf->cache_closure;
   
   // start up initial downloads 
   for( int i = 0; i < max_downloads; i++ ) {
      
      char* next_url = NULL;
      
      if( url_generator != NULL ) {
         
         // next URL
         next_url = (*url_generator)( downloads[i], url_generator_cls );
         if( next_url == NULL ) {
            
            // out of URLs 
            break;
         }
      }
      
      // track this download 
      md_download_set_add( dlset, downloads[i] );
      
      if( next_url != NULL ) {
         
         // enable this URL 
         curl_easy_setopt( downloads[i]->curl, CURLOPT_URL, next_url );
      }
      
      // start this download 
      rc = md_download_context_start( dl, downloads[i], cache_closure, next_url );
      
      if( rc != 0 ) {
         
         SG_error("md_download_context_start(%p, %s) rc = %d\n", downloads[i], next_url, rc );
         
         SG_safe_free( next_url );
         
         break;
      }
      
      // this one is running!
      num_running++;
      
      downloads[i]->__downloader_url = next_url;
   }
   
   if( rc == 0 ) {
      
      SG_debug("Started %d downloads\n", num_running);
      return num_running;
   }
   else {
      
      return rc;
   }
}

// set up a download config 
// always succeeds
void md_download_config_init( struct md_download_config* dlconf ) {
   
   memset( dlconf, 0, sizeof(struct md_download_config) );
   md_download_config_defaults( dlconf );
}

// set up a download config's URL generator
// always succeeds
void md_download_config_set_url_generator( struct md_download_config* dlconf, md_download_url_generator_func url_generator, void* url_generator_cls ) {
   
   dlconf->url_generator = url_generator;
   dlconf->url_generator_cls = url_generator_cls;
}

// set up a download config's CURL generator 
// always succeeds
void md_download_config_set_curl_generator( struct md_download_config* dlconf, md_download_curl_generator_func curl_generator, void* curl_generator_cls ) {
   
   dlconf->curl_generator = curl_generator;
   dlconf->curl_generator_cls = curl_generator_cls;
}

// set up a download config's cache connector 
// always succeeds
void md_download_config_set_cache_connector( struct md_download_config* dlconf, struct md_closure* cache_closure, md_cache_connector_func cache_func, void* cache_func_cls ) {
   
   dlconf->cache_closure = cache_closure;
   dlconf->cache_func = cache_func;
   dlconf->cache_func_cls = cache_func_cls;
}

// set up a download config's download post-processor 
// always succeeds
void md_download_config_set_postprocessor( struct md_download_config* dlconf, md_download_postprocess_func postprocessor_func, void* postprocessor_func_cls ) {
   
   dlconf->postprocess_func = postprocessor_func;
   dlconf->postprocess_func_cls = postprocessor_func_cls;
}

// set up a download config's download canceller 
// always succeeds
void md_download_config_set_canceller( struct md_download_config* dlconf, md_download_postprocess_func canceller_func, void* canceller_func_cls ) {
   
   dlconf->canceller_func = canceller_func;
   dlconf->canceller_func_cls = canceller_func_cls;
}

// set flow control information 
// always succeeds
void md_download_config_set_limits( struct md_download_config* dlconf, int max_downloads, off_t max_len ) {
   
   dlconf->max_downloads = max_downloads;
   dlconf->max_len = max_len;
}

// download a set of urls, in no particular order.
// ensure that no more than max_downloads are running at a given time.
// return 0 on success
// return negative on error, such as from allocation, initialization, download set initialization, download start-up, download cancellation, or download free-ing
int md_download_all( struct md_downloader* dl, struct md_syndicate_conf* conf, struct md_download_config* dlconf ) {
   
   int rc = 0;
   struct md_download_set dlset;
   struct md_download_context** downloads = NULL;
   int num_running = 0;
   int num_finalized = 0;
   bool done_early = false;    // did we get what we wanted early?
   bool cancel_at_end = false;
   
   // extract config values
   int max_downloads = dlconf->max_downloads;
   md_download_postprocess_func postprocess = dlconf->postprocess_func;
   void* postprocess_cls = dlconf->postprocess_func_cls;
   
   // allocate downloads...
   rc = md_download_context_alloc_all( &downloads, max_downloads );
   if( rc != 0 ) {
      
      SG_error("md_download_context_alloc_all( %p, %d ) rc = %d\n", dlconf, max_downloads, rc );
      return rc;
   }
   
   // initialize download contexts
   rc = md_download_context_init_all( downloads, max_downloads, dlconf );
   
   // error? clean up 
   if( rc != 0 ) {
      
      SG_error("md_download_context_init_all( %p, %d ) rc = %d\n", dlconf, max_downloads, rc );
      md_download_context_free_all( NULL, downloads, max_downloads, dlconf );
      return rc;
   }
   
   // setup download set 
   rc = md_download_set_init( &dlset );
   if( rc != 0 ) {
      
      SG_error("md_download_set_init( %p ) rc = %d\n", &dlset, rc );
      
      int free_rc = md_download_context_free_all( NULL, downloads, max_downloads, dlconf );
      if( free_rc != 0 ) {
      
         SG_error("md_download_context_free_all( %p ) rc = %d\n", dlconf, free_rc );
      }
      
      return rc;
   }
   
   // start up initial downloads 
   num_running = md_download_context_start_all( dl, &dlset, downloads, max_downloads, dlconf );
   
   // error? clean up 
   if( num_running < 0 ) {
      
      SG_error("md_download_context_start_all( %p, %d ) rc = %d\n", dlconf, max_downloads, num_running );
      
      // cancel them all
      while( true ) {
         int cancel_rc = md_download_context_cancel_all( dl, downloads, max_downloads, dlconf );
         if( cancel_rc != 0 ) {
            
            // do our best to recover
            SG_error("md_download_context_cancel_all( %p ) rc = %d\n", dlconf, cancel_rc );
            sleep(1);
         }
         else {
            break;
         }
      }
      
      rc = md_download_context_free_all( &dlset, downloads, max_downloads, dlconf );
      if( rc != 0 ) {
      
         SG_error("md_download_context_free_all( %p ) rc = %d\n", dlconf, rc );
      }
      
      md_download_set_free( &dlset );
      return num_running;
   }
   
   // run the downloads!
   while( num_running > 0 ) {
      
      // wait for some downloads to finish, but be resillent against deadlock
      rc = md_download_context_wait_any( &dlset, 10000 );
      if( rc != 0 && rc != -ETIMEDOUT ) {
         
         // failed
         SG_error("md_download_context_wait_any(%p) rc = %d\n", &dlset, rc );
         break;
      }
   
      else if( rc == -ETIMEDOUT ) {
         
         SG_debug("still waiting on download set %p\n", &dlset );
         continue;
      }
      
      // re-count up downloads
      num_running = 0;
      
      for( int i = 0; i < max_downloads; i++ ) {
         
         if( downloads[i] == NULL ) {
            continue;
         }
         
         if( md_download_context_running( downloads[i] ) || md_download_context_pending( downloads[i] ) ) {
            // NOTE: don't check finalization status, since a reset download is not finalized
            num_running++;
            
            continue;
         }
         
         else if( md_download_context_finalized( downloads[i] ) ) {
            
            // finalized!
            num_finalized ++;
            
            // guaranteed to not be running...
            
            // post-process
            rc = (*postprocess)( downloads[i], postprocess_cls );
            if( rc < 0 ) {
               
               // abort 
               SG_error("post-processor( %p ) rc = %d\n", downloads[i], rc );
               break;
            }
            
            if( rc == MD_DOWNLOAD_FINISH ) {
               
               // done early 
               done_early = true;
               rc = 0;
            }
            
            // detach 
            md_download_set_clear( &dlset, downloads[i] );
            
            if( !done_early ) {
               
               // reset internal downloader state
               md_download_context_reset( downloads[i], NULL );
               
               // start next download and reattach
               rc = md_download_context_start_all( dl, &dlset, &downloads[i], 1, dlconf );
               
               if( rc >= 0 ) {
                  
                  num_running += rc;
                  rc = 0;
               }
               else {
                  
                  // error 
                  SG_error("md_download_context_start_all(%p) rc = %d\n", downloads[i], rc );
                  break;
               }
            }
            else {
               
               SG_debug("Early cancelling %p at client's request\n", dlconf );
               
               cancel_at_end = true;
               
               num_running = 0;
               rc = 0;
               break;
            }
         }
      }
      
      if( rc != 0 ) {
         // error 
         break;
      }
      
      SG_debug("Still running %d downloads\n", num_running);
   }
   
   if( rc != 0 || cancel_at_end ) {
      
      // cancel everything
      while( true ) {
         int cancel_rc = md_download_context_cancel_all( dl, downloads, max_downloads, dlconf );
         
         if( cancel_rc != 0 ) {
            
            // do our best to recover
            SG_error("md_download_context_cancel_all(%p) rc = %d\n", dlconf, cancel_rc );
            sleep(1);
         }
         else {
            break;
         }
      }
   }
   
   // clean up
   md_download_context_free_all( &dlset, downloads, max_downloads, dlconf );
   md_download_set_free( &dlset );
   
   return rc;
}


// download a single item, using a download config 
// return 0 on success
// return -ENOTCONN if we failed to generate a curl handle, or failed to initialize the download context 
// return -ENODATA if we failed to run the download 
// return negative result from the postprocess callback
int md_download_single( struct md_syndicate_conf* conf, struct md_download_config* dlconf ) {
   
   int rc = 0;
   int curl_rc = 0;
   CURL* curl = NULL;
   long curl_errno = 0;
   long http_status = 0;
   struct md_download_context dlctx;
   
   memset( &dlctx, 0, sizeof(dlctx) );
   
   md_download_config_defaults( dlconf );

   // next CURL handle 
   curl = (*dlconf->curl_generator)( dlconf->curl_generator_cls );
   if( curl == NULL ) {
      return -ENOTCONN;
   }
   
   rc = md_download_context_init( &dlctx, curl, dlconf->cache_func, dlconf->cache_func_cls, dlconf->max_len );
   if( rc != 0 ) {
      SG_error("md_download_context_init(%p) rc = %d\n", &dlctx, rc );
      
      (*dlconf->curl_release)( curl, dlconf->curl_release_cls );
   
      return -ENOTCONN;
   }
   
   // do the download
   rc = md_download_context_run( &dlctx );
   
   curl_rc = md_download_context_get_curl_rc( &dlctx );
   curl_errno = md_download_context_get_errno( &dlctx );
   http_status = md_download_context_get_http_status( &dlctx );
   
   if( rc != 0 ) {
      SG_error("md_download_context_run: curl rc = %d, http status = %ld, errno = %ld, rc = %d\n", curl_rc, http_status, curl_errno, rc );
      
      if( dlconf->canceller_func ) {
         (*dlconf->canceller_func)( &dlctx, dlconf->canceller_func_cls );
      }
      
      rc = -ENODATA;
   }
   else {
      
      // postprocess 
      rc = (*dlconf->postprocess_func)( &dlctx, dlconf->postprocess_func_cls );
   }

   CURL* dead_curl = NULL;
   
   // NOTE: this unlocks dlctx
   md_download_context_free( &dlctx, &dead_curl );
   
   (*dlconf->curl_release)( dead_curl, dlconf->curl_release_cls );
   
   return rc;
}


// initialize a bound response buffer 
// return 0 on success
// return -ENOMEM if allocation failed 
int md_bound_response_buffer_init( struct md_bound_response_buffer* brb, off_t max_size ) {
   
   memset( brb, 0, sizeof(struct md_bound_response_buffer) );
   
   brb->rb = SG_safe_new( md_response_buffer_t() );
   if( brb == NULL ) {
      return -ENOMEM;
   }
   
   brb->max_size = max_size;
   brb->size = 0;
   
   return 0;
}

// free a bound response buffer 
// always succeeds
int md_bound_response_buffer_free( struct md_bound_response_buffer* brb ) {
   
   if( brb->rb != NULL ) {
      md_response_buffer_free( brb->rb );
      SG_safe_delete( brb->rb );
   }
   
   memset( brb, 0, sizeof(struct md_bound_response_buffer) );
   
   return 0;
}
