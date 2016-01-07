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

#include "workqueue.h"
#include "util.h"

// work queue main method 
static void* md_wq_main( void* cls ) {
   
   struct md_wq* wq = (struct md_wq*)cls;
   md_wq_queue_t* work = NULL;
   struct md_wreq wreq;
   int rc = 0;
   
   SG_debug("workqueue %p start\n", wq );
   
   while( wq->running ) {
      
      // wait for work 
      sem_wait( &wq->work_sem );
      
      // cancelled?
      if( !wq->running ) {
         break;
      }
      
      // exchange buffers--we have work 
      pthread_mutex_lock( &wq->work_lock );
      
      work = wq->work;
      
      if( wq->work == wq->work_1 ) {
         wq->work = wq->work_2;
      }
      else {
         wq->work = wq->work_1;
      }
      
      pthread_mutex_unlock( &wq->work_lock );
      
      // safe to use work buffer (we'll clear it out)
      while( work->size() > 0 ) {
         
         // next work
         wreq = work->front();
         work->pop();
         
         // carry out work 
         rc = (*wreq.work)( &wreq, wreq.work_data );
         
         SG_debug("Processed work %p (arg %p), rc = %d\n", wreq.work, wreq.work_data, rc );
         
         // is this a promise?  if so, tell the caller that we've fulfilled it 
         if( wreq.flags & MD_WQ_PROMISE ) {
            wreq.promise_ret = rc;
            sem_post( &wreq.promise_sem );
         }
         else {
            
            // done with this 
            md_wreq_free( &wreq );
         }
      }
   }
   
   SG_debug("workqueue %p stop\n", wq );
   
   return NULL;
}


// alloc work queues
struct md_wq* md_wq_new( int count ) {
   return SG_CALLOC( struct md_wq, count );
}

// set up a work queue, but don't start it.
// return 0 on success
// return negative on failure:
// * -ENOMEM if OOM
int md_wq_init( struct md_wq* wq, void* cls ) {
   
   int rc = 0;
   
   memset( wq, 0, sizeof(struct md_wq) );
   
   wq->work_1 = new (nothrow) md_wq_queue_t();
   wq->work_2 = new (nothrow) md_wq_queue_t();
   
   if( wq->work_1 == NULL ) {
      return -ENOMEM;
   }
   
   if( wq->work_2 == NULL ) {
      
      if( wq->work_1 != NULL ) {
         delete wq->work_1;
      }
      
      return -ENOMEM;
   }
   
   wq->work = wq->work_1;
   
   pthread_mutex_init( &wq->work_lock, NULL );
   sem_init( &wq->work_sem, 0, 0 );
   
   wq->cls = cls;
   
   return rc;
}


// start a work queue 
// return 0 on success
// return negative on error:
// * -EINVAL if already started
// * whatever pthread_create errors on
int md_wq_start( struct md_wq* wq ) {
   
   if( wq->running ) {
      return -EINVAL;
   }
   
   int rc = 0;
   pthread_attr_t attrs;
   
   memset( &attrs, 0, sizeof(pthread_attr_t) );
   
   wq->running = true;
   
   rc = pthread_create( &wq->thread, &attrs, md_wq_main, wq );
   if( rc != 0 ) {
      
      wq->running = false;
      
      rc = -errno;
      SG_error("pthread_create errno = %d\n", rc );
      
      return rc;
   }
   
   return 0;
}

// stop a work queue 
// return 0 on success
// return negative on error:
// * -EINVAL if not running
int md_wq_stop( struct md_wq* wq ) {
   
   if( !wq->running ) {
      return -EINVAL;
   }
   
   wq->running = false;
   
   // wake up the work queue so it cancels 
   sem_post( &wq->work_sem );
   pthread_cancel( wq->thread );
   
   pthread_join( wq->thread, NULL );
   
   return 0;
}


// free a work request queue 
static int md_wq_queue_free( md_wq_queue_t* wqueue ) {
   
   while( wqueue->size() > 0 ) {
      
      struct md_wreq wreq = wqueue->front();
      wqueue->pop();
      
      md_wreq_free( &wreq );
   }
   
   return 0;
}

// free up a work queue.  put its caller-specified state in ret_cls
// return 0 on success
// return negative on error:
// * -EINVAL if running 
int md_wq_free( struct md_wq* wq, void** ret_cls ) {
   
   if( wq->running ) {
      return -EINVAL;
   }
   
   // free all 
   if( wq->work_1 != NULL ) {
      md_wq_queue_free( wq->work_1 );
      delete wq->work_1;
      wq->work_1 = NULL;
   }
   
   if( wq->work_2 != NULL ) {
      md_wq_queue_free( wq->work_2 );
      delete wq->work_2;
      wq->work_2 = NULL;
   }
   
   pthread_mutex_destroy( &wq->work_lock );
   sem_destroy( &wq->work_sem );
   
   if( ret_cls != NULL ) {
      *ret_cls = wq->cls;
   }
   
   memset( wq, 0, sizeof(struct md_wq) );
   
   return 0;
}

// create a work request 
int md_wreq_init( struct md_wreq* wreq, md_wq_func_t work, void* work_data, int flags ) {

   memset( wreq, 0, sizeof(struct md_wreq) );
   
   wreq->work = work;
   wreq->work_data = work_data;
   wreq->flags = flags;
   
   if( flags & MD_WQ_PROMISE ) {
      // set up promise fields 
      sem_init( &wreq->promise_sem, 0, 0 );
      wreq->promise_ret = 0;
   }
   
   return 0;
}

// free a work request
int md_wreq_free( struct md_wreq* wreq ) {
   
   if( wreq->flags & MD_WQ_PROMISE ) {
      
      sem_destroy( &wreq->promise_sem );
   }
   
   memset( wreq, 0, sizeof(struct md_wreq) );
   return 0;
}
   
// wait for a work request (promise) to complete
// return 0 on success
// return negative on error;
// * -EINVAL if the work request was not initialized with MD_WQ_PROMISE
int md_wreq_promise_wait( struct md_wreq* wreq ) {
   
   if( (wreq->flags & MD_WQ_PROMISE) == 0 ) {
      return -EINVAL;
   }
   
   sem_wait( &wreq->promise_sem );
   
   return 0;
}

// get the result of a work request (promise)
// return -EINVAL if this isn't a promise
int md_wreq_promise_ret( struct md_wreq* wreq ) {
   
   if( (wreq->flags & MD_WQ_PROMISE) == 0 ) {
      return -EINVAL;
   }
   
   return wreq->promise_ret;
}

// enqueue work
// the data within wreq must remain accessible until the work request is handled,
// but a copy of the wreq struct will be made. 
// return 0 on success
// return -EINVAL if the work queue thread isn't running
int md_wq_add( struct md_wq* wq, struct md_wreq* wreq ) {
   
   int rc = 0;
   
   pthread_mutex_lock( &wq->work_lock );
   
   try {
      wq->work->push( *wreq );
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
   }
   
   pthread_mutex_unlock( &wq->work_lock );
   
   if( rc == 0 ) {
      // have work 
      sem_post( &wq->work_sem );
   }
   
   return rc;
}

// wake up the work queue 
int md_wq_wakeup( struct md_wq* wq ) {
   return sem_post( &wq->work_sem );
}

// get caller-specified data from the wq 
void* md_wq_cls( struct md_wq* wq ) {
   return wq->cls;
}
