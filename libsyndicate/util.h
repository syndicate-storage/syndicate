/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/



#ifndef _UTIL_H_
#define _UTIL_H_

#include <limits.h>
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <memory.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <attr/xattr.h>
#include <semaphore.h>
#include <signal.h>
#include <typeinfo>
#include <openssl/sha.h>
#include <regex.h>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <curl/curl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <math.h>

#include "libsyndicate.h"

#define SCHED_SLEEP 50000
#define CURL_DEFAULT_SELECT_SEC 0
#define CURL_DEFAULT_SELECT_USEC SCHED_SLEEP

using namespace std;

struct thread_args {
   void* context;
   int thread_no;
   struct sigaction act;
};

void block_all_signals();
int install_signal_handler(int signo, struct sigaction *action, sighandler_t handler);
int uninstall_signal_handler(int signo);

// class that defines how work gets distributed to a thread.
// A thread is indexed from 0 to N-1 for N threads.
template <class T>
class ThreadWorkDistributor {
public:
   
   ThreadWorkDistributor() {}
   virtual ~ThreadWorkDistributor() {}
   
   virtual int next_thread( int N, T* work ) = 0;
};


// random work distributor
template <class T>
class ThreadWorkDistributor_Random : public ThreadWorkDistributor<T> {
public:
   ThreadWorkDistributor_Random() {}
   ~ThreadWorkDistributor_Random() {}
   
   int next_thread( int N, T* work ) { return rand() % N; }
};

// NOTE: implementation of this class is in the header file since g++ does not support 'export'
template <class T>
class Threadpool {
public:
   static const int THREADPOOL_WORK_ALL = -1;
   
   // Threadpool constructor
   Threadpool( int num_threads, int max_work_per_thread, bool blocking ) {
      this->num_threads = num_threads;
      this->max_work = max_work_per_thread;
      this->blocking = blocking;
      
      this->threads = new pthread_t[ num_threads ];
      this->queues = new T**[ num_threads ];
      this->queue_counts = new sem_t[ num_threads ];
      this->queue_heads = new int[ num_threads ];
      this->queue_tails = new int[ num_threads ];
      this->queue_locks = new pthread_mutex_t[ num_threads ];
      this->dst = new ThreadWorkDistributor_Random<T>();
      
      for( int i = 0; i < num_threads; i++ ) {
         // next queue
         this->queues[i] = new T*[ max_work ];
         memset( this->queues[i], 0, sizeof(T*) * max_work );
         this->queue_heads[i] = 0;
         this->queue_tails[i] = 0;
         
         sem_init( &this->queue_counts[i], 0, 0 );
         pthread_mutex_init( &this->queue_locks[i], NULL );
      }
      
      this->active = false;
   }
   
   
   // Threadpool destructor--signal all threads to die and free memory
   virtual ~Threadpool() {
      this->signal_stop();
      
      for( int i = 0; i < this->num_threads; i++ ) {
         pthread_mutex_destroy( &this->queue_locks[i] );
         sem_destroy( &this->queue_counts[i] );
         delete[] this->queues[i];
      }
      
      delete[] this->threads;
      delete[] this->queues;
      delete[] this->queue_locks;
      delete[] this->queue_counts;
      delete[] this->queue_heads;
      delete[] this->queue_tails;
      delete this->dst;
   }
   
   
   // start the threads in the threadpool
   // NOTE: not re-entrant and not thread-safe!  Call only once
   int start() {
      if( !this->active ) {
         this->active = true;
         
         for( int i = 0; i < num_threads; i++ ) {
            // next thread
            struct thread_args* args = (struct thread_args*)calloc( sizeof(struct thread_args), 1 );
            args->thread_no = i;
            args->context = this;
            int rc = pthread_create( &this->threads[i], NULL, &Threadpool<T>::thread_main_helper, args );
            if( rc != 0 )
               return rc;
         }
         
         return 0;
      }
      else {
         return -EINVAL;
      }
   }
   
   // tell the threads in the threadpool to die once their work queues run out
   // NOTE: not re-entrant and not thread-safe!  Call only once
   int signal_stop() {
      if( this->active ) {
         this->active = false;
         
         // add a nonce work element to each thread
         for( int i = 0; i < this->num_threads; i++ ) {
            int rc = 0;
            do {
               rc = this->insert_work( &this->quit_nonce, i );
            } while( rc != 0 );
         }
         for( int i = 0; i < this->num_threads; i++ ) {
            pthread_join( this->threads[i], NULL );
         }
      
         return 0;
      }
      else {
         return -EINVAL;
      }
   }
   
   // forcibly stop all threads
   int kill( int sig ) {
      if( this->active ) {
         this->active = false;
         for( int i = 0; i < this->num_threads; i++ ) {
            pthread_kill( this->threads[i], sig );
         }
      }
      return 0;
   }

   // cancel a thread
   int cancel() {
      if( this->active ) {
         this->active = false;
         for( int i = 0; i < this->num_threads; i++ ) {
            pthread_cancel( this->threads[i] );
         }
      }
      return 0;
   }
   
   // add work to the thread.
   // it is assumed that the pointer is to a chunk of memory
   // meant for the threadpool's consumption (i.e. the caller
   // may need to duplicate the chunk for the threadpool).
   
   // single element of work
   // return the thread index that received the work
   // return -1 on failure
   virtual int add_work( T* work ) {
      if( !this->is_active() )
         return -1;
      
      int thread_no = this->next_thread( work );
      int rc = this->insert_work( work, thread_no );
      return rc;
   }
   
   virtual int add_work( T* work, int thread_no ) {
      if( !this->is_active() )
         return -1;
      
      return this->insert_work( work, thread_no );
   }
   
   // process work.
   virtual int process_work( T* work, int thread_no ) {
      return -1;
   }
   
   // how many work items are remaining for a given thread?
   int thread_work_count( int thread_no ) {
      if( !this->is_active() )
         return -1;
      
      int cnt = 0;
      sem_getvalue( &queue_counts[thread_no], &cnt );
      return cnt;
   }
   
   // how many work items are remaining for all threads?
   int work_count() {
      if( !this->is_active() )
         return -1;
      
      int total = 0;
      for( int i = 0; i < this->num_threads; i++ ) {
         int c = this->thread_work_count( i );
         if( c < 0 )
            return -1;
         else
            total += c;
      }
      
      return total;
   }
   
   // can a consumer rely on this threadpool to give it more data?
   virtual bool has_more() {
      return this->work_count() > 0;
   }
   

protected:
   // insert a work element into a specific queue
   int insert_work( T* work, int thread_no ) {
      if( !this->active )
         return -EPERM;
      
      this->work_lock( thread_no );
      if( (this->queue_heads[thread_no] + 1) % this->max_work == this->queue_tails[ thread_no ] ) {
         // queue is full
         this->work_unlock( thread_no );
         return -EAGAIN;
      }
      
      this->queues[ thread_no ][ this->queue_heads[ thread_no ] ] = work;
      this->queue_heads[ thread_no ] = (this->queue_heads[ thread_no ] + 1) % this->max_work;
      
      this->work_unlock( thread_no );
      
      sem_post( &this->queue_counts[ thread_no ] );
      
      return 0;
   }
   
   // iterate over the work in this thread
   int work_begin( int thread_no ) {
      return this->queue_heads[ thread_no ];
   }
   
   int work_next( int thread_no, int i ) {
      return (i + 1) % this->max_work;
   }
   
   int work_end( int thread_no ) {
      return this->queue_tails[ thread_no ];
   }
   
   int work_lock( int thread_no ) {
      return pthread_mutex_lock( &this->queue_locks[thread_no] );
   }
   
   int work_unlock( int thread_no ) {
      return pthread_mutex_unlock( &this->queue_locks[thread_no] );
   }
   
   T* work_at( int thread_no, int i ) {
      return this->queues[ thread_no ][ i ];
   }
   
   // get the next thread to receive work
   int next_thread( T* work ) { return this->dst->next_thread( this->num_threads, work ); }
   
   // get a work element from a specific queue
   T* get_work( int thread_no ) {
      if( this->blocking ) {
         sem_wait( &this->queue_counts[ thread_no ] );
      }
      else {
         int rc = sem_trywait( &this->queue_counts[ thread_no ] );
         if( rc != 0 ) {
            return NULL;   // could not lock --> no work
         }
      }
      
      this->work_lock( thread_no );
      
      T* work = this->queues[ thread_no ][ this->queue_tails[ thread_no ] ];
      this->queues[ thread_no ][ this->queue_tails[ thread_no ] ] = NULL;
      this->queue_tails[ thread_no ] = (this->queue_tails[ thread_no ] + 1 ) % this->max_work;
      
      this->work_unlock( thread_no );
      
      return work;
   }
   
   // Pthread main method for each thread.
   // repeatedly process work
   void* thread_main( int thread_no ) {
      bool run = true;
      while( this->is_active() && run ) {
         T* work = this->get_work( thread_no );
         
         if( work == &this->quit_nonce )
            run = false;
         else
            this->process_work( work, thread_no );
      }
      return NULL;
   }
   
   // helper method to bootstrap the thread
   static void* thread_main_helper( void* arg ) {
      struct thread_args* args = (struct thread_args*)arg;
      
      //Block all the signals
      block_all_signals(); 

      Threadpool<T> *context = (Threadpool<T> *)args->context; 
      
      context->thread_main( args->thread_no );
      
      free( args );
      return NULL;
   }
   
   bool is_active() { return this->active; }
   
   int num_threads;
   bool active;
   
private:
   sem_t* queue_counts;
   T*** queues;
   int* queue_heads;
   int* queue_tails;
   pthread_mutex_t* queue_locks;
   
   pthread_t* threads;
   
   int max_work;
   bool blocking;
   
   T quit_nonce;
   
   ThreadWorkDistributor<T>* dst;
};


// HTTP transfer system
class CURLTransfer {
public:
   
   CURLTransfer( int num_threads );
   ~CURLTransfer();
   
protected:
   CURLM** curlm_handles;
   int* curlm_running;
   
   // do a select() on this thread's curl multi handle
   int process_curl( int thread_no );
   
   // add a handle
   int add_curl_easy_handle( int thread_no, CURL* handle );
   
   // remove a handle
   int remove_curl_easy_handle( int thread_no, CURL* handle );
   
   // next curl message
   CURLMsg* get_next_curl_msg( int thread_no );
   
private:
   int num_handles;
   list<CURL*> added;
};


/*
class TransactionProcessor;

// 2-phase transaction
class Transaction {
public:
   Transaction() { }
   virtual ~Transaction();
   
   // REQUESTOR ONLY
   // prepare phase--get a request number
   virtual int64_t prepare() = 0;
   
   // RESPONDER ONLY
   // promise a request, giving back a REFERENCE to the data for this particular request (can be NULL)
   virtual ssize_t promise( int64_t request, int64_t* chosen_id, char** buf ) = 0;
   
   // REQUESTOR ONLY
   // submit data to be committed
   virtual int accept( int64_t request, char* data, ssize_t len ) = 0;
  
   // RESPONDER ONLY
   // acknowledge a commit, giving back a REFERENCE to the data that was committed
   virtual ssize_t accepted( int64_t request, char** buf ) = 0;

private:
   
   friend class TransactionProcessor;
};


typedef map< int64_t, Transaction* > transaction_set;

// transaction processor--listens for transaction requests
class TransactionProcessor {
public:
   TransactionProcessor() {
      pthread_mutex_init( &this->client_lock, NULL );
      pthread_mutex_init( &this->server_lock, NULL );
      pthread_mutex_init( &this->next_lock, NULL );
      pthread_mutex_init( &this->session_lock, NULL );
      this->next_txn_id = 1;
      this->next_ssn_id = 1;
   }
   
   virtual ~TransactionProcessor() {
      pthread_mutex_destroy( &this->client_lock );
      pthread_mutex_destroy( &this->server_lock );
      pthread_mutex_destroy( &this->next_lock );
      pthread_mutex_destroy( &this->session_lock );
   }
   
   // add a requestor (client) transaction
   int add_request_transaction( int64_t id, Transaction* xn ) {
      return this->add_transaction( id, xn, &this->client, &this->client_lock );
   }
   
   // remove a requestor (client) transaction
   Transaction* remove_request_transaction( int64_t id ) {
      return this->remove_transaction( id, &this->client, &this->client_lock );
   }
   
   // add a response (server) transaction
   int add_response_transaction( int64_t id, Transaction* xn ) {
      return this->add_transaction( id, xn, &this->server, &this->server_lock );
   }
   
   // remove a response (server) transaction
   Transaction* remove_response_transaction( int64_t id ) {
      return this->remove_transaction( id, &this->server, &this->server_lock );
   }
   
   // get the next transaction number
   uint64_t next_transaction_id() {
      pthread_mutex_lock( &this->next_lock );
      uint64_t ret = this->next_txn_id;
      this->next_txn_id++;
      pthread_mutex_unlock( &this->next_lock );
      return ret;
   }
   
   // get the next session number
   uint64_t next_session_id() {
      pthread_mutex_lock( &this->session_lock );
      uint64_t ret = this->next_ssn_id;
      this->next_ssn_id++;
      pthread_mutex_unlock( &this->session_lock );
      return ret;
   }
   
   // get the current session number
   uint64_t get_session_id() {
      pthread_mutex_lock( &this->session_lock );
      uint64_t ret = this->next_ssn_id;
      pthread_mutex_unlock( &this->session_lock );
      return ret;
   }
   
   // advance the transaction number to a new value.
   // return the smaller value
   uint64_t advance_transaction_id( uint64_t new_value ) {
      pthread_mutex_lock( &this->next_lock );
      uint64_t ret = min( new_value, this->next_txn_id );
      this->next_txn_id = max( this->next_txn_id, new_value );
      pthread_mutex_unlock( &this->next_lock );
      return ret;
   }
   
   // advance the session number to a new value.
   // return the smaller value
   uint64_t advance_session_id( uint64_t new_value ) {
      pthread_mutex_lock( &this->session_lock );
      uint64_t ret = min( new_value, this->next_ssn_id );
      this->next_ssn_id = max( new_value, this->next_ssn_id );
      pthread_mutex_unlock( &this->session_lock );
      return ret;
   }
   
private:
   
   // add a transaction
   int add_transaction( int64_t id, Transaction* xn, transaction_set* xn_set, pthread_mutex_t* lock ) {
      int rc = 0;
      
      pthread_mutex_lock( lock );
      
      transaction_set::iterator itr = xn_set->find( id );
      if( itr == xn_set->end() )
         (*xn_set)[ id ] = xn;
      else
         rc = -EEXIST;
      
      pthread_mutex_unlock( lock );
      return rc;
   }
   
   // remove a transaction
   Transaction* remove_transaction( int64_t id, transaction_set* xn_set, pthread_mutex_t* lock ) {
      Transaction* ret = NULL;
      
      pthread_mutex_lock( lock );
      
      transaction_set::iterator itr = xn_set->find( id );
      if( itr != xn_set->end() ) {
         ret = itr->second;
         xn_set->erase( itr );
      }
      
      pthread_mutex_unlock( lock );
      return ret;
   }
   
   
   uint64_t next_txn_id;         // ID of my next transaction
   uint64_t next_ssn_id;         // ID of the next session
   
   transaction_set client;       // outgoing (locally-started) transactions
   transaction_set server;       // incoming (remotely-started) transactions
   
   pthread_mutex_t client_lock;
   pthread_mutex_t server_lock;
   pthread_mutex_t next_lock;
   pthread_mutex_t session_lock;
};
*/

// file functions
char* dir_path( const char* path );
char* fullpath( char* root, const char* path );
mode_t get_umask();

// time functions
int64_t currentTimeSeconds();
int64_t currentTimeMillis();
double currentTimeMono();
int64_t currentTimeMicros();

// misc functions
unsigned char* sha256_hash( char const* input );
unsigned char* sha256_hash_data( char const* input, size_t len );
char* sha256_printable( unsigned char const* sha256 );
char* sha256_hash_printable( char const* input, size_t len );
unsigned char* sha256_data( char const* sha256_print );
unsigned char* sha256_file( char const* path );
unsigned char* sha256_fd( int fd );
unsigned char* sha256_dup( unsigned char const* sha256 );
int sha256_cmp( unsigned char const* sha256_1, unsigned char const* sha256_2 );
int mkdir_sane( char* dirpath );
int rmdir_sane( char* dirpath );
int dir_exists( char* dirpath );
char* dirname( char* path, char* dest );
int make_lockfiles( char* path, char* lnk );
char* load_file( char const* path, size_t* size );
char* url_encode( char const* str, size_t len );
char* url_decode( char const* str, size_t* len );
int reg_match(const char *string, char const *pattern);
int timespec_cmp( struct timespec* t1, struct timespec* t2 );
uint32_t CMWC4096(void);

int Base64Decode(char* b64message, size_t len, char** buffer, size_t* buffer_len);
int Base64Encode(const char* message, size_t len, char** buffer);

int util_init(void);
#endif
