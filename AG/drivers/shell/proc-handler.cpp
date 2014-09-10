/*
   Copyright 2013 The Trustees of Princeton University

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

#include "proc-handler.h"
#include "driver.h"

struct shell_driver_state* g_state = NULL;

struct shell_driver_state* shell_driver_get_state() {
   return g_state;
}

void shell_driver_set_state( struct shell_driver_state* state ) {
   g_state = state;
}

// lock control for process table entries
static int proc_table_entry_wlock(struct proc_table_entry *pte) {
   dbprintf("wlock PTE %p\n", pte);
   return pthread_rwlock_wrlock(&pte->pte_lock);
}

static int proc_table_entry_rlock(struct proc_table_entry *pte) {
   dbprintf("rlock PTE %p\n", pte);
   return pthread_rwlock_rdlock(&pte->pte_lock);
}

static int proc_table_entry_unlock(struct proc_table_entry *pte) {
   dbprintf("unlock PTE %p\n", pte);
   return pthread_rwlock_unlock(&pte->pte_lock);
}


// lock control for running processes 
static int proc_table_rlock( struct shell_driver_state* state ) {
   dbprintf("rlock process table %p\n", state->running );
   return pthread_rwlock_rdlock( &state->running_lock );
}

static int proc_table_wlock( struct shell_driver_state* state ) {
   dbprintf("wlock process table %p\n", state->running );
   return pthread_rwlock_wrlock( &state->running_lock );
}

static int proc_table_unlock( struct shell_driver_state* state ) {
   dbprintf("unlock process table %p\n", state->running );
   return pthread_rwlock_unlock( &state->running_lock );
}


// lock control for cache entries 
static int cache_table_rlock( struct shell_driver_state* state ) {
   dbprintf("rlock cache table %p\n", state->cache_table );
   return pthread_rwlock_rdlock( &state->cache_lock );
}

static int cache_table_wlock( struct shell_driver_state* state ) {
   dbprintf("wlock cache table %p\n", state->cache_table );
   return pthread_rwlock_wrlock( &state->cache_lock );
}

static int cache_table_unlock( struct shell_driver_state* state ) {
   dbprintf("unlock cache table %p\n", state->cache_table );
   return pthread_rwlock_unlock( &state->cache_lock );
}
   
   
// set up a proc table entry 
static int proc_table_entry_init( struct proc_table_entry* pte, char const* request_path, char const* stdout_path, pid_t pid ) {
   
   memset( pte, 0, sizeof(struct proc_table_entry) );
   
   pte->request_path = strdup(request_path);
   pte->stdout_path = strdup(stdout_path);
   pte->pid = pid;
   
   pthread_rwlock_init( &pte->pte_lock, NULL );
   
   pte->valid = true;
   return 0;
}

// clean up a proc table entry, for reuse
static void proc_table_entry_clean(struct proc_table_entry *pte) {
   
   if( pte != NULL ) {
      
      pte->valid = false;
      pte->pid = -1;
      
      if( pte->stdout_path ) {
         free( pte->stdout_path );
         pte->stdout_path = NULL;
      }
      
      if( pte->request_path ) {
         free( pte->request_path );
         pte->request_path = NULL;
      }
   }
}

// free up a proc table entry
static void proc_table_entry_free(struct proc_table_entry *pte) {
   
   if( pte != NULL ) {
      
      proc_table_entry_clean( pte );
      
      pthread_rwlock_destroy(&pte->pte_lock);
      
      memset(pte, 0, sizeof(struct proc_table_entry) );
   }
}


// is a process running?
static bool proc_is_running( struct proc_table_entry* entry ) {
   
   if( !entry->valid ) {
      return false;
   }
   
   if( entry->pid < 0 ) {
      return false;
   }
   
   int rc = kill( entry->pid, 0 );
   if( rc != 0 ) {
      return false;
   }
   
   return true;
}

// stop a process and clean it up
static int proc_kill( struct proc_table_entry* entry ) {
   
   int rc = 0;
   
   proc_table_entry_wlock(entry);
   
   // still running?
   rc = kill( entry->pid, 0 );
   if( rc != 0 ) {
      rc = -errno;
      
      if( rc != -ESRCH ) {
         errorf("kill %d (stdout=%s) errno = %d\n", entry->pid, entry->stdout_path, rc );
      }
   }
   else {
      // it's running.  try to kill it.
      rc = kill( entry->pid, SIGKILL );
      if( rc != 0 ) {
         rc = -errno;
         errorf("kill SIGKILL %d (stdout=%s) errno = %d\n", entry->pid, entry->stdout_path, rc );
      }
   }
   
   proc_table_entry_clean(entry);
   
   proc_table_entry_unlock(entry);
   
   return rc;
}


// stop monitoring a process.  Kill it if it is still running 
static int proc_remove( proc_table_t* running, proc_table_t::iterator itr ) {
   
   int rc = 0;
   
   struct proc_table_entry *pte = itr->second;
   
   running->erase( itr );
   
   if( proc_is_running( pte ) ) {
      rc = proc_kill( pte );
      
      if( rc != 0 ) {
         errorf("WARN: proc_kill(%p pid=%d) rc = %d\n", pte, pte->pid, rc );
      }
   }
   
   proc_table_entry_free( pte );
   
   free( pte );

   return rc;
}


// find a process table entry by PID 
static proc_table_t::iterator proc_table_find_by_pid( proc_table_t* proc_table, pid_t pid ) {
   
   proc_table_t::iterator itr = proc_table->find( pid );
   return itr;
}


// find a process table entry by request path 
static proc_table_t::iterator proc_table_find_by_request_path( proc_table_t* proc_table, char const* request_path ) {
   
   for( proc_table_t::iterator itr = proc_table->begin(); itr != proc_table->end(); itr++ ) {
      
      if( strcmp(itr->second->request_path, request_path) == 0 ) {
         return itr;
      }
   }
   
   return proc_table->end();
}



// handle SIGCHLD--possibly a child finished, so we have to update our process table
void proc_sigchld_handler(int signum) {
    
   pid_t pid = 0;
   int status = 0;
   
   struct shell_driver_state* state = shell_driver_get_state();
   
   while( true ) {
      
      // get child status
      pid = waitpid( -1, &status, WNOHANG );
      if( pid < 0 ) {
         
         int errsv = -errno;
         if( errsv == -ECHILD ) {
            // no more children 
            return;
         }
         else {
            errorf("waitpid errno = %d\n", errsv );
            return;
         }
      }
      
      if( WIFEXITED( status ) || WIFSIGNALED( status ) ) {
         
         // a child died
         
         char* request_path = NULL;
         struct AG_driver_publish_info pubinfo;
         memset( &pubinfo, 0, sizeof(struct AG_driver_publish_info) );
         
         // child is dead.  clean up 
         proc_table_wlock( state );
         
         proc_table_t::iterator itr = proc_table_find_by_pid( state->running, pid );
         if( itr != state->running->end() ) {
            
            // remember this before deleting it
            request_path = strdup( itr->second->request_path );
            
            proc_remove( state->running, itr );
         }
         
         proc_table_unlock( state );
         
         if( request_path != NULL ) {
            
            // ask the AG to post new information for our stdout--i.e. the size and mtime
            struct stat sb;
            
            dbprintf("Process %d finished generating %s; try to re-publish\n", pid, request_path);
            
            int rc = proc_stat_data( state, request_path, &sb );
            if( rc != 0 ) {
               errorf("proc_stat_data(%s) rc = %d\n", request_path, rc );
            }
            else {
               
               // fill in publish info 
               pubinfo.size = sb.st_size;
               pubinfo.mtime_sec = sb.st_mtime;
               pubinfo.mtime_nsec = 0;
               
               // ask the AG to republish this process's stdout, now that we know the size and mtime
               rc = AG_driver_request_reversion( request_path, &pubinfo );
               
               if( rc != 0 ) {
                  errorf("WARN: AG_driver_request_reversion(%s) rc = %d\n", request_path, rc );
               }
            }
         }
      }
   }
}

// set up driver state 
int shell_driver_state_init( struct shell_driver_state* state ) {
   
   memset( state, 0, sizeof( struct shell_driver_state ) );
   
   // allocate 
   state->running = new proc_table_t();
   state->cache_table = new cache_table_t();
   
   pthread_rwlock_init( &state->running_lock, NULL );
   pthread_rwlock_init( &state->cache_lock, NULL );
   
   return 0;
}


// start the driver
int shell_driver_state_start( struct shell_driver_state* state ) {
   
   return 0;
}

// stop all running processes and clear out the cache
int shell_driver_state_stop( struct shell_driver_state* state ) {
   
   dbprintf("Stopping all running processes for %p\n", state );
   
   proc_table_wlock( state );
   
   for( proc_table_t::iterator itr = state->running->begin(); itr != state->running->end(); itr++ ) {
      
      struct proc_table_entry* pte = itr->second;
      
      int rc = proc_kill( pte );
      if( rc != 0 ) {
         errorf("WARN: proc_kill( %d ) rc = %d\n", pte->pid, rc );
         
         proc_table_entry_clean( pte );
      }
      
      proc_table_entry_free( pte );
   }
   
   state->running->clear();
   proc_table_unlock( state );
   
   // clear cache 
   cache_table_rlock( state );
   
   for( cache_table_t::iterator itr = state->cache_table->begin(); itr != state->cache_table->end(); itr++ ) {
      
      char const* stdout_path = itr->second.c_str();
      
      int rc = unlink( stdout_path );
      if( rc != 0 ) {
         
         rc = -errno;
         errorf("WARN: unlink(%s) errno = %d\n", stdout_path, rc );
      }
   }
   
   state->cache_table->clear();
   
   cache_table_unlock( state );
   
   return 0;
}


// free driver state 
int shell_driver_state_free( struct shell_driver_state* state ) {
   
   proc_table_wlock( state );
   
   if( state->running->size() > 0 ) {
      
      // can't free if there's still processes running.  They'll need to be reaped.
      proc_table_unlock( state );
      return -EINVAL;
   }
   
   if( state->running != NULL ) {
      
      for( proc_table_t::iterator itr = state->running->begin(); itr != state->running->end(); itr++ ) {
         
         proc_table_entry_free( itr->second );
      }
      
      delete state->running;
      state->running = NULL;
   }
   
   if( state->cache_table != NULL ) {
      
      delete state->cache_table;
      state->cache_table = NULL;
   }
   
   proc_table_unlock( state );
   
   pthread_rwlock_destroy( &state->running_lock );
   pthread_rwlock_destroy( &state->cache_lock );
   
   return 0;
}


// create a temporary path, suitable for mkstemp, for storing a running process's stdout
static char* proc_stdout_path( char const* storage_root ) {
   
   char* ret = md_fullpath( storage_root, "shell-driver-XXXXXX", NULL );
   return ret;
}

// duplicate a file descriptor, or exit on failure 
static int dup2_or_exit( int old_fd, int new_fd ) {
   
   int rc = dup2( old_fd, new_fd );
   if( rc != 0 ) {
      
      rc = -errno;
      errorf("dup2 errno = %d\n", rc );
      exit(4);
   }
   
   return rc;
}


// evict data from the cache 
int proc_evict_cache( struct shell_driver_state* state, char const* request_path ) {

   int rc = 0;
   
   cache_table_wlock( state );
   
   cache_table_t::iterator itr = state->cache_table->find( string(request_path) );
   if( itr != state->cache_table->end() ) {
      
      char const* stdout_path = itr->second.c_str();
      
      rc = unlink( stdout_path );
      if( rc != 0 ) {
         
         rc = -errno;
         errorf("unlink(%s) errno = %d\n", stdout_path, rc );
      }
      
      state->cache_table->erase( itr );
   }
   
   cache_table_unlock( state );
   
   return rc;
}


// start a process, and store the information into a proc_table_entry.
// if the process has already been run for this request_path, then return -EINPROGRESS
static int proc_start( struct shell_driver_state* state, char const* request_path, char const* shell_cmd ) {
   
   int rc = 0;
   int child_pipe[2];           // used by the parent to delay the child's execution by having the child read an int from the parent before exec()
   pid_t child_pid;
   
   // make a pipe to the child 
   rc = pipe( child_pipe );
   if( rc != 0 ) {
      
      rc = -errno;
      errorf("pipe rc = %d\n", rc);
      return rc;
   }
   
   // make a temporary file to write stdout/stderr
   char* stdout_path = proc_stdout_path( state->storage_root );
   
   int stdout_fd = mkstemp( stdout_path );
   if( stdout_fd < 0 ) {
      
      rc = -errno;
      errorf("mkstemp(%s) errno = %d\n", stdout_path, rc );
      
      free( stdout_path );
      
      close( child_pipe[0] );
      close( child_pipe[1] );
      
      return rc;
   }
   
   // stop races to start the same job by reserving the cache table entry for this request 
   cache_table_wlock( state );
   
   cache_table_t::iterator cache_itr = state->cache_table->find( string(request_path) );
   if( cache_itr != state->cache_table->end() ) {
      
      // already have data!
      cache_table_unlock( state );
      
      // clean up 
      free( stdout_path );
      close( stdout_fd );
      close( child_pipe[0] );
      close( child_pipe[1] );
      
      return -EINPROGRESS;
   }
   else {
      // reserve this for ourselves 
      (*state->cache_table)[ string(request_path) ] = string(stdout_path);
   }
   
   cache_table_unlock( state );
   
   // make the child 
   child_pid = fork();
   
   if( child_pid == 0 ) {
      
      // child 
      
      free( stdout_path );
      
      // wait until the parent is ready 
      int go = 0;
      ssize_t len = read( child_pipe[0], &go, sizeof(int) );
      
      if( len < 0 ) {
         
         rc = -errno;
         errorf("Child: read errno = %d\n", rc );
         exit(1);
      }
      
      if( len == 0 ) {
         
         errorf("%s", "BUG: parent closed pipe before writing\n");
         exit(2);
      }
      
      // this is our cue to start 
      close( child_pipe[0] );
      close( child_pipe[1] );
      
      // re-route stdout to stdout_fd 
      close( STDOUT_FILENO );
      dup2_or_exit( STDOUT_FILENO, stdout_fd );
      
      // close everything else, except the new stdout and the current stderr
      long max_fd = sysconf( _SC_OPEN_MAX );
      for( int fd = 0; fd < max_fd; fd++ ) {
         
         if( fd == stdout_fd || fd == STDERR_FILENO ) {
            continue;
         }
         
         close( fd );
      }
      
      // run the command 
      rc = system( shell_cmd );
      if( rc == -1 || rc == 127 ) {
         errorf("Running '%s' exited with return code %d\n", shell_cmd, rc );
      }
      
      // done!  parent will get SIGCHLD
      exit(rc);
   }
   else if( child_pid > 0 ) {
      
      // parent 
      close( stdout_fd );
      
      struct proc_table_entry* pte = CALLOC_LIST( struct proc_table_entry, 1 );
      
      // set up the process table entry 
      proc_table_entry_init( pte, request_path, stdout_path, child_pid );
      
      proc_table_wlock( state );
      
      // tell the child to go!
      int go = 0;
      ssize_t len = write( child_pipe[1], &go, sizeof(int) );
      if( len < 0 ) {
         
         // somehow, we failed to wake the child up 
         rc = -errno;
         errorf("ERR: write(%d) for child %d errno = %d\n", child_pipe[1], child_pid, rc );
         
         // clean up 
         proc_kill( pte );
         
         proc_table_entry_free( pte );
         free( pte );
         
         proc_table_unlock( state );
         
         // unreserve this
         proc_evict_cache( state, request_path );
         
         return -EIO;
      }
      
      // we're good!  track this process
      (*state->running)[ child_pid ] = pte;
      
      proc_table_unlock( state );
      
      // update our cache table 
      cache_table_wlock( state );
      
      (*state->cache_table)[ string(request_path) ] = string(stdout_path);
      
      cache_table_unlock( state );
   }
   else {
      
      // failed to fork
      int fork_errno = -errno;
      errorf("ERR: fork() errno = %d\n", fork_errno );
      
      rc = unlink( stdout_path );
      if( rc != 0 ) {
      
         rc = -errno;
         errorf("WARN: unlink(%s) errno = %d\n", stdout_path, rc );
      }
      
      free( stdout_path );
      
      return fork_errno;
   }
   
   free( stdout_path );
   return 0;
}


// ensure that we have data for a request path.
// That is, either we have already run the job for the given request path AND have cached data for it,
// or we're in the process of generating said data.
int proc_ensure_has_data( struct shell_driver_state* state, struct proc_connection_context* ctx ) {
   
   // do we have cached data for this request path?
   cache_table_rlock( state );
   
   cache_table_t::iterator cache_itr = state->cache_table->find( string(ctx->request_path) );
   if( cache_itr != state->cache_table->end() ) {
      
      // we have, or are already in the process of getting data 
      cache_table_unlock( state );
      return 0;
   }
   
   cache_table_unlock( state );
   
   // start the job
   int rc = proc_start( state, ctx->request_path, ctx->shell_cmd );
   if( rc != 0 && rc != -EINPROGRESS ) {
      
      errorf("proc_start( request_path=%s proc='%s' ) rc = %d\n", ctx->request_path, ctx->shell_cmd, rc );
      return rc;
   }
   
   return 0;
}


// see if a process is still running for a particular request path 
bool proc_is_generating_data( struct shell_driver_state* state, char const* request_path ) {
   
   proc_table_rlock( state );
   
   proc_table_t::iterator itr = proc_table_find_by_request_path( state->running, request_path );
   if( itr != state->running->end() ) {
      
      proc_table_unlock( state );
      return true;
   }
   
   proc_table_unlock( state );
   return false;
}


// see if a process has completed generating its data 
bool proc_finished_generating_data( struct shell_driver_state* state, char const* request_path ) {
   
   cache_table_rlock( state );
   
   cache_table_t::iterator cache_itr = state->cache_table->find( string(request_path) );
   if( cache_itr != state->cache_table->end() ) {
      
      // no cached data? hasn't even started.
      cache_table_unlock( state );
      return false;
   }
   
   cache_table_unlock( state );
   
   return !proc_is_generating_data( state, request_path );
}


// stat the output of a running process
int proc_stat_data( struct shell_driver_state* state, char const* request_path, struct stat* sb ) {
   
   
   cache_table_rlock( state );
   
   cache_table_t::iterator cache_itr = state->cache_table->find( string(request_path) );
   if( cache_itr != state->cache_table->end() ) {
      
      // no cached data? hasn't even started.
      cache_table_unlock( state );
      return -ENOENT;
   }
   
   char* stdout_path = strdup( cache_itr->second.c_str() );
   
   cache_table_unlock( state );
   
   // stat this 
   int rc = stat( stdout_path, sb );
   
   if( rc != 0 ) {
      
      rc = -errno;
      errorf("stat(%s) errno = %d\n", stdout_path, rc );
   }
   
   free( stdout_path );
   return rc;
}

// read data into the buffer
// return 0 on success
// if the process is still running, and the requested data hasn't been generated yet, then return -EAGAIN.
// if the process is not running, and the requested data is out of range, then return 1
// if there is no data for this request, then return -ENOENT (to avoid this, call proc_ensure_has_data first).
int proc_read_block_data( struct shell_driver_state* state, char const* request_path, uint64_t block_id, char* buf, ssize_t read_size ) {
   
   int rc = 0;
   struct stat sb;
   
   // look up the location on disk where this should be
   cache_table_rlock( state );
   
   cache_table_t::iterator cache_itr = state->cache_table->find( string(request_path) );
   if( cache_itr == state->cache_table->end() ) {
      
      // no data 
      cache_table_unlock( state );
      
      return -ENOENT;
   }
   
   char* stdout_path = strdup( cache_itr->second.c_str() );
   
   cache_table_unlock( state );
   
   // look up size
   rc = stat( stdout_path, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      errorf("stat(%s) for %s errno = %d\n", stdout_path, request_path, rc );
      
      free( stdout_path );
      return rc;
   }
   
   // do we have enough data?
   uint64_t block_size = AG_driver_get_block_size();
   uint64_t offset = block_id * block_size;
   
   if( offset > (unsigned)sb.st_size || offset + (unsigned)read_size > (unsigned)sb.st_size ) {
      
      free( stdout_path );
      
      // is the process still working on generating data for us?
      if( proc_is_generating_data( state, request_path ) ) {
         return -EAGAIN;
      }
      else {
         // EOF 
         return 1;
      }
   }
   else {
      
      // read data 
      int fd = open( stdout_path, O_RDONLY );
      if( fd < 0 ) {
         
         rc = -errno;
         errorf("open(%s) errno = %d\n", stdout_path, rc );
         
         free( stdout_path );
         return rc;
      }
      
      ssize_t num_read = 0;
      while( num_read < read_size ) {
         
         ssize_t nr = read( fd, buf + num_read, read_size - num_read );
         if( nr < 0 ) {
            
            rc = -errno;
            errorf("read(%s) errno = %d\n", stdout_path, rc );
            break;
         }
         if( nr == 0 ) {
            
            // EOF 
            rc = 1;
            break;
         }
         
         num_read += nr;
      }
      
      free( stdout_path );
      return rc;
   }
}
