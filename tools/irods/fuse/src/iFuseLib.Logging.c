#include "iFuseLib.Logging.h" 
#include "iFuseLib.Http.h"

// get task ID (no glibc wrapper around this...)
pid_t gettid(void) {
   return syscall( __NR_gettid );
}


// hash a path 
void log_hash_path( struct log_context* ctx, char const* path, char hash_buf[LOG_PATH_HASH_LEN] ) {
   
   unsigned char digest[SHA256_DIGEST_LENGTH];

   // omit the last / in the path 
   size_t path_len = strlen(path);
   if( path[path_len - 1] == '/' )
      path_len --;
   
   SHA256_CTX context;
   SHA256_Init( &context );

   SHA256_Update( &context, ctx->log_path_salt, strlen(ctx->log_path_salt) );
   SHA256_Update( &context, path, path_len );

   SHA256_Final( digest, &context );

   char buf[3];
   for( int i = 0; i < SHA256_DIGEST_LENGTH; i++ ) {
      sprintf(buf, "%02x", digest[i] );
      hash_buf[2*i] = buf[0];
      hash_buf[2*i + 1] = buf[1];
   }
   hash_buf[ 2 * SHA256_DIGEST_LENGTH ] = 0;
}


// create a log path, suitlabe for log_open 
char* log_make_path( char const* tmpl ) {
   
   char* log_path = (char*)calloc( strlen(tmpl) + 1, 1 );
   if( log_path == NULL ) {
      
      // OOM
      return NULL;
   }
   
   strcpy( log_path, tmpl );
   
   return log_path;
}


// open up a log as a temporary file.
// log_path will be modified by this method.
static FILE* log_open( char* log_path ) {
   
   int tmpfd = mkstemp( log_path );
   if( tmpfd < 0 ) {
      
      int rc = -errno;
      fprintf(stderr, "log_open: mkstemp(%s) errno = %d\n", log_path, rc );
      
      return NULL;
   }
   
   FILE* logfile = fdopen( tmpfd, "r+" );
   if( logfile == NULL ) {
      
      int rc = -errno;
      fprintf(stderr, "log_open: fdopen(%s) errno = %d\n", log_path, rc );
      
      close( tmpfd );
      return NULL;
   }
   
   return logfile;
}


// compress a log, sending the output to the given path.
// return the path to the compressed log (caller must free)
int log_compress( FILE* log_input, char const* output_path ) {
   
  // basically, gzip the file 
  int rc = 0;
  char const* gzip_cmd_fmt = "/bin/gzip > %s";
  
  size_t gzip_cmd_len = strlen(gzip_cmd_fmt) + strlen(output_path);
  char* gzip_cmd = (char*)calloc( gzip_cmd_len + 1, 1 );
  
  if( gzip_cmd == NULL ) {
     return -ENOMEM;
  }
  
  snprintf( gzip_cmd, gzip_cmd_len, gzip_cmd_fmt, output_path );
  
  // begin piping the data into gzip
  FILE* gzip_pipe = popen( gzip_cmd, "w" );
  
  free( gzip_cmd );
  
  if( gzip_pipe == NULL ) {
     
     int errsv = -errno;
     fprintf( stderr, "log_compress: popen(%s) errno = %d\n", gzip_cmd, errsv );
     
     return errsv;
  }
  
  // start at the beginning of the log 
  rc = fseek( log_input, 0, SEEK_SET );
  if( rc != 0 ) {
     
     rc = -errno;
     fprintf( stderr, "log_compress: fseek(%p) errno = %d\n", log_input, rc );
     
     pclose( gzip_pipe );
     return rc;
  }
  
  char buf[65536];
  
  int err = 0;
  
  while( 1 ) {
     
     size_t nr = fread( buf, 1, 65536, log_input );
     size_t nw = 0;
     
     if( nr > 0 ) {
        nw = fwrite( buf, 1, nr, gzip_pipe );
     }
     
     if( nr != 65536 || nw != 65536 ) {
        // EOF?
        if( feof( log_input ) ) {
           break;
        }
        // error on input?
        else if( ferror( log_input ) ) {
           
           int errsv = -errno;
           int ferr = ferror( log_input );
           
           clearerr( log_input );
           
           fprintf( stderr, "log_compress: error reading %p, ferror = %d, errno = %d\n", log_input, errsv, ferr );
           
           err = -1;
           break;
        }
        else if( ferror( gzip_pipe ) ) {
           
           int errsv = -errno;
           int ferr = ferror( gzip_pipe );
           
           clearerr( gzip_pipe );
           
           fprintf( stderr, "log_compress: error writing %p, ferror = %d, errno = %d\n", gzip_pipe, errsv, ferr );
           
           err = -1;
           break;
        }
     }
  }
  
  pclose( gzip_pipe );

  return err;
}


// set up a log context 
struct log_context* log_init( char const* http_server, int http_port, int sync_delay, int timeout, char const* log_path_salt ) {
   
   struct log_context* logctx = (struct log_context*)calloc( sizeof(struct log_context), 1 );
   if( logctx == NULL ) {
      return NULL;
   }
   
   logctx->hostname = strdup( http_server );
   logctx->log_path_salt = strdup( log_path_salt );
   logctx->sync_buf = new log_sync_buf_t();
   
   if( logctx->hostname == NULL || logctx->sync_buf == NULL || logctx->log_path_salt == NULL ) {
      
      // OOM
      log_free( logctx );
      return NULL;
   }
   
   char* log_path = log_make_path( LOG_PATH_FMT );
   if( log_path == NULL ) {
      
      // OOM
      log_free( logctx );
      return NULL;
   }
   
   FILE* logfile = log_open( log_path );
   if( logfile == NULL ) {
      
      fprintf(stderr, "log_open(%s) failed\n", log_path );
      
      free( log_path );
      log_free( logctx );
      return NULL;
   }
   
   logctx->logfile = logfile;
   
   // line bufferring
   setvbuf( logctx->logfile, NULL, _IOLBF, 0 );
   
   logctx->logfile_path = log_path;
   
   logctx->portnum = http_port;
   logctx->sync_delay = sync_delay;
   logctx->running = 0;
   logctx->timeout = timeout;
   
   pthread_rwlock_init( &logctx->lock, NULL );
   sem_init( &logctx->sync_sem, 0, 0 );
   
   return logctx;
}


// free a log context 
int log_free( struct log_context* logctx ) {
   
   if( logctx->running ) {
      return -EINVAL;
   }
   
   if( logctx->logfile_path ) {
      free( logctx->logfile_path );
      logctx->logfile_path = NULL;
   }
   
   if( logctx->hostname ) {
      free( logctx->hostname );
      logctx->hostname = NULL;
   }
   
   if( logctx->log_path_salt ) {
      free( logctx->log_path_salt );
      logctx->log_path_salt = NULL;
   }
   
   if( logctx->sync_buf ) {
      
      for( unsigned int i = 0; i < logctx->sync_buf->size(); i++ ) {
         
         if( logctx->sync_buf->at(i) != NULL ) {
            
            free( logctx->sync_buf->at(i) );
         }
      }
      
      logctx->sync_buf->clear();
      
      delete logctx->sync_buf;
      logctx->sync_buf = NULL;
   }
   
   pthread_rwlock_destroy( &logctx->lock );
   sem_destroy( &logctx->sync_sem );
   
   memset( logctx, 0, sizeof(struct log_context) );
   free( logctx );
   
   return 0;
}


// start up the logging thread
int log_start_threads( struct log_context* logctx ) {
   
   pthread_attr_t attrs;
   int rc;
   
   rc = pthread_attr_init( &attrs );
   if( rc != 0 ) {
      fprintf(stderr, "pthread_attr_init rc = %d\n", rc);
      return rc;
   }
   
   logctx->running = 1;
   
   // start the rollover thread 
   rc = pthread_create( &logctx->rollover_thread, &attrs, log_rollover_thread, logctx );
   if( rc != 0 ) {
      
      fprintf( stderr, "pthread_create(rollover) rc = %d\n", rc );
      logctx->running = 0;
      return rc;
   }
   
   // start the sync thread 
   rc = pthread_create( &logctx->sync_thread, &attrs, http_sync_log_thread, logctx );
   if( rc != 0 ) {
      
      fprintf(stderr, "pthread_create(sync) rc = %d\n", rc );
      logctx->running = 0;
      
      // join the rollover thread 
      pthread_cancel( logctx->rollover_thread );
      pthread_join( logctx->rollover_thread, NULL );
      return rc;
   }
   
   return 0;
}


// stop the logging thread 
int log_stop_threads( struct log_context* logctx ) {
   
   logctx->running = 0;
   
   pthread_cancel( logctx->rollover_thread );
   pthread_join( logctx->rollover_thread, NULL );
   
   pthread_cancel( logctx->sync_thread );
   pthread_join( logctx->sync_thread, NULL );
   
   return 0;
}


// swap logs atomically 
// return the file stream to the old log, replacing the internal file stream and path name with the new log
FILE* log_swap( struct log_context* logctx ) {
   
   if( logctx == NULL ) {
      return NULL;
   }
   
   pthread_rwlock_wrlock( &logctx->lock );
   
   // new path 
   char* new_logpath = log_make_path( LOG_PATH_FMT );
   if( new_logpath == NULL ) {
      
      pthread_rwlock_unlock( &logctx->lock );
      
      fprintf(stderr, "log_swap: log_make_path failed\n");
      return NULL;
   }
   
   // new logfile 
   FILE* new_logfile = log_open( new_logpath );
   if( new_logfile == NULL ) {
      
      pthread_rwlock_unlock( &logctx->lock );
      
      fprintf(stderr, "log_swap: log_open(%s) failed\n", new_logpath );
      
      free( new_logpath );
      return NULL;
   }
   
   // swap the new logfile information in 
   FILE* old_logfile = logctx->logfile;
   char* old_logfile_path = logctx->logfile_path;
   
   logctx->logfile = new_logfile;
   logctx->logfile_path = new_logpath;
   
   pthread_rwlock_unlock( &logctx->lock );
   
   free( old_logfile_path );
   return old_logfile;
}


// swap and compress a logfile, and track it in the log context 
int log_rollover( struct log_context* ctx ) {

   int rc = 0;
   
   // roll over and compress this log 
   // get the current logfile path 
   pthread_rwlock_rdlock( &ctx->lock );
   
   char* curr_logpath = strdup( ctx->logfile_path );
   
   pthread_rwlock_unlock( &ctx->lock );
   
   if( curr_logpath == NULL ) {
      // out of memory 
      return -ENOMEM;
   }
   
   // swap the logs
   FILE* old_logfile = log_swap( ctx );
   if( old_logfile != NULL ) {
      
      // swapped successfully.
      size_t curr_logpath_len = strlen(curr_logpath);
      char* compressed_logfile_path = (char*)calloc( curr_logpath_len + 5, 1 );
      
      if( compressed_logfile_path == NULL ) {
         // OOM 
         return -ENOMEM;
      }
      
      snprintf( compressed_logfile_path, curr_logpath_len + 5, "%s.gz", curr_logpath );
      
      free( curr_logpath );
      
      // compress the log 
      rc = log_compress( old_logfile, compressed_logfile_path );
      if( rc != 0 ) {
         
         fprintf(stderr, "log_compress(%s) rc = %d\n", compressed_logfile_path, rc );
         free( compressed_logfile_path );
         return rc;
      }
      
      // success! give it to the sync thread 
      pthread_rwlock_wrlock( &ctx->lock );
      
      ctx->sync_buf->push_back( compressed_logfile_path );
      
      pthread_rwlock_unlock( &ctx->lock );
      
      return 0;
   }
   else {
      
      // fatal
      fprintf(stderr, "Failed to roll over %s\n", curr_logpath );
      free( curr_logpath );
      
      return -EIO;
   }
}


// log rollover thread 
void* log_rollover_thread( void* arg ) {
   
   struct log_context* ctx = (struct log_context*)arg;
   int rc = 0;
   
   // since we don't hold any resources between compressions, we can cancel immediately
   pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, NULL );
   
   while( ctx->running ) {
      
      struct timespec ts;
      clock_gettime( CLOCK_MONOTONIC, &ts );
      
      // wait a bit 
      ts.tv_sec += ctx->sync_delay;
      
      while( ctx->running ) {
         rc = clock_nanosleep( CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL );
         
         if( rc != 0 ) {
            rc = -errno;
            
            if( !ctx->running ) {
               // got asked to stop?
               break;
            }
            
            if( rc == -EINTR ) {
               // just interrupted.  keep waiting
               continue;
            }
            else {
               fprintf(stderr, "clock_nanosleep errno = %d\n", rc);
               break;
            }
         }
         else {
            // done!
            break;
         }
      }
      
      if( !ctx->running ) {
         // got asked to stop?
         break;
      }
      
      // roll over and compress this log 
      // get the current logfile path 
      pthread_rwlock_rdlock( &ctx->lock );
      
      char* curr_logpath = strdup( ctx->logfile_path );
      
      pthread_rwlock_unlock( &ctx->lock );
      
      if( curr_logpath == NULL ) {
         // OOM 
         break;
      }
      
      // is there any new data in this log?
      struct stat sb;
      rc = stat( curr_logpath, &sb );
      
      if( rc != 0 ) {
         // should *never* happen--this is fatal
         rc = -errno;
         fprintf(stderr, "stat(%s) errno = %d\n", curr_logpath, rc );
         
         break;
      }
      
      free( curr_logpath );
   
      // don't even bother swapping if there's no new data 
      if( sb.st_size == 0 ) {
         continue;
      }
      
      // compress, but don't allow interrupts
      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
      
      rc = log_rollover( ctx );
      
      // re-enable interrupts
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
      
      if( rc != 0 ) {
         fprintf(stderr, "log_rollover_and_compress rc = %d\n", rc );
         break;
      }
      else {
         // wake up the HTTP uploader 
         sem_post( &ctx->sync_sem );
      }
   }
   
   return NULL;
}



