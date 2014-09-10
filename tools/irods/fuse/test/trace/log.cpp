#include "log.h"


void log_argv( struct log_context* logctx, int argc, char** argv ) {
   
   // log argv 
   for( int i = 1; i < argc; i++ ) {
      
      char path_hash[LOG_PATH_HASH_LEN];
      
      log_hash_path( logctx, argv[i], path_hash );
      
      logmsg( logctx, "Hashed path %s is: %s\n", argv[i], path_hash );
      logerr( logctx, "Hashed path %s is: %s\n", argv[i], path_hash );
   }
}


void print_compressed_log_paths( struct log_context* logctx ) {
   
   printf("Compressed logs:\n");
   
   pthread_rwlock_rdlock( &logctx->lock );
   
   for( unsigned int i = 0; i < logctx->sync_buf->size(); i++ ) {
      printf("%s\n", logctx->sync_buf->at(i) );
   }
   
   pthread_rwlock_unlock( &logctx->lock );
}


int main( int argc, char** argv ) {
   
   struct log_context* logctx = NULL;
   int rc = 0;
   
   if( strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0 ) {
      fprintf(stderr, "Log test program\n");
      trace_usage();
      exit(0);
   }
   
   rc = trace_begin( &logctx );
   if( rc != 0 ) {
      fprintf(stderr, "trace_begin rc = %d\n", rc );
      exit(1);
   }
   
   log_argv( logctx, argc, argv );
   
   // compress 
   rc = log_rollover( logctx );
   if( rc != 0 ) {
      
      fprintf(stderr, "log_rollover rc = %d\n", rc );
      exit(1);
   }
   
   log_argv( logctx, argc, argv );
   
   // compress, again 
   rc = log_rollover( logctx );
   if( rc != 0 ) {
      
      fprintf(stderr, "log_rollover rc = %d\n", rc );
      exit(1);
   }
   
   // where are the compressed logs?
   print_compressed_log_paths( logctx );
   
   // clean up 
   trace_end( &logctx );
   
   return 0;
}