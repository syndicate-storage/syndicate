#include "iFuseLib.Logging.h" 

// get task ID (no glibc wrapper around this...)
pid_t gettid(void) {
   return syscall( __NR_gettid );
}


// get the log file path
char* log_make_path(void) {
   
   size_t path_len = strlen(LOG_PATH_FMT) + 20;
   char* path = (char*)calloc( path_len, 1 );

   snprintf( path, path_len - 1, LOG_PATH_FMT, getpid() );

   return path;
}

// hash a path 
void log_hash_path( char const* path, char hash_buf[LOG_PATH_HASH_LEN] ) {
   unsigned char digest[SHA256_DIGEST_LENGTH];

   // omit the last / in the path 
   size_t path_len = strlen(path);
   if( path[path_len - 1] == '/' )
      path_len --;
   
   SHA256_CTX context;
   SHA256_Init( &context );

   SHA256_Update( &context, LOG_FILENAME_SALT, strlen(LOG_FILENAME_SALT) );
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

// compress a log.
// return the path to the compressed log (caller must free)
char* log_compress( char const* logpath ) {
   
  size_t gzip_path_len = strlen(logpath) + 4;
  char* gzip_path = (char*)calloc( gzip_path_len + 1, 1 );
  snprintf( gzip_path, gzip_path_len, "%s.gz", logpath );

  char const* gzip_cmd_fmt = "/bin/gzip -c %s > %s";

  size_t gzip_cmd_len = strlen(gzip_cmd_fmt) + strlen(logpath) + strlen(gzip_path) + 3;

  char* gzip_cmd = (char*)calloc( gzip_cmd_len + 1, 1 );
  snprintf( gzip_cmd, gzip_cmd_len, gzip_cmd_fmt, logpath, gzip_path );

  FILE* gzip_pipe = popen( gzip_cmd, "r" );
  
  free( gzip_cmd );

  if( gzip_pipe == NULL ) {
     free( gzip_path );
     return NULL;
  }

  pclose( gzip_pipe );

  return gzip_path;
}

/*
 * Open the log file.  Return a handle to the file, or NULL on error (i.e. if no logpath is given, or the file can't be opened).
 * Interpret "stdout" as stdout, and "stderr" as stderr.
 */
FILE* log_init(const char* logpath) {
   if( logpath == NULL )
      return NULL;

   if( strcmp(logpath, "stdout") == 0 )
      return stdout;

   if( strcmp(logpath, "stderr") == 0 )
      return stderr;

   FILE* lf = fopen( logpath, "w" );
   if( lf == NULL )
      return NULL;      // error opening
   
   setvbuf(lf, NULL, _IOLBF, 0);    // line bufferring
   return lf;
}

/*
 * Close the logfile
 */
int log_shutdown( FILE* logfile ) {
   if( logfile && logfile != stdout && logfile != stderr )
      return fclose( logfile );
   else
      return 0;
}
