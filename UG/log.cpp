/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "log.h"

/*
 * Open the log file.  Return a handle to the file, or NULL on error.
 */
FILE* log_init(const char* logpath) {
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
   return fclose( logfile );
}

/*
 * Log a message
 */
void logmsg2( FILE* logfile, const char* fmt, ... ) {
   if( get_debug_level() == 0 )
      return;
   
   if( logfile ) {
      va_list args;
   
      va_start(args, fmt);
      vfprintf(logfile, fmt, args);
      va_end(args);
   }
   
   if( get_debug_level() > 0 ) {
      va_list args;
      va_start( args, fmt );
      vprintf(fmt, args);
      fflush(stdout);
      va_end(args);
   }
}

/*
 * Log an error
 */
int logerr2( FILE* logfile, const char* fmt, ... ) {
   
   int ret = -errno;
   
   if( logfile ) {
      va_list args;
      
      va_start(args, fmt);
      vfprintf(logfile, fmt, args);
      va_end(args);
   }
   if( get_debug_level() > 0 ) {
      va_list args;
      
      va_start(args, fmt);
      vfprintf(stderr, fmt, args);
      fflush(stderr);
      va_end(args);
   }
   return ret;
}



void logmsg( FILE* logfile, const char* fmt, ... ) {
   if( get_debug_level() == 0 )
      return;
   
   if( logfile ) {
      va_list args;
   
      va_start(args, fmt);
      vfprintf(logfile, fmt, args);
      va_end(args);
   }
   
   if( get_debug_level() > 0 ) {
      va_list args;
      va_start( args, fmt );
      vprintf(fmt, args);
      fflush(stdout);
      va_end(args);
   }
}



int logerr( FILE* logfile, const char* fmt, ... ) {
   int ret = -errno;
   
   if( logfile ) {
      va_list args;
      
      va_start(args, fmt);
      vfprintf(logfile, fmt, args);
      va_end(args);
   }
   if( get_debug_level() > 0 ) {
      va_list args;
      
      va_start(args, fmt);
      vfprintf(stderr, fmt, args);
      fflush(stderr);
      va_end(args);
   }
   return ret;
}
