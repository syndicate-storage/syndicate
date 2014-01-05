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
   if( logfile )
      return fclose( logfile );
   else
      return 0;
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
