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

#include "libsyndicate/system.h"

// turn into a daemonn. if non-null, log to the file referred to by logfile_path.  If non-null, put the child PID into the file referred to by pidfile_path
// return 0 on success (to both the child and parent), and set *logfile to the logfile stream opened if logfile_path is not NULL
// return 0 to the parent if the fork() succeeded, -errno if not (see fork(2))
// return 0 to the child if the child started successfully.
// return -errno if the child failed to create the logfile or PID file (see open(2), fopen(3))
int md_daemonize( char* logfile_path, char* pidfile_path, FILE** logfile ) {

   FILE* log = NULL;
   int pid_fd = -1;
   int rc = 0;
   pid_t pid, sid;

   pid = fork();
   if (pid < 0) {
      
      rc = -errno;
      SG_error( "fork() rc = %d\n", rc);
      return rc;
   }

   if (pid > 0) {
      // parent 
      return 0;
   }

   // child process 
   // umask(0);
   
   // create logfile 
   if( logfile_path ) {
      
      log = fopen( logfile_path, "a" );
      if( log == NULL ) {
         
         rc = -errno;
         return rc;
      }
   }
   
   // create PID file
   if( pidfile_path ) {
      
      pid_fd = open( pidfile_path, O_CREAT | O_EXCL | O_WRONLY, 0644 );
      if( pid_fd < 0 ) {
         
         // specified a PID file, and we couldn't make it.  someone else is running
         rc = -errno;
         SG_error( "open('%s') rc = %d\n", pidfile_path, rc );
         return rc;
      }
   }

   sid = setsid();
   if( sid < 0 ) {
      
      rc = -errno;
      SG_error("setsid() rc = %d\n", rc );
      return rc;
   }
   
   if( chdir("/") < 0 ) {
      
      rc = -errno;
      SG_error("chdir() rc = %d\n", rc );
      return rc;
   }

   close( STDIN_FILENO );
   close( STDOUT_FILENO );
   close( STDERR_FILENO );

   if( log ) {
      
      // send stdout and stderr to the log
      int log_fileno = fileno( log );

      if( dup2( log_fileno, STDOUT_FILENO ) < 0 ) {
         
         rc = -errno;
         SG_error( "dup2 errno = %d\n", rc);
         return rc;
      }
      if( dup2( log_fileno, STDERR_FILENO ) < 0 ) {
         
         rc = -errno;
         SG_error( "dup2 errno = %d\n", rc);
         return rc;
      }

      if( logfile ) {
         
         *logfile = log;
      }
      
      else {
         fclose( log );
      }
   }
   else {
      
      // send stdout and stderr to /dev/null
      int null_fileno = open("/dev/null", O_WRONLY);
      dup2( null_fileno, STDOUT_FILENO );
      dup2( null_fileno, STDERR_FILENO );
   }

   if( pid_fd >= 0 ) {
      
      // write the PID
      char buf[10];
      sprintf(buf, "%d", getpid() );
      write( pid_fd, buf, strlen(buf) );
      fsync( pid_fd );
      close( pid_fd );
   }
   
   return 0;
}


// assume the privileges of a lesser user
// return 0 on success
// return negative on failure (see getpwnam(3))
// NOTE: this is not thread-safe
int md_release_privileges( char const* username ) {
   
   struct passwd* pwd;
   int ret = 0;
   
   // switch to the user, if possible
   pwd = getpwnam( username );
   if( pwd != NULL ) {
      
      setuid( pwd->pw_uid );
      SG_debug( "became user '%s'\n", username );
      ret = 0;
   }
   else {
      SG_debug( "getpwnam('%s') rc = %d\n", username, ret );
      ret = -abs(ret);
   }
   
   return ret;
}

