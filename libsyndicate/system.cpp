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

// turn into a deamon
int md_daemonize( char* logfile_path, char* pidfile_path, FILE** logfile ) {

   FILE* log = NULL;
   int pid_fd = -1;
   
   if( logfile_path ) {
      log = fopen( logfile_path, "a" );
   }
   if( pidfile_path ) {
      pid_fd = open( pidfile_path, O_CREAT | O_EXCL | O_WRONLY, 0644 );
      if( pid_fd < 0 ) {
         // specified a PID file, and we couldn't make it.  someone else is running
         int errsv = -errno;
         errorf( "Failed to create PID file %s (error %d)\n", pidfile_path, errsv );
         return errsv;
      }
   }
   
   pid_t pid, sid;

   pid = fork();
   if (pid < 0) {
      int rc = -errno;
      errorf( "Failed to fork (errno = %d)\n", -errno);
      return rc;
   }

   if (pid > 0) {
      exit(0);
   }

   // child process 
   // umask(0);

   sid = setsid();
   if( sid < 0 ) {
      int rc = -errno;
      errorf("setsid errno = %d\n", rc );
      return rc;
   }

   if( chdir("/") < 0 ) {
      int rc = -errno;
      errorf("chdir errno = %d\n", rc );
      return rc;
   }

   close( STDIN_FILENO );
   close( STDOUT_FILENO );
   close( STDERR_FILENO );

   if( log ) {
      int log_fileno = fileno( log );

      if( dup2( log_fileno, STDOUT_FILENO ) < 0 ) {
         int errsv = -errno;
         errorf( "dup2 errno = %d\n", errsv);
         return errsv;
      }
      if( dup2( log_fileno, STDERR_FILENO ) < 0 ) {
         int errsv = -errno;
         errorf( "dup2 errno = %d\n", errsv);
         return errsv;
      }

      if( logfile )
         *logfile = log;
      else
         fclose( log );
   }
   else {
      int null_fileno = open("/dev/null", O_WRONLY);
      dup2( null_fileno, STDOUT_FILENO );
      dup2( null_fileno, STDERR_FILENO );
   }

   if( pid_fd >= 0 ) {
      char buf[10];
      sprintf(buf, "%d", getpid() );
      write( pid_fd, buf, strlen(buf) );
      fsync( pid_fd );
      close( pid_fd );
   }
   
   return 0;
}


// assume daemon privileges
int md_release_privileges() {
   struct passwd* pwd;
   int ret = 0;
   
   // switch to "daemon" user, if possible
   pwd = getpwnam( "daemon" );
   if( pwd != NULL ) {
      setuid( pwd->pw_uid );
      dbprintf( "became user '%s'\n", "daemon" );
      ret = 0;
   }
   else {
      dbprintf( "could not become '%s'\n", "daemon" );
      ret = -1;
   }
   
   return ret;
}

