#!/usr/bin/env python 

"""
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
"""

import sys
import os
import signal
import daemon
import grp
import lockfile


import logging
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.INFO )

#-------------------------------
def daemonize( main_method, logfile_path=None, pidfile_path=None ):
   
   signal_map = {
      signal.SIGTERM: 'terminate',
      signal.SIGHUP: None
   }
   
   daemon_gid = grp.getgrnam('daemon').gr_gid
   
   output_fd = None
   error_fd = None
   
   if logfile_path is not None:
      # create log file
      output_fd = open( logfile_path, "w+" )
      error_fd = output_fd 
      
      os.dup2( output_fd.fileno(), sys.stdout.fileno() )
      os.dup2( output_fd.fileno(), sys.stderr.fileno() )
      
   else:
      # write to stdout and stderr
      output_fd = sys.stdout 
      error_fd = sys.stderr
   
   context = daemon.DaemonContext( umask=0o002, prevent_core=True, signal_map=signal_map, stdout=output_fd, stderr=error_fd, working_directory="/", detach_process=True )
   
   # don't close these if they're open...
   files_preserve = []
   if output_fd:
      files_preserve.append(output_fd)
   if error_fd and error_fd != output_fd:
      files_preserve.append(error_fd)
   
   context.files_preserve = files_preserve
   
   # pid file?
   if pidfile_path is not None:
      context.pidfile = lockfile.FileLock(pidfile_path)
   
   # start up
   with context:
      main_method()
      
