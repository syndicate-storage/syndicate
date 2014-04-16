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
   
   context = daemon.DaemonContext( umask=0o002, prevent_core=True, signal_map=signal_map, stdout=output_fd, stderr=error_fd )
   
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
      

#-------------------------------
def exec_with_piped_stdin( binary, argv, stdin_buf ):
   """
   exec() a process image, but send some data to it initially via stdin.
   WARNING: this is hacky--you shouldn't use pipes in this manner. 
   This method limits len(stdin_buf) to not exceed the host OS's pipe buffer
   length, but a better method should be found.
   """
   
   # WARNING: this is a hack, and is not really the right way to use pipes.
   # open a pipe, replace stdin the read end, and send ourselves the command-list over the pipe.
   
   # make sure our pipe write won't block (otherwise, we're stuck)
   pipe_buflen = os.pathconf('.', os.pathconf_names['PC_PIPE_BUF'])
   if len(stdin_buf) >= pipe_buflen:
      print >> sys.stderr, "stdin buffer too long"
      return -1
   
   try:
      r, w = os.pipe()
   except OSError, oe:
      print >> sys.stderr, "pipe errno = %s" % oe.errno
      return -1
   
   os.dup2( r, sys.stdin.fileno() )
   os.write( w, stdin_buf )
   os.close( w )
   
   # exec the binary 
   sys.stdout.flush()
   sys.stderr.flush()
   
   try:
      os.execvp( binary, argv )
   except OSError, oe:
      log.error("Failed to execute %s, errno = %s" % (binary, -oe.errno))
      return -oe.errno
      
      