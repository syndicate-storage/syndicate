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

# utility functions for making (gateway) watchdogs

import sys
import os
import signal
import resource
import daemon 
import grp
import lockfile
import errno 
import time
import logging

log = logging.getLogger("watchdog")

#-------------------------------
def spawn( child_method, old_exit_status ):
   # fork, become a watchdog, and run the main server
   child_pid = os.fork()
   
   if child_pid == -1:
      # failure
      raise Exception("Failed to fork")
   
   if child_pid == 0:
      # we're the child...
      rc = child_method( old_exit_status )
      sys.exit(rc)
   
   log.info("Spawned child %s" % child_pid)
   
   return child_pid


#-------------------------------
def handle_child_death( exit_status, respawn_exit_statuses, respawn_signals ):
   need_respawn = False
   
   # did the child exit on its own accord?
   if os.WIFEXITED(exit_status):
      ex_status = os.WEXITSTATUS( exit_status )
      log.info("Child exit status %s" % ex_status )
      
      if respawn_exit_statuses is not None and ex_status in respawn_exit_statuses:
         need_respawn = True
   
   # was the child signaled? 
   elif os.WIFSIGNALED(exit_status):
      sig = os.WTERMSIG( exit_status )
      log.info("Child exited with signal %s" % sig )
      
      if respawn_signals is None or sig in respawn_signals:
         need_respawn = True
      
   return need_respawn


#-------------------------------
def kill_child( child_pid ):
   # send SIGKILL
   log.info("Sending SIGKILL to %s" % child_pid)
   os.kill( child_pid, signal.SIGKILL )


#-------------------------------
def stop_child( child_pid ):
   # send SIGTERM, then SIGKILL
   log.info("Sending SIGTERM to RG %s" % child_pid)
   os.kill( child_pid, signal.SIGTERM )
   
   count = 3
   while count > 0:
      try:
         pid, exit_status = os.waitpid( child_pid, os.WNOHANG )
      except OSError, e:
         log.exception(e)
         
         # if this wasn't ECHILD (i.e. the PID isn't invalid), then kill the process and die
         if e.errno != errno.ECHILD:
            kill_child( child_pid )
            return -1
      
      # did it exit?
      if os.WIFEXITED(exit_status) or os.WIFSIGNALED(exit_status):
         break
      
      # if not, try again
      time.sleep(1.0)
      count -= 1

   if count == 0:
      kill_child( child_pid )
      
   return 0
   

#-------------------------------
def main( spawn_method, pid_cb=None, respawn_exit_statuses=None, respawn_signals=None ):
   
   # fork, become a watchdog, and run the main server
   child_pid = spawn( spawn_method, 0 )
   
   if pid_cb is not None:
      # pass along the PID
      pid_cb( child_pid )
   
   # we're the parent...start monitoring 
   while True:
      
      # wait for child to die
      try:
         pid, exit_status = os.waitpid( child_pid, 0 )
      except OSError, e:
         log.exception(e)
         log.error( "PID %s exiting" % os.getpid() )
         sys.exit(1)
         
      except (KeyboardInterrupt, SystemExit):
         # kill the child and return 
         my_exit_status = stop_child( child_pid )
         log.info("watchdog exit, child exit status %s" % my_exit_status )
         return my_exit_status
         
      need_respawn = handle_child_death( exit_status, respawn_exit_statuses, respawn_signals )
      
      if need_respawn:
         child_pid = spawn( spawn_method, exit_status )
         
         # pass along the PID 
         if pid_cb is not None:
            pid_cb( child_pid )
      
      else:
         # no respawn...return the exit status
         log.info("not respawning, child exit status %s" % exit_status )
         return exit_status
         
         