#/usr/bin/python

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

import os
import re
import sys
import logging
import argparse
import logging
import threading
import time

import syndicate.ag.curation.specfile as AG_curation

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )

MAX_RETRIES = 3
DRIVER_NAME = "curl"

# you might have to pip install this one...
import ftputil

# do this in its own thread 
class ftp_crawl_thread( threading.Thread ):
   
   def __init__(self, threadno, ftphost ):
      super( ftp_crawl_thread, self ).__init__()
      self.ftphost = ftphost 
      self.threadno = threadno
      self.producer_sem = threading.Semaphore(0)
      
      self.running = True
      self.result_files = None
      self.result_dirs = None
      self.working = False
      self.cur_dir = None
      self.crawl_status = True
      
   @classmethod
   def crawl( cls, threadno, ftphost, cur_dir ):
      
      log.info( "thread %s: listdir %s" % (threadno, cur_dir ) )
      
      names = None
      for i in xrange(0, MAX_RETRIES):
         try:
            names = ftphost.listdir( cur_dir )
            break
         except Exception, e:
            log.exception(e)
            log.info("thread %s: Trying to crawl %s again" % (threadno, cur_dir) )
            time.sleep(1)
            pass


      if names is None:
         return (None, None, False)
      
      # harvest the work 
      files = []
      dirs = []
      
      for name in names:
         
         abs_path = os.path.join( cur_dir, name )
         
         is_directory = False
         
         for i in xrange(0, MAX_RETRIES):
            try:
               is_directory = ftphost.path.isdir( abs_path )
               break
            except Exception, e:
               log.exception(e)
               log.info("thread %s: Trying to isdir %s again" % (threadno, abs_path))
               time.sleep(1)
               pass
         
         if is_directory:
            dirs.append( name )

         else:
            files.append( name )
            
      # done!
      return (files, dirs, True)
   
   def run(self):
      
      while self.running:
         
         # wait for work
         self.producer_sem.acquire()
         
         if not self.running:
            return
      
         work = self.cur_dir 
         
         self.result_files, self.result_dirs, self.crawl_status = ftp_crawl_thread.crawl( self.threadno, self.ftphost, work )
         
         self.working = False
         
         log.info("thread %s: expored %s" % (self.threadno, self.cur_dir ))
   
   
   def stop_working(self):
      self.running = False 
      self.producer_sem.release()
      
   def is_working(self):
      return self.working
      
   def consume_files(self):
      ret = self.result_files
      self.result_files = None
      return ret
   
   def consume_dirs( self ):
      ret = self.result_dirs
      self.result_dirs = None
      return ret

   def consume_crawl_status( self ):
      ret = self.crawl_status
      self.crawl_status = True 
      return ret 
   
   def get_cur_dir( self ):
      return self.cur_dir 
   
   def next_dir( self, cur_dir ):
      if self.is_working():
         raise Exception("thread %s: Thread is still working on %s" % (self.threadno, self.cur_dir))
      
      self.cur_dir = cur_dir
      self.working = True
      self.producer_sem.release()


# walk an FTP directory, and do something with each directory 
# callback signature: ( abs_path, is_directory )
# callback must return true for a directory to be added to the dir queue to walk
def ftp_walk( ftphost_pool, cwd, callback ):
   
   dir_queue = []
   failed = []
   
   log.info("Starting %s threads for crawling" % len(ftphost_pool) )
   
   total_processed = 1
   
   running = []
   
   i = 0
   
   for ftphost in ftphost_pool:
      ct = ftp_crawl_thread( i, ftphost )
      ct.start()
      
      running.append( ct )
      
      i += 1
   
   running[0].next_dir( cwd )
   
   num_running = lambda: len( filter( lambda th: th.is_working(), running ) )

   while True:
      
      time.sleep(1)
      
      working_list = [th.is_working() for th in running]
      
      added_work = False 
      thread_working = False
      
      log.info("Gather thread results")
      
      # find the finished thread(s) and given them more work
      for i in xrange(0, len(running)):

         if not running[i].is_working():
            
            # did the thread do work?
            files = running[i].consume_files()
            dirs = running[i].consume_dirs()
            status = running[i].consume_crawl_status()
            
            if not status:
               log.error("Failed to explore %s" % running[i].get_cur_dir())
               failed.append( running[i].get_cur_dir() )
               continue
            
            processed_here = 0
            explore = []
            
            if files is not None and dirs is not None:
               
               log.info("Gather thread %s's results (%s items gathered)", i, len(files) + len(dirs))
               
               # process files
               for name in files:
                  abs_path = os.path.join( running[i].get_cur_dir(), name )
                  
                  rc = callback( abs_path, False )
                  
               processed_here += len(files)
               
               
               # process dirs
               for dir_name in dirs:
                  abs_path = os.path.join( running[i].get_cur_dir(), dir_name )
                  
                  rc = callback( abs_path, True )
                  
                  if rc:
                     explore.append( abs_path )
            
               processed_here += len(dirs)
               
            if processed_here > 0:
               total_processed += processed_here
               log.info("%s: %s entries processed (total: %s)" % (running[i].get_cur_dir(), processed_here, total_processed))
               
            if len(explore) > 0:
               dir_queue += explore 
   
      
      log.info("Assign thread work")
      
      for i in xrange(0, len(running)):
         
         if not running[i].is_working():
            # queue up more work
            if len(dir_queue) > 0:
               next_dir = dir_queue[0]
               dir_queue.pop(0)
               
               log.info("Thread %s: explore %s" % (i, next_dir))
               
               running[i].next_dir( next_dir )
               added_work = True
            
            else:
               log.info("Thread %s is not working, but no directories queued", i)

         else:
            log.info("Thread %s is working" % i)
            thread_working = True
               
      log.info("Directories left to explore: %s" % len(dir_queue))
      
      if not added_work and not thread_working:
         break
      
   log.info("Finished exploring %s, shutting down..." % cwd)
   
   # stop all threads 
   for ct in running:
      ct.stop_working()
      
   for ct in running:
      ct.join()
   
   if len(failed) == 0:
      return True
   
   else:
      log.error("Failed to explore the following files and directories:\n%s\n" % ("\n".join( ["    %s" % failed_path for failed_path in failed] )) )
      return False


# add a hierarchy element
def add_hierarchy_element( abs_path, is_directory, include_cb, file_reval_sec_cb, dir_reval_sec_cb, file_perm_cb, dir_perm_cb, host_url, hierarchy_dict ):
   """
   Generate a hierarchy element and put it into hierarchy_dict.
   
   Call include_cb(abs_path, is_directory) to determine whether or not to include this element in the hierarchy.
   
   Call file_reval_sec_cb(abs_path) to determine the file's revalidation time
   Call file_perm_cb(abs_path) to determine the file's permission bits
   
   Call dir_reval_sec_cb(abs_path) to determine the directory's revalidation time 
   Call dir_perm_cb(abs_path) to determine the directory's permission bits
   """
   
   include = include_cb( abs_path, is_directory )
   
   if not include:
      return False 
   
   # duplicate?
   if abs_path in hierarchy_dict.keys():
      log.error("Duplicate entry %s" % abs_path )
      return False
   
   if is_directory:
      
      dir_perm = dir_perm_cb( abs_path )
      reval_sec = dir_reval_sec_cb( abs_path )
      
      dir_data = AG_curation.make_dir_data( abs_path, reval_sec, DRIVER_NAME, dir_perm );
      hierarchy_dict[abs_path] = dir_data 
      
   else:
      
      file_perm = file_perm_cb( abs_path )
      reval_sec = file_reval_sec_cb( abs_path )
      
      file_data = AG_curation.make_file_data( abs_path, reval_sec, DRIVER_NAME, file_perm, host_url + abs_path )
      hierarchy_dict[abs_path] = file_data 
   
   return True


# build a hierarchy 
def build_hierarchy( protocol, hostname, root_dir, num_threads=2, ftp_username="anonymous", ftp_password="", allow_partial_failure=False,
                     include_cb=None, file_reval_sec_cb=None, file_perm_cb=None, dir_reval_sec_cb=None, dir_perm_cb=None ):
   
   host_url = protocol + hostname
   
   if include_cb is None:
      include_cb = lambda path, is_dir: True 

   if file_reval_sec_cb is None:
      file_reval_sec_cb = lambda path: "1d"
   
   if file_perm_cb is None:
      file_perm_cb = lambda path: 0444
   
   if dir_reval_sec_cb is None:
      dir_reval_sec_cb = lambda path: "1d"
   
   if dir_perm_cb is None:
      dir_perm_cb = lambda path: 0555 
   
   ftphost_pool = []
   
   for i in xrange(0, num_threads):
      ftphost = ftputil.FTPHost( hostname, ftp_username, ftp_password )
      
      # big cache 
      ftphost.stat_cache.resize( 50000 )
      ftphost.keep_alive()
      ftphost_pool.append( ftphost )
      
   hierarchy = {}
   
   status = ftp_walk( ftphost_pool, root_dir, lambda abs_path, is_dir: add_hierarchy_element( abs_path, is_dir, include_cb, file_reval_sec_cb, dir_reval_sec_cb, file_perm_cb, dir_perm_cb, host_url, hierarchy ) )
   
   if not status and not allow_partial_failure:
      return None 
   
   # build up the path to the root directory, if we need to 
   if len(root_dir.strip("/")) > 0:
      
      prefixes = []
      names = root_dir.strip("/").split("/")
      p = "/"
      
      prefixes.append(p)
      
      for name in names:
         p += name + "/"
         prefixes.append(p)
         
      for prefix in prefixes:
         add_hierarchy_element( prefix, True, include_cb, file_reval_sec_cb, dir_reval_sec_cb, file_perm_cb, dir_perm_cb, host_url, hierarchy )
         
      
   return hierarchy