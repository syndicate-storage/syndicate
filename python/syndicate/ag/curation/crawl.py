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

import syndicate.ag.curation.specfile as AG_specfile

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )

class crawler_callbacks(object):
   """
   Callbacks the crawler will invoke to generate a specfile.
   """
   
   include_cb = None    # include_cb(path, is_directory) => {True,False}
   listdir_cb = None    # listdir_cb(path) => [names of children]
   isdir_cb = None      # isdir_cb(path) => {True,False}
   
   def __init__(self, include_cb=None,
                      listdir_cb=None,
                      isdir_cb=None ):
      
      if include_cb is None:
         include_cb = lambda path, is_dir: True

      self.include_cb = include_cb
      self.listdir_cb = listdir_cb 
      self.isdir_cb = isdir_cb
      
      

# do this in its own thread 
class crawl_thread( threading.Thread ):
   
   def __init__(self, threadno, context, callbacks, max_retries ):
      """
      Make a new crawler thread.
      * listdir_cb is a function that takes (context, absolute path to a directory) as arguments
      and returns a list of names (not paths) of its immediate children.
      * isdir_cb is a function that takes (context, absolute path to a dataset entry) as arguments and returns 
      True if it is a directory.
      """
      
      super( crawl_thread, self ).__init__()
      self.callbacks = callbacks
      self.threadno = threadno
      self.producer_sem = threading.Semaphore(0)
      self.context = context 
      self.max_retires = max_retries
      
      self.running = True
      self.result_files = None
      self.result_dirs = None
      self.working = False
      self.cur_dir = None
      self.crawl_status = True
      
   @classmethod
   def crawl( cls, threadno, cur_dir, context, callbacks, max_retries ):
      
      log.info( "thread %s: listdir %s" % (threadno, cur_dir ) )
      
      names = None
      for i in xrange(0, max_retries):
         try:
            names = callbacks.listdir_cb( context, cur_dir )
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
         
         abs_path = "/" + os.path.join( cur_dir.strip("/"), name.strip("/") )
         
         is_directory = False
         
         for i in xrange(0, max_retries):
            try:
               is_directory = callbacks.isdir_cb( context, abs_path )
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
      
         self.result_files, self.result_dirs, self.crawl_status = crawl_thread.crawl( self.threadno, self.cur_dir, self.context, self.callbacks, self.max_retires )
         
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


# walk a dataset, and do something with each directory 
# give each thread one of context_list's items
# include_cb takes (absolute path to a dataset entry, whether or not it is a directory) as arguments,
# and must return True for a directory to be explored further.
def walk_dataset( context_list, root_dir, callbacks, max_retries ):
   
   dir_queue = []
   failed = []
   
   log.info("Starting %s threads for crawling" % len(context_list) )
   
   total_processed = 1
   
   running = []
   walk_stats = {}      # map directories to child counts
   
   i = 0
   
   for context in context_list:
      ct = crawl_thread( i, context, callbacks, max_retries )
      ct.start()
      
      running.append( ct )
      
      i += 1
   
   dir_queue.append( root_dir )
   
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
                  
                  rc = callbacks.include_cb( abs_path, False )
                  
               processed_here += len(files)
               
               
               # process dirs
               for dir_name in dirs:
                  abs_path = os.path.join( running[i].get_cur_dir(), dir_name )
                  
                  rc = callbacks.include_cb( abs_path, True )
                  
                  if rc:
                     explore.append( abs_path )
            
               processed_here += len(dirs)
               
            if processed_here > 0:
               total_processed += processed_here
               log.info("%s: %s entries processed (total: %s)" % (running[i].get_cur_dir(), processed_here, total_processed))

               if not walk_stats.has_key( running[i].get_cur_dir() ):
                   walk_stats[ running[i].get_cur_dir() ] = processed_here
               else:
                   walk_stats[ running[i].get_cur_dir() ] += processed_here
               
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
      
   
   stats_buf = ""
   for (dirname, count) in walk_stats.items():
       stats_buf += "% 15s %s\n" % (count, dirname)

   log.info("Walk stats:\n%s" % stats_buf )

   log.info("Finished exploring %s, shutting down..." % root_dir)
   
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



# build a hierarchy 
def build_hierarchy( contexts, root_dir, driver_name, crawler_cbs, specfile_cbs, allow_partial_failure=False, max_retries=1 ):
   """
   Given a crawler_callbacks and specfile_callbacks bundle and a list of contexts, generate a hierarchy by crawling the dataset.
   Spawn one thread per context 
   """
   
   hierarchy_dict = {}
   
   # generate and store data based on the caller's include_cb
   generator_cb = lambda abs_path, is_dir: AG_specfile.add_hierarchy_element( abs_path, is_dir, driver_name, crawler_cbs.include_cb, specfile_cbs, hierarchy_dict )
   
   # override the include_cb in crawler_cbs to build up the hierarchy, based on the user-given include_cb's decisions 
   generator_callbacks = crawler_callbacks( include_cb=generator_cb,
                                            listdir_cb=crawler_cbs.listdir_cb,
                                            isdir_cb=crawler_cbs.isdir_cb )
   
   status = walk_dataset( contexts, root_dir, generator_callbacks, max_retries )
   
   if not status and not allow_partial_failure:
      return None 
   
   AG_specfile.add_hierarchy_prefixes( root_dir, driver_name, crawler_cbs.include_cb, specfile_cbs, hierarchy_dict )
   
   return hierarchy_dict
