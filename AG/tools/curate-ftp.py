#!/usr/bin/python 

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

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )

import collections

AG_file_data = collections.namedtuple("AG_file_data", ["path", "reval_sec", "driver", "perm", "query_string"] )
AG_dir_data = collections.namedtuple("AG_dir_data", ["path", "reval_sec", "driver", "perm"] )

MAX_RETRIES = 3

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
            log.error("thread %s: Trying to crawl %s again" % (threadno, cur_dir) )
            time.sleep(1)
            pass


      if names is None:
         return (None, None)
      
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
               log.error("thread %s: Trying to isdir %s again" % (threadno, abs_path))
               time.sleep(1)
               pass
         
         if is_directory:
            dirs.append( name )

         else:
            files.append( name )
            
      # done!
      return (files, dirs)
   
   def run(self):
      
      while self.running:
         
         # wait for work
         self.producer_sem.acquire()
         
         if not self.running:
            return
      
         work = self.cur_dir 
         
         self.result_files, self.result_dirs = ftp_crawl_thread.crawl( self.threadno, self.ftphost, work )
         
         self.working = False
         
         log.info("thread %s: expored %s" % (self.threadno, self.cur_dir ))
   
   
   def stop_working(self):
      self.running = False 
      self.producer_sem.release()
      
   def is_working(self):
      return self.working
      
   def get_files(self):
      ret = self.result_files
      self.result_files = None
      return ret
   
   def get_dirs( self ):
      ret = self.result_dirs
      self.result_dirs = None
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
            files = running[i].get_files()
            dirs = running[i].get_dirs()
            
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
      
   return True


# check whitelist/blacklist
def matches_listing( abs_path, listing ):
   for ll in listing:
      # either a string or regular expression
      if type(ll) == str or type(ll) == unicode:
         if str(abs_path).rstrip("/") == str(ll).rstrip("/"):
            return ll
      
      elif ll.match(abs_path) is not None:
         return ll.pattern
      
   return None


# check whitelist and blacklist to see if the path should be published
def can_publish( abs_path, blacklist=[], whitelist=[] ):
   
   if len(blacklist) > 0:
      ll = matches_listing( abs_path, blacklist )
      if ll is not None:
         log.info("%s is blacklisted by %s" % (abs_path, ll))
         return False
   
   if len(whitelist) > 0:
      ll = matches_listing( abs_path, whitelist )
      if ll is None:
         log.info("%s is not whitelisted" % abs_path)
         return False
      
   return True


# generate data for a file, respecting blacklists and whitelists 
def ftp_file_data( abs_path, blacklist=[], whitelist=[], reval_sec=None, perm=None, driver="ftp", url_prefix=None ):
   
   if not can_publish( abs_path, blacklist=blacklist, whitelist=whitelist ):
      return None
      
   file_data = AG_file_data( path=abs_path, reval_sec=reval_sec, driver=driver, perm=perm, query_string=os.path.join( url_prefix, abs_path ) )
   return file_data 


# generate data for a directory, respecting blacklists and whitelists 
def ftp_dir_data( abs_path, blacklist=[], whitelist=[], reval_sec=None, perm=None, driver="ftp" ):
   
   if not can_publish( abs_path, blacklist=blacklist, whitelist=whitelist ):
      return None
      
   dir_data = AG_dir_data( path=abs_path, reval_sec=reval_sec, perm=perm, driver=driver )
   return dir_data 


# load a listing 
def load_listing( path ):
   
   try:
      fd = open(path, "r")
      lines = fd.readlines()
      fd.close()
   except Exception, e:
      log.exception(e)
      print >> sys.stderr, "Could not read %s" % path
      return None
   
   listing = []
   
   for i in xrange(0,len(lines)):
      # format: [re|path] [text]
      
      line = lines[i]
      
      parts = line.strip().split(" ", 1)
      if len(parts) != 2:
         print >> sys.stderr, "%s, line %s: Malformed line" % (path, i)
         return None
      
      if parts[0] == 're':
         
         try:
            regexp = re.compile( parts[1] )
         except Exception, e:
            log.exception(e)
            print >> sys.stderr, "%s, line %s: Could not parse regular expression '%s'" % (path, i, parts[1] )
            return None
         
         listing.append( regexp )
         
         log.info("Blacklist paths matching '%s'" % parts[1] )
         
      elif parts[0] == 'path':
         
         listing.append( parts[1] )
         
         log.info("Blacklist path '%s'" % parts[1] )
         
      else:
         print >> sys.stderr, "%s, line %s: Unknown listing type '%s'"  % (path, i, parts[1] )
         return None
      
   return listing
   

# build up a hierarchy in RAM 
def build_hierarchy( abs_path, is_directory, whitelist, blacklist, reval_sec, file_perm, dir_perm, url_prefix, hierarchy_dict ):
   
   # duplicate?
   if abs_path in hierarchy.keys():
      log.error("Duplicate entry %s" % abs_path )
      return False
   
   if is_directory:
      dir_data = ftp_dir_data( abs_path, blacklist=blacklist, whitelist=whitelist, reval_sec=reval_sec, perm=dir_perm )
      if dir_data is not None:
         # success 
         hierarchy_dict[abs_path] = dir_data 
         return True
      
      else:
         return False
      
   else:
      file_data = ftp_file_data( abs_path, blacklist=blacklist, whitelist=whitelist, reval_sec=reval_sec, perm=file_perm, url_prefix=url_prefix )
      if file_data is not None:
         # success
         hierarchy_dict[abs_path] = file_data 
         return True 
      
      else:
         return False 
      

# generate XML output 
def generate_specfile( hierarchy_dict, ftp_hostname ):
   
   log.info("Generating specfile")
   
   import lxml
   import lxml.etree as etree
   
   root = etree.Element("Map")
   
   # empty config
   config = etree.Element("Config")
   root.append( config )
   
   paths = hierarchy_dict.keys()
   paths.sort()
   
   for path in paths:
      
      h = hierarchy[path]
      ent = None 
      query = None
      
      if h.__class__.__name__ == "AG_file_data":
         # this is a file 
         ent = etree.Element("File", perm=oct(h.perm) )
         query = etree.Element("Query", type=h.driver )
         
         query.text = h.query_string
                               
      else:
         # this is a directory 
         ent = etree.Element("Dir", perm=oct(h.perm) )
         query = etree.Element("Query", type=h.driver )

      ent.text = h.path
      
      pair = etree.Element("Pair", reval=h.reval_sec)
      
      pair.append( ent )
      pair.append( query )
      
      root.append( pair )
      
   return etree.tostring( root, pretty_print=True )
   
   
if __name__ == "__main__":
   parser = argparse.ArgumentParser( description="Syndicate Acquisition Gateway FTP dataset curation tool" )
   parser.add_argument( '-b', "--blacklist", dest="blacklists", action='append', help='File containing a list of strings or regular expressions, separated by newlines, that describe paths that should NOT be published.  Any FTP path matched by a blacklisted path or regular expression will be ignored.  If not given, no paths are blacklisted.  This argument can be given multiple times.')
   parser.add_argument( '-w', "--whitelist", dest="whitelists", action='append', help="File containing a list of strings or regular expressions, separated by newlines, that describe the paths that should be published.  An FTP path must be matched by at least one whitelisted path or regular expression to be published.  If not given, all paths are whitelisted.  This argument can be given multiple times.")
   parser.add_argument( '-F', "--file-perm", dest="file_perm", required=True, help="Permission bits (in octal) to be applied for each file.")
   parser.add_argument( '-D', "--dir-perm", dest="dir_perm", required=True, help="Permission bits (in octal) to be applied for each directory.")
   parser.add_argument( '-R', "--reval", dest="reval_sec", required=True, help="Length of time to go between revalidating entries.  Formatted as a space-separated list of numbers, with units 's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days.  For example, '10d 12h 30m' means ten days, twelve hours, and thirty minutes.")
   parser.add_argument( '-r', "--root-dir", dest="root_dir", default="/", help="Directory on the FTP server to treat as the root directory for this Volume.")
   parser.add_argument( '-u', "--username", dest="username", default="anonymous", help="FTP username.")
   parser.add_argument( '-p', "--password", dest="password", default="", help="FTP password.")
   parser.add_argument( '-d', "--debug", dest="debug", action='store_true', help="If set, print debugging information.")
   parser.add_argument( '-t', "--threads", dest="num_threads", default="4", help="Number of threads (default: 4)" )
   parser.add_argument( "hostname", nargs=1, help="Name of the host to be crawled.")

   args = parser.parse_args()

   if args.debug:
      log.setLevel( logging.DEBUG )
      
   # sanity check: only one host
   if len(args.hostname) > 1:
      print >> sys.stderr, "ERROR: Only one hostname is allowed"
      parser.print_help()
      sys.exit(1)
      
   # sanity check: octal file perm and dir perm
   file_perm = 0
   dir_perm = 0
   
   try:
      file_perm = int( args.file_perm, 8 )
      dir_perm = int( args.dir_perm, 8 )
   except:
      print >> sys.stderr, "ERROR: invalid permission string"
      parser.print_help()
      sys.exit(1)
      
   # load whitelists and blacklists
   whitelists = []
   blacklists = []
   
   if args.whitelists is not None:
      for wlpath in args.whitelists:
         wl = load_listing( wlpath )
         if wl is None:
            sys.exit(1)
         else:
            log.info("Processed whitelist %s" % wlpath)
            
         whitelists.append(wl)

   if args.blacklists is not None:
      for blist in args.blacklists:
         bl = load_listing( blist )
         if bl is None:
            sys.exit(1)
         else:
            log.info("Processed blacklist %s" % blist)
         
         blacklists.append(bl)

   # flatten lists
   whitelist = reduce( lambda x,y: x + y, whitelists, [] )
   blacklist = reduce( lambda x,y: x + y, blacklists, [] )
   
   # make the hierarchy
   hierarchy = {}
   
   url_prefix = "ftp://" + args.hostname[0] + args.root_dir
   log.info("crawl %s" % url_prefix )
   
   ftphost_pool = []
   
   try:
      num_threads = int(args.num_threads)
   except:
      log.error("Invalid argument for number of threads")
      parser.print_help()
      sys.exit(1)
   
   for i in xrange(0, num_threads):
      ftphost = ftputil.FTPHost( args.hostname[0], args.username, args.password )
      
      # big cache 
      ftphost.stat_cache.resize( 50000 )
      ftphost.keep_alive()
      ftphost_pool.append( ftphost )
   
   
   ret = ftp_walk( ftphost_pool, args.root_dir, lambda path, is_dir: build_hierarchy( path, is_dir, whitelist, blacklist, args.reval_sec, file_perm, dir_perm, url_prefix, hierarchy ) )
   
   if not ret:
      print >> sys.stderr, "Failed to load hierarchy from %s" % url_prefix
   
   else:
      specfile_text = generate_specfile( hierarchy, "ftp://" + args.hostname[0] )
      
      print specfile_text 
      