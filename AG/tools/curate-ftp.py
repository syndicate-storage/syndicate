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

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )

import collections

AG_file_data = collections.namedtuple("AG_file_data", ["path", "reval_sec", "driver", "perm", "query_string"] )
AG_dir_data = collections.namedtuple("AG_dir_data", ["path", "reval_sec", "driver", "perm"] )

# you might have to pip install this one...
import ftputil

# walk an FTP directory, and do something with each directory 
# callback signature: ( abs_path, is_directory )
# callback must return true for a directory to be added to the dir queue to walk
def ftp_walk( ftphost, cwd, callback ):
   
   dir_queue = []
   dir_queue.append( cwd )
   
   # process root
   rc = callback( cwd, True )
   if not rc:
      return rc
   
   total_processed = 1
   
   while len(dir_queue) > 0:
      
      cur_dir = dir_queue[0]
      dir_queue.pop(0)
      
      log.info( "listdir %s" % cur_dir )
      
      try:
         # list the current dir
         names = ftphost.listdir( cur_dir )
      except Exception, e:
         log.exception(e)
         return False
      
      dirs = []
      
      for name in names:
         
         abs_path = os.path.join( cur_dir, name )
         is_directory = ftphost.path.isdir( abs_path )
         
         rc = callback( abs_path, is_directory )
         
         if rc and is_directory:
            dirs.append( abs_path )
      
      total_processed += len(names)
      log.info("%s: %s entries processed (total: %s)" % (cur_dir, len(names), total_processed))
      
      dir_queue += dirs 
      
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
   
   ftphost = ftputil.FTPHost( args.hostname[0], args.username, args.password )
   
   # big cache 
   ftphost.stat_cache.resize( 50000 )
   
   ret = ftp_walk( ftphost, args.root_dir, lambda path, is_dir: build_hierarchy( path, is_dir, whitelist, blacklist, args.reval_sec, file_perm, dir_perm, url_prefix, hierarchy ) )
   
   if not ret:
      print >> sys.stderr, "Failed to load hierarchy from %s" % url_prefix
   
   else:
      specfile_text = generate_specfile( hierarchy, "ftp://" + args.hostname[0] )
      
      print specfile_text 
      