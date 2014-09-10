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

import syndicate.ag.curation.specfile as AG_specfile
import syndicate.ag.datasets.ftp as AG_ftp

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )


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
   

# include a file/directory?
def include_in_listing( abs_path, is_dir, blacklist, whitelist ):
   
   if not can_publish( abs_path, blacklist=blacklists, whitelist=whitelist ):
      return False 
   
   else:
      return True
      
   
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
   parser.add_argument( '-t', "--threads", dest="num_threads", default="4", help="Number of threads (default: 4)" ),
   parser.add_argument( '-f', "--fail-fast", dest="fail_fast", action='store_true', help="Fail fast on error.  Do not give back partial results."),
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
   
   try:
      num_threads = int(args.num_threads)
   except:
      parser.print_help()
      sys.exit(1)
      
   # make the hierarchy
   log.info("crawl %s" % "ftp://" + args.hostname[0] + args.root_dir )
   
   hierarchy = AG_ftp.build_hierarchy( "ftp://", args.hostname[0], args.root_dir, num_threads=num_threads, ftp_username=args.username, ftp_password=args.password,
                                       allow_partial_failure = (not args.fail_fast),
                                       include_cb        = lambda path, is_dir: include_in_listing( path, is_dir, blacklist, whitelist ),
                                       file_reval_sec_cb = lambda path: args.reval_sec,
                                       dir_reval_sec_cb  = lambda path: args.reval_sec,
                                       file_perm_cb      = lambda path: file_perm,
                                       dir_perm_cb       = lambda path: dir_perm )
   
   if hierarchy is not None:
      specfile_text = AG_specfile.generate_specfile( {}, hierarchy )
   
      print specfile_text
      