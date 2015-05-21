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
   
   if not can_publish( abs_path, blacklist=blacklist, whitelist=whitelist ):
      return False 
   
   else:
      return True
      
      

# load blacklists and whitelists 
def load_blacklists_and_whitelists( blacklist_paths, whitelist_paths ):
   
   # load whitelists and blacklists
   whitelists = []
   blacklists = []
   
   if whitelist_paths is not None:
      for wlpath in whitelist_paths:
         wl = load_listing( wlpath )
         if wl is None:
            raise AGACLException("Failed to load whitelist %s" % wlpath )
         else:
            log.info("Processed whitelist %s" % wlpath)
            
         whitelists.append(wl)

   if blacklist_paths is not None:
      for blist in blacklist_paths:
         bl = load_listing( blist )
         if bl is None:
            raise AGACLException("Failed to load blacklist %s" % wlpath )
         else:
            log.info("Processed blacklist %s" % blist)
         
         blacklists.append(bl)

   # flatten lists
   whitelist = reduce( lambda x,y: x + y, whitelists, [] )
   blacklist = reduce( lambda x,y: x + y, blacklists, [] )
   
   return (blacklist, whitelist)