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
import syndicate.ag.curation.crawl as AG_crawl
import syndicate.ag.curation.args as AG_args
import syndicate.ag.curation.acl as AG_acl
import syndicate.ag.datasets.mlab as AG_mlab

GSUTIL_BINARY_PATH = "/usr/local/bin/gsutil"

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )
   
if __name__ == "__main__":
   parser = AG_args.make_arg_parser( "Syndicate Acquisition Gateway M-Lab dataset curation tool" )
   
   parser.add_argument( '-F', "--file-perm", dest="file_perm", required=True, help="Permission bits (in octal) to be applied for each file.")
   parser.add_argument( '-D', "--dir-perm", dest="dir_perm", required=True, help="Permission bits (in octal) to be applied for each directory.")
   parser.add_argument( '-R', "--reval", dest="reval_sec", required=True, help="Length of time to go between revalidating entries.  Formatted as a space-separated list of numbers, with units 's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days.  For example, '10d 12h 30m' means ten days, twelve hours, and thirty minutes.")
   parser.add_argument( '-G', "--gsutil-path", dest="gsutil_path", help="Path to the gsutil binary.")
   parser.add_argument( '-L', "--listing", dest="listing_path", help="Compressed dataset listing path.  If not given, it will be downloaded.")

   args = parser.parse_args()

   if args.debug:
      log.setLevel( logging.DEBUG )
   
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
   
   num_threads = AG_args.get_num_threads_or_die( args )
   max_retries = AG_args.get_num_retries_or_die( args )
   
   try:
      gsutil_binary_path = args.gsutil_path
   except:
      gsutil_binary_path = GSUTIL_BINARY_PATH
      
   try:
      listing_path = args.listing_path 
   except:
      listing_path = None
      
   blacklists, whitelists = AG_acl.load_blacklists_and_whitelists( args.blacklists, args.whitelists )
   
   # make the hierarchy
   log.info("crawl gs://m-lab%s" % args.root_dir )
   
   mlab_include_callback = lambda path, is_directory: AG_acl.include_in_listing( path, is_directory, blacklists, whitelists )
   
   mlab_specfile_callbacks = AG_specfile.specfile_callbacks( file_reval_sec_cb = lambda path: args.reval_sec,
                                                             dir_reval_sec_cb  = lambda path: args.reval_sec,
                                                             file_perm_cb      = lambda path: file_perm,
                                                             dir_perm_cb       = lambda path: dir_perm,
                                                             query_string_cb   = lambda path: gsutil_binary_path + " cat " + AG_mlab.GSUTIL_PROTOCOL + AG_mlab.GSUTIL_ROOT + os.path.join( args.root_dir, path.strip("/") ) )
   
   AG_mlab.generate_specfile_from_global_listing( gsutil_binary_path, args.root_dir, mlab_include_callback, mlab_specfile_callbacks, sys.stdout, max_retries=max_retries, compressed_listing_path=listing_path )
   