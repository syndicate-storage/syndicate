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
import syndicate.ag.datasets.ftp as AG_ftp


logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )
   
if __name__ == "__main__":
   parser = AG_args.make_arg_parser( "Syndicate Acquisition Gateway FTP dataset curation tool" )
   
   parser.add_argument( '-F', "--file-perm", dest="file_perm", required=True, help="Permission bits (in octal) to be applied for each file.")
   parser.add_argument( '-D', "--dir-perm", dest="dir_perm", required=True, help="Permission bits (in octal) to be applied for each directory.")
   parser.add_argument( '-R', "--reval", dest="reval_sec", required=True, help="Length of time to go between revalidating entries.  Formatted as a space-separated list of numbers, with units 's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days.  For example, '10d 12h 30m' means ten days, twelve hours, and thirty minutes.")
   parser.add_argument( '-u', "--username", dest="username", default="anonymous", help="FTP username.")
   parser.add_argument( '-p', "--password", dest="password", default="", help="FTP password.")
   parser.add_argument( '-r', "--root-dir", dest="root_dir", default="/", help="Directory on the FTP server to treat as the root directory for this Volume.")
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
   
   num_threads = AG_args.get_num_threads_or_die( args )
   max_retries = AG_args.get_num_retries_or_die( args )
      
   blacklists, whitelists = AG_acl.load_blacklists_and_whitelists( args.blacklists, args.whitelists )
   
   # make the hierarchy
   log.info("crawl %s" % "ftp://" + args.hostname[0] + args.root_dir )
   
   ftp_include_callback = lambda path, is_directory: AG_acl.include_in_listing( path, is_directory, blacklists, whitelists )
   
   ftp_specfile_callbacks = AG_specfile.specfile_callbacks( file_reval_sec_cb = lambda path: args.reval_sec,
                                                            dir_reval_sec_cb  = lambda path: args.reval_sec,
                                                            file_perm_cb      = lambda path: file_perm,
                                                            dir_perm_cb       = lambda path: dir_perm,
                                                            query_string_cb   = lambda path: "ftp://" + args.hostname[0] + os.path.join( args.root_dir, path.strip("/") ) )
   
   hierarchy = AG_ftp.build_hierarchy( args.hostname[0], args.root_dir, ftp_include_callback, ftp_specfile_callbacks, 
                                       num_threads           = num_threads,
                                       ftp_username          = args.username,
                                       ftp_password          = args.password,
                                       max_retries           = max_retries,
                                       allow_partial_failure = (not args.fail_fast) )
   
   if hierarchy is not None:
      specfile_text = AG_specfile.generate_specfile( {}, hierarchy )
   
      print specfile_text
      