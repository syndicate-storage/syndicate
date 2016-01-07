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

import argparse
import logging 
import sys

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )

def make_arg_parser( tool_desc ):
   """
   Generate a default argparser for a curation tool.
   """
   parser = argparse.ArgumentParser( description=tool_desc )
   
   parser.add_argument( '-b', "--blacklist", dest="blacklists", action='append', help='File containing a list of strings or regular expressions, separated by newlines, that describe paths that should NOT be published.  Any FTP path matched by a blacklisted path or regular expression will be ignored.  If not given, no paths are blacklisted.  This argument can be given multiple times.')
   parser.add_argument( '-w', "--whitelist", dest="whitelists", action='append', help="File containing a list of strings or regular expressions, separated by newlines, that describe the paths that should be published.  An FTP path must be matched by at least one whitelisted path or regular expression to be published.  If not given, all paths are whitelisted.  This argument can be given multiple times.")
   parser.add_argument( '-d', "--debug", dest="debug", action='store_true', help="If set, print debugging information.")
   parser.add_argument( '-t', "--threads", dest="num_threads", default="4", help="Number of threads (default: 4)" )
   parser.add_argument( '-f', "--fail-fast", dest="fail_fast", action='store_true', help="Fail fast on error.  Do not give back partial results.")
   parser.add_argument( '-a', "--attempts", dest="max_attempts", default="3", help="Number of times to retry transient FTP errors.")
   
   return parser 

def get_num_threads_or_die( args ):
   
   try:
      num_threads = int(args.num_threads)
      return num_threads
   except:
      parser.print_help()
      sys.exit(1)
      
def get_num_retries_or_die( args ):
   
   try:
      max_retries = int(args.max_attempts)
      return max_retries
   except:
      parser.print_help()
      sys.exit(1)