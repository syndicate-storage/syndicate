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
import subprocess
import StringIO

import syndicate.ag.curation.specfile as AG_specfile
import syndicate.ag.curation.crawl as AG_crawl
from syndicate.ag.curation.specfile import AGCurationException


logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )

DRIVER_NAME = "shell"

# where is Google's gsutil?
GSUTIL_PROTOCOL = "gs://"
GSUTIL_ROOT = "m-lab"
GSUTIL_LISTING_TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

# global listing of all datasets available
ALL_MLAB_DATASETS_URL = "%sm-lab/list/all_mlab_tarfiles.txt.gz" % GSUTIL_PROTOCOL


class gsutil_context(object):
   """
   New-style object, that we can set attributes on willy-nilly.
   """
   gsutil_binary_path = None
   dirs = None


def gsutil_parse_path( url ):
   """
   Extract the path from a gs url.
   Return the path on success; None on error
   """
   
   if not url.startswith( GSUTIL_PROTOCOL ):
      return None 
   
   try:
      path = url[len(GSUTIL_PROTOCOL + GSUTIL_ROOT):]
   except:
      return None 
   
   return path 


def gsutil_parse_file_listing( listing_line ):
   """
   Parse a file's line of gsutil's dataset listing.
   Return (path, size, UTC-mtime), or None on parse error.
   """
   
   parts = listing_line.strip().split()
   
   # format: [size, mtime, gs://path]
   size = None 
   mtime = None 
   path = None 
   
   try:
      size = int(parts[0])
   except:
      log.error("No size in '%s' of '%s'" % (parts[0], listing_line))
      return None 
   
   try:
      mtime_struct = time.strptime( parts[1], GSUTIL_LISTING_TIME_FMT )
      mtime = int( time.mktime(mtime_struct) )
   except:
      log.error("No mtime in '%s' of '%s'" % (parts[1], listing_line))
      return None 
   
   path = gsutil_parse_path( parts[2] )
   if path is None:
      log.error("Invalid URL '%s' in '%s'" % (parts[2], listing_line))
      return None 
   
   return (path, size, mtime)


def gsutil_parse_dir_listing( listing_line ):
   """
   Parse a directory's line of gsutil's dataset listing.
   Return the path, since that's all gsutil gives back.
   """
   
   part = listing_line.strip()
   return gsutil_parse_path( part )
   

def gsutil_list_dataset( gsutil_binary_path, url ):
   """
   List a dataset (ls -l)
   Return the contents.
   """
   gs_args = "%s ls -l %s" % (gsutil_binary_path, url)
   
   p = subprocess.Popen( gs_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
   gs_output_buf, gs_error_buf = p.communicate()
   
   if p.returncode != 0:
      raise AGCurationException("gsutil exited with rc = %s, stderr: \"%s\"" % (p.returncode, gs_error_buf))
   
   return gs_output_buf


def gsutil_listdir( context, dir_path ):
   """
   Given an absolute path to a directory, list its contents.
   context should be an instance of gsutil_context.
   Return the listing of names on success
   Raise an exception on error
   """
   
   url = GSUTIL_PROTOCOL + GSUTIL_ROOT + dir_path 
   
   listing_text = gsutil_list_dataset( context.gsutil_binary_path, url )
   listing = listing_text.strip().split("\n")
   
   names = []
   dirpaths = []
   
   for line in listing[:-1]:    # the last line is a TOTAL: listing
      file_data = gsutil_parse_file_listing( line )
      dir_data = gsutil_parse_dir_listing( line )
      
      if file_data is None and dir_data is None:
         raise Exception("Failed to parse '%s'" % line )
      
      elif file_data is None:
         # this is a directory 
         dir_data = dir_data.rstrip("/")
         names.append( os.path.basename( dir_data ) )
         dirpaths.append( dir_data )
         
      else:
         # this is a file 
         path, _, _ = file_data 
         path = path.rstrip("/")
         names.append( os.path.basename( path ) )
         
         
   # remember these for isdir 
   context.dirs = dirpaths 
   
   return names
   

def gsutil_isdir( context, path ):
   """
   Given an absolute path, determine whether or not it is a directory.
   """
   return path.rstrip("/") in context.dirs 
   

# build the hierarchy 
def build_hierarchy( root_dir, gsutil_binary_path, include_cb, gs_specfile_cbs, max_retries=3, num_threads=2, allow_partial_failure=False, ):   
   """
   Build up the directory heirarchy and return it
   """
   
   contexts = []
   for i in xrange(0,num_threads):
      context = gsutil_context()
      
      context.gsutil_binary_path = gsutil_binary_path
      
      contexts.append( context )
   
   gs_crawler_cbs = AG_crawl.crawler_callbacks( include_cb=include_cb,
                                                listdir_cb=gsutil_listdir,
                                                isdir_cb=gsutil_isdir )
   
   hierarchy = AG_crawl.build_hierarchy( contexts, root_dir, DRIVER_NAME, gs_crawler_cbs, gs_specfile_cbs, allow_partial_failure=allow_partial_failure, max_retries=max_retries )
   
   return hierarchy