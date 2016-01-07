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
import time

import syndicate.ag.curation.specfile as AG_specfile
import syndicate.ag.curation.crawl as AG_crawl

DRIVER_NAME = "curl"

# you might have to pip install this one...
import ftputil


# list a directory 
def ftp_listdir( ftphost, dirpath ):
   return ftphost.listdir( dirpath )

# is this a directory?
def ftp_isdir( ftphost, dirpath ):
   return ftphost.path.isdir( dirpath )

# build a hierarchy, using sensible default callbacks
def build_hierarchy( hostname, root_dir, include_cb, ftp_specfile_cbs, num_threads=2, max_retries=3, ftp_username="anonymous", ftp_password="", allow_partial_failure=False ):
   
   ftphost_pool = []
   
   for i in xrange(0, num_threads):
      ftphost = ftputil.FTPHost( hostname, ftp_username, ftp_password )
      
      # big cache 
      ftphost.stat_cache.resize( 50000 )
      ftphost.keep_alive()
      ftphost_pool.append( ftphost )
   
   ftp_crawler_cbs = AG_crawl.crawler_callbacks( include_cb=include_cb,
                                                 listdir_cb=ftp_listdir,
                                                 isdir_cb=ftp_isdir )
   
   hierarchy = AG_crawl.build_hierarchy( ftphost_pool, root_dir, DRIVER_NAME, ftp_crawler_cbs, ftp_specfile_cbs, allow_partial_failure=allow_partial_failure, max_retries=max_retries )
   
   for ftphost in ftphost_pool:
      ftphost.close()
      
   return hierarchy