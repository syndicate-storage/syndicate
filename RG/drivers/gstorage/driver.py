#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University

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


import StringIO
import os
import sys
import shutil
import tempfile
import time
from gslib.third_party.oauth2_plugin import oauth2_plugin

import boto

# URI scheme for Google Cloud Storage.
GOOGLE_STORAGE = 'gs'
# URI scheme for accessing local files.
LOCAL_FILE = 'file'

#XX this should be removed from here
PROJECT_ID = '344533994214'

DEBUG = True

FILE_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(FILE_ROOT, "config/")

HEADER_VALUES= {"x-goog-api-version": "2",
                "x-goog-project-id": PROJECT_ID}

#-------------------------
def create_bucket(bucket_name):
   
    # instantiate a BucketStorageUri object.
    uri = boto.storage_uri(bucket_name, GOOGLE_STORAGE)
  
    try:
        uri.create_bucket(headers=HEADER_VALUES)
        
    except boto.exception.StorageCreateError, e:
        print "Failed to create bucket: ", e

    else:
        if(DEBUG): print "Successfully created bucket: " + bucket_name

    return True

#-------------------------
def list_buckets():

    uri = boto.storage_uri('', GOOGLE_STORAGE)
  
    for bucket in uri.get_all_buckets(headers=HEADER_VALUES):
        print bucket.name

	return True

#-------------------------
def delete_bucket(bucket_name):

    uri = boto.storage_uri(bucket, GOOGLE_STORAGE)

    
#-------------------------
def write_file(file_name, bucket_name):
    
    if(DEBUG): print "Writing File: " + file_name

    contents = file(file_name, 'r')

    dst_uri = boto.storage_uri(bucket_name + '/' + file_name, GOOGLE_STORAGE)

    # the key-related functions are a consequence of boto's interoperability with s3
    # (concept of a key mapping to contents).
    dst_uri.new_key(headers=HEADER_VALUES).set_contents_from_file(contents)
    
    contents.close()

    if(DEBUG): print 'Written file to GS: "%s/%s"' % (dst_uri.bucket_name, dst_uri.object_name)

    return True

#-------------------------
def read_file(file_name, bucket_name):
    
    if(DEBUG): print "Reading File: " + file_name
 
    # Create a file-like object for holding the object contents.
    object_contents = StringIO.StringIO()

    src_uri = boto.storage_uri(bucket_name + '/' + file_name, GOOGLE_STORAGE)

    # get_file() doesn't return the file contents
    # it writes the file contents to "object_contents" instead
    src_uri.get_key(headers=HEADER_VALUES).get_file(object_contents)

    local_dst_uri = boto.storage_uri(file_name, LOCAL_FILE)

    bucket_dst_uri = boto.storage_uri(bucket_name + '/' + file_name, GOOGLE_STORAGE)

    for dst_uri in (local_dst_uri, bucket_dst_uri):
        object_contents.seek(0)
        dst_uri.new_key(headers=HEADER_VALUES).set_contents_from_file(object_contents)

    object_contents.close()

    if(DEBUG): print "Read data from GCS to file: " + file_name

    return True
   
#-------------------------
def delete_file(file_name, bucket_name):
    
    if(DEBUG): print "Deleting File: " + bucket_name + '/' + file_name

    uri = boto.storage_uri(bucket_name + '/' + file_name, GOOGLE_STORAGE)
  
    #XX find the right delete object method
    #uri.delete_object()
  
    if(DEBUG): print "Deleted GS file: " + bucket_name + '/' + file_name

    return True

#-------------------------
def delete_bucket(bucket_name):
    
    if(DEBUG): print "Deleting bucket: " + bucket_name

    uri = boto.storage_uri(bucket_name, GOOGLE_STORAGE)
  
    #bucket must be empty before it can be deleted
    #so delete objects first
    for obj in uri.get_bucket():
        print 'Deleting object: %s...' % obj.name
        obj.delete()
 
    uri.delete_bucket()

    if(DEBUG): print "Deleted bucket: " + bucket_name 

    return True

#-------------------------    
def usage():
    print 'Usage: {prog} [OPTIONS -w -r -d --list] <file_name> <bucket_name>'.format(prog=sys.argv[0])
    return -1 

#-------------------------    
if __name__ == "__main__":
  
    if len(sys.argv) < 4:
        usage() 
    else:
    	option = sys.argv[1]
    	file_name = sys.argv[2]
    	bucket_name = sys.argv[3]

    	if(option == '-w'):
        	write_file(file_name,bucket_name)
        elif(option == '-r'):
        	read_file(file_name,bucket_name)
        elif(option == '-d'):
        	delete_bucket(bucket_name)
        elif(option == '--create'):
        	create_bucket(bucket_name)
        elif(option == '--list'):
            list_buckets()
        else:
            usage()


