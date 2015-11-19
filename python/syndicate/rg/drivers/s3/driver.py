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
 
import sys
import os
import boto
import logging 
import errno

from boto.s3.key import Key

log = logging.get_logger()
formatter = logging.Formatter('[%(levelname)s] [%(module)s:%(lineno)d] %(message)s')
handler_stream = logging.StreamHandler()
handler_stream.setFormatter(formatter)
log.addHandler(handler_stream)

#-------------------------
def get_bucket( bucket_name, secrets ):

   aws_id = secrets.get( 'AWS_ACCESS_KEY_ID', None )
   aws_key = secrets.get( 'AWS_SECRET_ACCESS_KEY', None )
   
   assert aws_id is not None, "No AWS ID given"
   assert aws_key is not None, "No AWS key given"
   
   try:
      conn = boto.connect_s3(aws_id, aws_key)
   except Exception, e:
      log.error("Connection to S3 failed")
      log.exception(e)
      return None
      
   else:
      log.debug("Connected to S3")

   bucket = None
   try:
      bucket = conn.create_bucket(bucket_name)
   except Exception, e:
      log.error("Could not create/fetch bucket " + bucket_name)
      log.exception(e)
      
   else:
      log.debug("Fetched/created bucket: " + bucket_name)

   return bucket

#-------------------------
def write_chunk( chunk_path, chunk_buf, config, secrets ):
   
   log.debug("Writing File: " + chunk_path)

   assert config is not None, "No config given"
   assert secrets is not None, "No AWS API tokens given"
   assert config.has_key("BUCKET"), "No bucket name given"
   
   bucket_name = config['BUCKET']
   
   bucket = get_bucket(context, bucket_name)
   if bucket == None:
      raise Exception("Failed to get bucket")

   k = Key(bucket)
   k.key = chunk_path

   rc = 0
   try:
      k.set_contents_from_string( chunk_buf )
      log.debug("Wrote %s to s3" % chunk_path)
      
   except Exception, e:
      log.error("Failed to write file %s" % chunk_path)
      log.exception(e)
      rc = -errno.EREMOTEIO
   
   return rc

#-------------------------
def read_chunk( chunk_path, outfile, config, secrets ):
   
   log.debug("Reading File: " + chunk_path)

   assert config is not None, "No config given"
   assert secrets is not None, "No AWS API tokens given"
   assert config.has_key("BUCKET"), "No bucket name given"
   
   bucket_name = config['BUCKET']
   
   bucket = get_bucket(context, bucket_name)
   if bucket == None:
      raise Exception("Failed to get bucket")

   k = Key(bucket)
   k.key = chunk_path

   rc = 0
   
   try:
      k.get_contents_to_file(outfile)
      log.debug("Read data from s3")
      
   except Exception, e:
      log.error("Failed to read %s" % chunk_path)
      log.exception(e)
      rc = -errno.REMOTEIO
      
   return rc
   
#-------------------------
def delete_chunk(chunk_path, config, secrets ):
   
   log.debug("Deleting File: " + chunk_path)
   
   assert config is not None, "No config given"
   assert secrets is not None, "No AWS API tokens given"
   assert config.has_key("BUCKET"), "No bucket name given"
   
   bucket_name = config['BUCKET']
   
   bucket = get_bucket(context, bucket_name)
   if bucket == None:
      raise Exception("Failed to get bucket")

   k = Key(bucket)
   k.key = chunk_path
   
   rc = 0
   try:
      k.delete()
      log.debug("Deleted s3 file %s" % (chunk_path) )
   except Exception, e:
      log.error("Failed to delete %s" % chunk_path)
      log.exception(e)
      rc = -errno.REMOTEIO

   return rc

