#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved
 
import sys
import os
import boto

def get_bucket(context, bucket_name):

   aws_id = context.secrets.get( 'AWS_ACCESS_KEY_ID', None )
   aws_key = context.secrets.get( 'AWS_SECRET_ACCESS_KEY', None )
   
   print "key ID: '%s'" % aws_id
   print "key   : '%s'" % aws_key

   log = context.log
   
   #from boto.s3.connection import Location

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
def write_file(file_name, infile, **kw):
   
   context = kw['context']
   
   log = context.log
   config = context.config
   secrets = context.secrets
   
   log.debug("Writing File: " + file_name)

   assert config != None, "No config given"
   assert secrets != None, "No AWS API tokens given"
   assert config.has_key("BUCKET"), "No bucket name given"
   
   bucket_name = config['BUCKET']
   
   bucket = get_bucket(context, bucket_name)
   if bucket == None:
      raise Exception("Failed to get bucket")

   from boto.s3.key import Key
   k = Key(bucket)
   k.key = file_name

   rc = 500
   try:
      k.set_contents_from_file(infile)
      rc = 200
      log.debug("Wrote %s to s3" % file_name)
      
   except Exception, e:
      log.error("Failed to write file %s" % file_name)
      log.exception(e)
   
   return rc

#-------------------------
def read_file( file_name, outfile, **kw ):
   
   context = kw['context']
   
   log = context.log
   config = context.config
   secrets = context.secrets
   
   
   log.debug("Reading File: " + file_name)

   assert config != None, "No config given"
   assert secrets != None, "No AWS API tokens given"
   assert config.has_key("BUCKET"), "No bucket name given"
   
   bucket_name = config['BUCKET']
   
   bucket = get_bucket(context, bucket_name)
   if bucket == None:
      raise Exception("Failed to get bucket")

   from boto.s3.key import Key
   
   k = Key(bucket)
   k.key = file_name

   rc = 500
   
   try:
      k.get_contents_to_file(outfile)
      rc = 200
      log.debug("Read data from s3")
   except Exception, e:
      log.error("Failed to read %s" % file_name)
      log.exception(e)
   
   return rc
   
#-------------------------
def delete_file(file_name, **kw):
   
   context = kw['context']
   
   log = context.log
   config = context.config
   secrets = context.secrets
   
   log.debug("Deleting File: " + file_name)
   
   assert config != None, "No config given"
   assert secrets != None, "No AWS API tokens given"
   assert config.has_key("BUCKET"), "No bucket name given"
   
   bucket_name = config['BUCKET']
   
   bucket = get_bucket(context, bucket_name)
   if bucket == None:
      raise Exception("Failed to get bucket")

   from boto.s3.key import Key
   k = Key(bucket)
   k.key = file_name
   
   rc = 500

   try:
      k.delete()
      rc = 200
      log.debug("Deleted s3 file %s" % (file_name) )
   except Exception, e:
      log.error("Failed to delete %s" % file_name)
      log.exception(e)

   return rc

#-------------------------    
def usage():
    print 'Usage: {prog} [OPTIONS -w -r -d] <access_key_id> <secret_access_key> <file_name> <bucket_name>'.format(prog=sys.argv[0])
    return -1 

#-------------------------    
if __name__ == "__main__":

   if len(sys.argv) != 6:
      usage() 
   else:
      option = sys.argv[1]
      access_key_id = sys.argv[2]
      secret_access_key = sys.argv[3]
      file_name = sys.argv[4]
      bucket_name = sys.argv[5]
      
      import syndicate.rg.closure as rg_closure
      import syndicate.rg.common as rg_common
      
      config = {
         "BUCKET": bucket_name
      }
      
      secrets = {
         "AWS_ACCESS_KEY_ID": access_key_id,
         "AWS_SECRET_ACCESS_KEY": secret_access_key
      }
      
      log = rg_common.get_logger()
      
      context = rg_closure.make_context( config, secrets, None, log )

      if(option == '-w'):
         test_path = "/tmp/sd_s3_test.write"
         
         testfile_fd = open( test_path, "w")
         testfile_fd.write("This is a test of the S3 storage driver.  If you can read this, it works.")
         testfile_fd.close()
         
         testfile_fd = open( test_path, "r")
         write_file(file_name, testfile_fd, context=context)
         testfile_fd.close()
         
         os.unlink( test_path )
         
      elif(option == '-r'):
         
         test_path = "/tmp/sd_s3_test.read"
         
         testfile_fd = open(test_path, "w")
         read_file(file_name, testfile_fd, context=context)
         testfile_fd.close()
         
         print "wrote to %s" % test_path
         
      elif(option == '-d'):
         
         delete_file(file_name, context=context)
         
      else:
         usage()


