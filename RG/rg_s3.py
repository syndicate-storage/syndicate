#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved
 
import sys
import os
import boto

DEBUG = True

FILE_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(FILE_ROOT, "config/")

def get_bucket(bucket_name):

	from etc.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

	aws_id = AWS_ACCESS_KEY_ID
	aws_key = AWS_SECRET_ACCESS_KEY

	#from boto.s3.connection import Location

	try:
		conn = boto.connect_s3(aws_id, aws_key)
	except:
		if(DEBUG):
			print "ERROR: Connection to S3 failed"
	else:
		if(DEBUG):
			print "Connected to S3"

	try:
		bucket = conn.create_bucket(bucket_name)
	except:
		if(DEBUG):
			print "ERROR: Could not create/fetch bucket " + bucket_name
	else:
		if(DEBUG):
			print "Fetched/created bucket: " + bucket_name

	return bucket
 
#-------------------------
def write_file(file_name, bucket_name):
    
    if(DEBUG):
    	print "Writing File: " + file_name

    bucket = get_bucket(bucket_name)

    from boto.s3.key import Key
    k = Key(bucket)
    k.key = file_name

    file_name_with_path = FILE_ROOT + '/data/out/' + file_name

    try:
    	k.set_contents_from_filename(file_name_with_path)
    except:
    	return False
    
    if(DEBUG):
    	print "Written file to s3: " + file_name_with_path

    return True

#-------------------------
def read_file(file_name, bucket_name):
    
    if(DEBUG):
    	print "Reading File: " + file_name
 
    bucket = get_bucket(bucket_name)

    from boto.s3.key import Key
    k = Key(bucket)
    k.key = file_name

    file_name_with_path = FILE_ROOT + '/data/in/' + file_name

    try:
    	k.get_contents_to_filename(file_name_with_path)
    except:
    	if(DEBUG):
    		print "ERROR: reading"
    	return False
	
	if(DEBUG):
		print "Read data from s3 to file: " + file_name_with_path
    
    return True
   
#-------------------------
def delete_file(file_name, bucket_name):
    
    if(DEBUG):
    	print "Deleting File: " + file_name
    
    bucket = get_bucket(bucket_name)

    from boto.s3.key import Key
    k = Key(bucket)
    k.key = file_name

    try:
    	k.delete()
    except:
    	return False
    
    if(DEBUG):
    	print "Deleted s3 file: " + bucket_name + '/' + file_name

    return True

#-------------------------    
def usage():
    print 'Usage: {prog} [OPTIONS -w -r -d] <file_name> <bucket_name>'.format(prog=sys.argv[0])
    return -1 

#-------------------------    
if __name__ == "__main__":
  
    if len(sys.argv) != 4:
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
        	delete_file(file_name,bucket_name)
        else:
        	usage()


