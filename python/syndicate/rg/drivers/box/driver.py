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

RG_STORE = "Box"

#XXX -- Work in Progress
APP_KEY=None
APP_SECRET=None

#----------------------------
def get_access_token(app_key=APP_KEY,app_secret=APP_SECRET):

    '''
        Get a new access token for a client 
    '''

    flow = dropbox.client.DropboxOAuth2FlowNoRedirect(app_key, app_secret)

    # Have the user sign in and authorize this token
    authorize_url = flow.start()
    print '1. Go to: ' + authorize_url
    print '2. Click "Allow" (you might have to log in first)'
    print '3. Copy the authorization code.'
    code = raw_input("Enter the authorization code here: ").strip()

    # This will fail if the user enters an invalid authorization code
    access_token, user_id = flow.finish(code)

    return access_token

#----------------------------
def get_bucket(bucket_name):

   from etc.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

   aws_id = AWS_ACCESS_KEY_ID
   aws_key = AWS_SECRET_ACCESS_KEY

   #from boto.s3.connection import Location

   try:
        client = dropbox.client.DropboxClient(ACCESS_TOKEN)

        if(DEBUG): print 'Using account: ', client.account_info()

   except:
      if(DEBUG): print "ERROR: Connection to Dropbox failed"
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

    try:

        f = open(bucket_name + file_name)
        response = client.put_file(bucket_name + file_name, f)

    except Exception as e:
        print e
        return False
    
    if(DEBUG):
       print "Written file to dropbox: " + file_name

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

