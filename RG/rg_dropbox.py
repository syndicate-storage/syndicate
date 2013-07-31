#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved
 
import sys

# Include the Dropbox SDK
import dropbox

from rg_common import DEBUG, FILE_ROOT, CONFIG_PATH

from rg_common import get_logger
log = get_logger()

RG_STORE = "Dropbox"

# Get your app key and secret from the Dropbox developer website

from etc.config import DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_ACCESS_TOKEN 

APP_KEY = DROPBOX_APP_KEY
APP_SECRET = DROPBOX_APP_SECRET
ACCESS_TOKEN = DROPBOX_ACCESS_TOKEN

#if ACCESS_TOKEN has been initialized 
if(ACCESS_TOKEN != 'XXX'):
    client = dropbox.client.DropboxClient(ACCESS_TOKEN)
    if(DEBUG): log.debug(u'Using account: %s', client.account_info())

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

    print 'please save the ACCESS_TOKEN: ' + access_token 
    
    return access_token

#-------------------------
def get_bucket(bucket_name):

	try:
		conn = boto.connect_s3(aws_id, aws_key)
	except:
		if(DEBUG): log.debug(u'ERROR: Connection to %s failed', RG_STORE)
	else:
		if(DEBUG): log.debug(u'ERROR: Connected to %s', RG_STORE)

	try:
		folder_metadata = client.metadata(bucket_name)

	except:
		if(DEBUG): log.debug(u'ERROR: Could not create/fetch bucket: %s', bucket_name)
        return False

	if(DEBUG): log.debug(u'Fetched/created bucket: %s', folder_metadata)

	return True
 
#-------------------------
def write_file(file_name, bucket_name):
    
    if(DEBUG): log.debug(u'Writing file: %s', file_name)

    try:

        f = open(bucket_name + file_name)
        response = client.put_file(bucket_name + file_name, f)

    except Exception as e:
        print e
        return False
    
    if(DEBUG): log.debug(u'Written file to %s', RG_STORE)

    return True

#-------------------------
def read_file(file_name, bucket_name):
    
    if(DEBUG): log.debug(u'Reading file: %s', file_name)

    try:
        f, metadata = client.get_file_and_metadata(bucket_name + file_name)

        out = open(bucket_name + file_name, 'w')
        out.write(f.read())
        out.close()

    except:
    	if(DEBUG):
    		print "ERROR: reading file"
    	return False
	
	if(DEBUG): log.debug(u'Read data from %s: %s', RG_STORE, metadata)
    
    return True
   
#-------------------------
def delete_file(file_name, bucket_name):
    
    if(DEBUG): log.debug(u'Deleting file: %s', file_name)
    
    bucket = get_bucket(bucket_name)

    from boto.s3.key import Key
    k = Key(bucket)
    k.key = file_name

    try:
    	k.delete()
    except:
    	return False
    
    if(DEBUG): log.debug(u'Deleted %s file: %s', RG_STORE, file_name)

    return True

#-------------------------    
def usage():
    print 'Usage: {prog} [OPTIONS -w -r -d --access] <file_name> <bucket_name>'.format(prog=sys.argv[0])
    return -1 

#-------------------------    
if __name__ == "__main__":

    if(len(sys.argv) < 2):
        usage() 
    else:
    	option = sys.argv[1]

        if(option == '--access'):
            get_access_token()
        elif(len(sys.argv) != 4):
            usage() 
        else:

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
