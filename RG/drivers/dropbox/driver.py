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


from rm_common import *

# Include the Dropbox SDK
import dropbox

RG_STORE = "Dropbox"

# Get your app key and secret from the Dropbox developer website

from etc.config import DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_ACCESS_TOKEN 

APP_KEY = DROPBOX_APP_KEY
APP_SECRET = DROPBOX_APP_SECRET
ACCESS_TOKEN = DROPBOX_ACCESS_TOKEN

#if ACCESS_TOKEN has been initialized 
if(ACCESS_TOKEN != 'XXX'):
	client = dropbox.client.DropboxClient(ACCESS_TOKEN)
	#if(DEBUG): log.debug(u'Using account: %s', client.account_info())

#----------------------------
def get_access_token(app_key=APP_KEY,app_secret=APP_SECRET):

	'''
		Get a new access token for a client 
	'''

	flow = dropbox.client.DropboxOAuth2FlowNoRedirect(app_key, app_secret)

	# Have the user sign in and authorize this tokenexit
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
def write_file(file_name):
	
	if(DEBUG): log.debug(u'Writing file: %s', file_name)

	try:

		f = open(file_name)
		#by default dropbox doesn't overwrite a previous file
		response = client.put_file(file_name, f, overwrite=True)

	except Exception as e:
		print e
		return False
	
	if(DEBUG): log.debug(u'Written file to %s', RG_STORE)

	return True

#-------------------------
def read_file(file_name):
	
	if(DEBUG): log.debug(u'Reading file: %s', file_name)

	try:
		f, metadata = client.get_file_and_metadata(file_name)

		out = open(file_name, 'w')
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
def run_server(port):

	import socket 
 
	port = int(port)

	host = '' 
	backlog = 5 
	size = 1024

	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	#set SO_REUSEADDR to reuse the PORT of a recently closed socket
	server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	server.bind((host,port)) 
	server.listen(backlog) 

	while 1:
		client, address = server.accept() 
		data = client.recv(size) 
		
		if data: 

			request = HTTPRequest(data)

			print '-' * 10
			print request.error_code       # None  (check this first)
			print request.command          # "GET"
			print request.path             # "/who/ken/trust.html"
			print request.request_version  # "HTTP/1.1"
			print len(request.headers)     # 3
			print request.headers.keys()   # ['accept-charset', 'host', 'accept']
			print request.headers['host'] 
			print request.post_body
			print '-' * 10

			if(request.command == "STATUS"):
				client.send("OK")

			if(request.command == "POST"):
				print request.path
				client.send(request.post_body)
					
			client.close()

	server.close() 

#-------------------------    
def process_input(option,file_name):

	if(option == '-w'):
		write_file(file_name)
	elif(option == '-r'):
		read_file(file_name)
	elif(option == '-d'):
		delete_file(file_name)
	else:
		raise UsageException(100)

#-------------------------    
if __name__ == "__main__":

	try:
		if(len(sys.argv) < 2): raise UsageException(100)

		option = sys.argv[1]

		if(option == '--get_key'):
			get_access_token()
			
		elif(option == '--run_server'):
			if(len(sys.argv) != 3): raise UsageException(100)

			port = sys.argv[2]
			run_server(port)
		else:
			if(len(sys.argv) != 3): raise UsageException(100)

			file_name = sys.argv[2]

			process_input(option,file_name)

	except UsageException as e:
		print e 

	except Exception as e:
		print_exception(log)
		
