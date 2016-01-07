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

DEBUG = True

FILE_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(FILE_ROOT, "config/")

import shelve
import boto.glacier
from boto.glacier.exceptions import UnexpectedHTTPResponseError
 
from etc.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

SHELVE_FILE = os.path.expanduser(FILE_ROOT + '/data/' + "glaciervault.db")
 
#----------------------------------
class glacier_shelve(object):
    """
    Context manager for shelve
    """
 
    def __enter__(self):
        self.shelve = shelve.open(SHELVE_FILE)
 
        return self.shelve
 
    def __exit__(self, exc_type, exc_value, traceback):
        self.shelve.close()
 
 #----------------------------------
class GlacierVault:
    """
    Wrapper for uploading/download archive to/from Amazon Glacier Vault
    Makes use of shelve to store archive id corresponding to filename and waiting jobs.
 
    Backup:
    >>> GlacierVault("myvault")upload("myfile")
    
    Restore:
    >>> GlacierVault("myvault")retrieve("myfile")
 
    or to wait until the job is ready:
    >>> GlacierVault("myvault")retrieve("serverhealth2.py", True) XXX not working right now
    """
    def __init__(self, vault_name):
        """
        Initialize the vault
        """
        layer2 = boto.connect_glacier(aws_access_key_id = AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
 
        self.vault = layer2.get_vault(vault_name)
 
 
    def upload(self, filename):
        """
        Upload filename and store the archive id for future retrieval
        """
        archive_id = self.vault.create_archive_from_file(filename, description=filename)
 
        # Storing the filename => archive_id data.
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()
 
            archives = d["archives"]
            archives[filename] = archive_id
            d["archives"] = archives
 
    def get_archive_id(self, filename):
        """
        Get the archive_id corresponding to the filename
        """
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()
 
            archives = d["archives"]
 
            if filename in archives:
                return archives[filename]
 
        return None
 
    def retrieve(self, filename, wait_mode=False):
        """
        Initiate a Job, check its status, and download the archive when it's completed.
        """
        archive_id = self.get_archive_id(filename)
        if not archive_id:
            if(DEBUG):
                print "ERROR: didn't find archive_id"
            return
        
        with glacier_shelve() as d:
            if not d.has_key("jobs"):
                d["jobs"] = dict()
 
            jobs = d["jobs"]
            job = None
 
            if filename in jobs:
                # The job is already in shelve
                job_id = jobs[filename]
                try:
                    job = self.vault.get_job(job_id)
                except UnexpectedHTTPResponseError: # Return a 404 if the job is no more available
                    pass
 
            if not job:
                # Job initialization
                job = self.vault.retrieve_archive(archive_id)
                jobs[filename] = job.id
                job_id = job.id
 
            # Commiting changes in shelve
            d["jobs"] = jobs
 
        print "Job {action}: {status_code} ({creation_date}/{completion_date})".format(**job.__dict__)
 
        # checking manually if job is completed every 10 secondes instead of using Amazon SNS
        if wait_mode:
            import time
            while 1:
                job = self.vault.get_job(job_id)
                if not job.completed:
                    time.sleep(10)
                else:
                    break
 
        if job.completed:
            print "Downloading..."
            job.download_to_file(filename)
        else:
            print "Not completed yet"

def write_file(file_name,vault_name):

    vault = GlacierVault(vault_name)
    vault.upload(FILE_ROOT + '/data/out/' + file_name)

def read_file(file_name,vault_name):

    if(DEBUG):
        print "Fetching file: " + vault_name + "/" + file_name

    vault = GlacierVault(vault_name)
    vault.retrieve(file_name,wait_mode=True)
 
#-------------------------    
def usage():
    print 'Usage: {prog} [OPTIONS -w -r -d] <file_name> <vault_name>'.format(prog=sys.argv[0])
    return -1 

#-------------------------    
if __name__ == "__main__":
  
    if len(sys.argv) != 4:
        usage() 
    else:
    	option = sys.argv[1]
    	file_name = sys.argv[2]
    	vault_name = sys.argv[3]

    	if(option == '-w'):
        	write_file(file_name,vault_name)
        elif(option == '-r'):
        	read_file(file_name,vault_name)
        elif(option == '-d'):
        	delete_file(file_name,vault_name)
        else:
        	usage()


