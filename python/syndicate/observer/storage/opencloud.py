#!/usr/bin/python

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
import sys
import random
import json
import time
import requests
import traceback
import base64
import BaseHTTPServer
import setproctitle
import threading
import urllib
import binascii

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

import syndicate.ms.msconfig as msconfig
import syndicate.ms.api as api
import syndicate.util.storage as syndicate_storage_api
import syndicate.util.watchdog as syndicate_watchdog
import syndicate.util.daemonize as syndicate_daemon
import syndicate.util.crypto as syndicate_crypto
import syndicate.util.provisioning as syndicate_provisioning
import syndicate.syndicate as c_syndicate

# for testing 
TESTING = False
class FakeObject(object):
   def __init__(self):
       pass

if os.getenv("OPENCLOUD_PYTHONPATH") is not None:
   sys.path.insert(0, os.getenv("OPENCLOUD_PYTHONPATH"))
else:
   logger.warning("No OPENCLOUD_PYTHONPATH set.  Assuming syndicate_storage models are in your PYTHONPATH")

try:
   os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

   # get our models (syndicate_storage)
   import syndicate_storage.models as models
   
   # get OpenCloud models 
   from core.models import Slice,Sliver
   
   from django.core.exceptions import ObjectDoesNotExist
   from django.db import IntegrityError

except ImportError, ie:
   logger.warning("Failed to import models; some tests will not work")

   # create a fake "models" package that has exactly the members we need for testing.
   models = FakeObject()
   models.Volume = FakeObject()
   models.Volume.CAP_READ_DATA = 1
   models.Volume.CAP_WRITE_DATA = 2
   models.Volume.CAP_HOST_DATA = 4
   
   TESTING = True
   
   
#-------------------------------
def put_principal_data( principal_id, public_key_pem, sealed_private_key ):
    """
    Seal and store the principal's private key into the database, in a SyndicatePrincipal object,
    so the sliver-side Syndicate daemon syndicated.py can get them later.
    Overwrite an existing principal if one exists.
    """
    
    try:
       sp = models.SyndicatePrincipal( sealed_private_key=sealed_private_key, public_key_pem=public_key_pem, principal_id=principal_id )
       sp.save()
    except IntegrityError, e:
       logger.error("WARN: overwriting existing principal %s" % principal_id)
       sp.delete()
       sp.save()
    
    return True


#-------------------------------
def delete_principal_data( principal_id ):
    """
    Delete an OpenCloud SyndicatePrincipal object.
    """
    
    sp = get_principal_data( principal_id )
    if sp is not None:
      sp.delete()
    
    return True


#-------------------------------
def get_principal_data( principal_id ):
    """
    Get a SyndicatePrincipal record from the database 
    """
    
    try:
        sp = models.SyndicatePrincipal.objects.get( principal_id=principal_id )
        return sp
    except ObjectDoesNotExist:
        logger.error("No SyndicatePrincipal record for %s" % principal_id)
        return None


#--------------------------------
def get_slice_secret( observer_pkey_pem, slice_name, slice_fk=None ):
    """
    Get the shared secret for a slice.
    """
    
    ss = None 
    
    # get the sealed slice secret from Django
    try:
       if slice_fk is not None:
          ss = models.SliceSecret.objects.get( slice_id=slice_fk )
       else:
          ss = models.SliceSecret.objects.get( slice_id__name=slice_name )
    except ObjectDoesNotExist, e:
       logger.error("Failed to load slice secret for (%s, %s)" % (slice_fk, slice_name) )
       return None 

    return ss.secret 
 

#-------------------------------
def put_slice_secret( observer_pkey_pem, slice_name, slice_secret, slice_fk=None, opencloud_slice=None ):
    """
    Put the shared secret for a slice, encrypting it first.
    """
    
    ss = None 
    
    if opencloud_slice is None:
       # look up the slice 
       try:
          if slice_fk is None:
             opencloud_slice = models.Slice.objects.get( name=slice_name )
          else:
             opencloud_slice = models.Slice.objects.get( id=slice_fk.id )
       except Exception, e:
          logger.exception(e)
          logger.error("Failed to load slice (%s, %s)" % (slice_fk, slice_name) )
          return False 
    
    ss = models.SliceSecret( slice_id=opencloud_slice, secret=slice_secret )
    
    ss.save()
    
    return True
 
 
#-------------------------------
def put_volumeslice_creds( volume_name, slice_name, creds ):
    """
    Save slice credentials.
    """
    vs = get_volumeslice( volume_name, slice_name )
    ret = False
    
    if vs is not None:
       vs.credentials_blob = creds
       vs.save()
      
       # success!
       ret = True
    else:
       logger.error("Failed to look up VolumeSlice(%s, %s)" % (volume_name, slice_name))
    
    return ret 
 
 
#-------------------------------
def get_volumeslice_volume_names( slice_name ):
    """
    Get the list of Volume names from the datastore.
    """
    try:
        all_vs = models.VolumeSlice.objects.filter( slice_id__name = slice_name )
        volume_names = []
        for vs in all_vs:
           volume_names.append( vs.volume_id.name )
           
        return volume_names
    except Exception, e:
        logger.exception(e)
        logger.error("Failed to query datastore for volumes mounted in %s" % slice_name)
        return None 
 

#-------------------------------
def get_volumeslice( volume_name, slice_name ):
    """
    Get a volumeslice record from the datastore.
    """
    try:
        vs = models.VolumeSlice.objects.get( volume_id__name = volume_name, slice_id__name = slice_name )
        return vs
    except Exception, e:
        logger.exception(e)
        logger.error("Failed to query datastore for volumes (mounted in %s)" % (slice_name if (slice_name is not None or len(slice_name) > 0) else "UNKNOWN"))
        return None 


#-------------------------------
def get_slice_hostnames( slice_name ):
   """
   Query the Django DB and get the list of hosts running in a slice.
   """

   openstack_slice = Slice.objects.get( name=slice_name )
   if openstack_slice is None:
       logger.error("No such slice '%s'" % slice_name)
       return None

   hostnames = [s.node.name for s in openstack_slice.slivers.all()]

   return hostnames


#-------------------------------
# Begin functional tests.
# Any method starting with ft_ is a functional test.
#-------------------------------
  

#-------------------------------
def ft_volumeslice( slice_name ):
    """
    Functional tests for reading VolumeSlice information
    """
    print "slice: %s" % slice_name
    
    volumes = get_volumeslice_volume_names( slice_name )
    
    print "volumes mounted in slice %s:" % slice_name
    for v in volumes:
       print "   %s:" % v
      
       vs = get_volumeslice( v, slice_name )
       
       print "      %s" % dir(vs)
          

#-------------------------------
def ft_get_slice_hostnames( slice_name ):
   """
   Functional tests for getting slice hostnames
   """
   
   print "Get slice hostnames for %s" % slice_name
   
   hostnames = get_slice_hostnames( slice_name )
   import pprint 
   
   pp = pprint.PrettyPrinter()
   
   pp.pprint( hostnames )

    

# run functional tests
if __name__ == "__main__":
    sys.path.append("/opt/planetstack")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

    argv = sys.argv[:]
    
    if len(argv) < 2:
      print "Usage: %s testname [args]" % argv[0]
    
    # call a method starting with ft_, and then pass the rest of argv as its arguments
    testname = argv[1]
    ft_testname = "ft_%s" % testname
    
    test_call = "%s(%s)" % (ft_testname, ",".join(argv[2:]))
   
    print "calling %s" % test_call
   
    rc = eval( test_call )
   
    print "result = %s" % rc
    