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
import json
import time
import traceback
import base64
import binascii
from collections import namedtuple

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

import syndicate.util.storage as syndicate_storage_api 

import syndicate.observer.core as observer_core
import syndicate.observer.cred as observer_cred
import syndicate.syndicate as c_syndicate

CONFIG = observer_core.get_config()
observer_storage = observer_core.get_observer_storage()

TESTING = False 

#-------------------------------
def do_push( sliver_hosts, portnum, payload ):
    """
    Push a payload to a list of slivers.
    NOTE: this has to be done in one go, since we can't import grequests
    into the global namespace (without wrecking havoc on the credential server),
    but it has to stick around for the push to work.
    """
    
    global TESTING, CONFIG
    
    from gevent import monkey
    
    if TESTING:
       monkey.patch_all()
    
    else:
       # make gevents runnabale from multiple threads (or Django will complain)
       monkey.patch_all(socket=True, dns=True, time=True, select=True, thread=False, os=True, ssl=True, httplib=False, aggressive=True)
    
    import grequests
    
    # fan-out 
    requests = []
    for sh in sliver_hosts:
      data = {observer_cred.OPENCLOUD_JSON: payload, observer_cred.OPENCLOUD_SLIVER_HOSTNAME: sh}
      
      # TODO: https, using the sliver's public key, since we're pushing over the hostname
      rs = grequests.post( "http://" + sh + ":" + str(portnum), data=data, timeout=getattr(CONFIG, "SYNDICATE_HTTP_PUSH_TIMEOUT", 60) )
      requests.append( rs )
      
    # fan-in
    responses = grequests.map( requests )
    
    assert len(responses) == len(requests), "grequests error: len(responses) != len(requests)"
    
    for i in xrange(0,len(requests)):
       resp = responses[i]
       req = requests[i]
       
       if resp is None:
          logger.error("Failed to connect to %s" % (req.url))
          continue 
       
       # verify they all worked 
       if resp.status_code != 200:
          logger.error("Failed to POST to %s, status code = %s" % (resp.url, resp.status_code))
          continue
          
    return True

   
#-------------------------------
def push_credentials_to_slice( slice_name, payload ):
   """
   Push a credentials payload to the VMs in a slice.
   """
   hostnames = observer_storage.get_slice_hostnames( slice_name )
   return do_push( hostnames, CONFIG.SYNDICATE_SLIVER_PORT, payload )


def ft_do_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port,
                instantiate_UG=None, run_UG=None, UG_port=0, UG_closure=None,
                instantiate_RG=None, run_RG=None, RG_port=0, RG_closure=None, RG_global_hostname=None,
                instantiate_AG=None, run_AG=None, AG_port=0, AG_closure=None, AG_global_hostname=None,
                gateway_name_prefix="" ):
   
    """
    Push credentials to a single host.
    """
    
    c_syndicate.crypto_init()
    
    observer_key = syndicate_storage_api.read_private_key( CONFIG.SYNDICATE_OBSERVER_PRIVATE_KEY )
    user_key = syndicate_storage_api.read_private_key( principal_pkey_path )
    
    observer_key_pem = observer_key.exportKey()
    user_pkey_pem = user_key.exportKey()
    
    if observer_key_pem is None:
       raise Exception("Failed to read observer private key from %s" % observer_key_pem )
    
    if user_pkey_pem is None:
       raise Exception("Failed to read user private key from %s" % principal_pkey_path )
    
    # convert to binary
    slice_secret = binascii.unhexlify( slice_secret )
    
    cred = observer_cred.create_slice_credential_blob( observer_key_pem, slice_name, slice_secret, syndicate_url, volume_name, volume_owner, user_pkey_pem,
                                                       instantiate_UG=instantiate_UG, run_UG=run_UG, UG_port=UG_port, UG_closure=UG_closure,
                                                       instantiate_RG=instantiate_RG, run_RG=run_RG, RG_port=RG_port, RG_closure=RG_closure, RG_global_hostname=RG_global_hostname,
                                                       instantiate_AG=instantiate_AG, run_AG=run_AG, AG_port=AG_port, AG_closure=AG_closure, AG_global_hostname=AG_global_hostname,
                                                       gateway_name_prefix=gateway_name_prefix )
    
    if cred is None:
       raise Exception("Failed to generate slice credential")
    
    rc = do_push( [hostname], automount_daemon_port, cred )
    
    c_syndicate.crypto_shutdown()
    
                

#-------------------------------
def ft_do_nothing_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port ):
    """
    Push credentials to a single host.
    """
    
    return ft_do_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port, gateway_name_prefix="OpenCloud" )


#-------------------------------
def ft_do_create_UG_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port, UG_port ):
    """
    Push credentials to a single host.
    """
    
    return ft_do_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port, gateway_name_prefix="OpenCloud",
                       instantiate_UG=True, run_UG=True, UG_port=UG_port, UG_closure=None )


#-------------------------------
def ft_do_start_UG_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port ):
    """
    Push credentials to a single host.
    """
    
    return ft_do_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port, gateway_name_prefix="OpenCloud",
                       instantiate_UG=None, run_UG=True, UG_port=0, UG_closure=None )


#-------------------------------
def ft_do_stop_UG_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port ):
    """
    Push credentials to a single host.
    """
    
    return ft_do_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port, gateway_name_prefix="OpenCloud",
                       instantiate_UG=None, run_UG=False, UG_port=0, UG_closure=None )
 
#-------------------------------
def ft_do_delete_UG_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port ):
    """
    Push credentials to a single host.
    """
    
    return ft_do_push( syndicate_url, volume_name, volume_owner, slice_name, slice_secret, principal_pkey_path, hostname, automount_daemon_port, gateway_name_prefix="OpenCloud",
                       instantiate_UG=False, run_UG=False, UG_port=0, UG_closure=None )


# run functional tests
if __name__ == "__main__":
    argv = sys.argv[:]
    
    if len(argv) < 2:
      print "Usage: %s testname [args]" % argv[0]
    
    TESTING = True 
    
    # call a method starting with ft_, and then pass the rest of argv as its arguments
    testname = argv[1]
    ft_testname = "ft_%s" % testname
    
    test_call = "%s(%s)" % (ft_testname, ",".join(argv[2:]))
   
    print "calling %s" % test_call
   
    rc = eval( test_call )
   
    print "result = %s" % rc
    
    