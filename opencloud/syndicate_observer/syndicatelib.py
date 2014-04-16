#!/usr/bin/python

"""
Define some common methods for the Syndicate observer.
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

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner


try:
    from util.logger import Logger, logging
    logger = Logger(level=logging.INFO)

except:
    # for testing
    import logging
    from logging import Logger
    logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
    logger = logging.getLogger()
    logger.setLevel( logging.INFO )

# get config package 
import syndicatelib_config.config as CONFIG

if hasattr( CONFIG, "SYNDICATE_PYTHONPATH" ):
    os.environ.setdefault("SYNDICATE_PYTHONPATH", CONFIG.SYNDICATE_PYTHONPATH)

# get core syndicate modules 
inserted = False
if os.getenv("SYNDICATE_PYTHONPATH") is not None:
    sys.path.insert(0, os.getenv("SYNDICATE_PYTHONPATH") )
    inserted = True
else:
    logger.warning("No SYNDICATE_PYTHONPATH set.  Assuming Syndicate is in your PYTHONPATH")

import syndicate 
syndicate = reload(syndicate)

import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig
import syndicate.util.storage as syndicate_storage
import syndicate.util.watchdog as syndicate_watchdog
import syndicate.util.daemonize as syndicate_daemon
import syndicate.syndicate as c_syndicate

# find the Syndicate models (also in a package called "syndicate")
if inserted:
    sys.path.pop(0)

if os.getenv("OPENCLOUD_PYTHONPATH") is not None:
    sys.path.insert(0, os.getenv("OPENCLOUD_PYTHONPATH"))
else:
    logger.warning("No OPENCLOUD_PYTHONPATH set.  Assuming Syndicate models are in your PYTHONPATH")

try:
   os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

   # get our models
   import syndicate 
   syndicate = reload(syndicate)

   import syndicate.models as models
   
except ImportError, ie:
   logger.warning("Failed to import models; assming testing environment")
   pass


#-------------------------------
def openid_url( email ):
    """
    Generate an OpenID identity URL from an email address.
    """
    return os.path.join( CONFIG.SYNDICATE_OPENID_TRUSTROOT, "id", email )


#-------------------------------
def registration_password():
    """
    Generate a random user registration password.
    """
    return "".join( random.sample("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 32) )


#-------------------------------
def exc_user_exists( exc ):
    """
    Given an exception, is it due to the user already existing?
    """
    return "already exists" in exc.message


#-------------------------------
def connect_syndicate():
    """
    Connect to the OpenCloud Syndicate SMI
    """
    client = syntool.Client( CONFIG.SYNDICATE_OPENCLOUD_USER, CONFIG.SYNDICATE_SMI_URL,
                             password=CONFIG.SYNDICATE_OPENCLOUD_PASSWORD,
                             debug=True )

    return client


#-------------------------------
def opencloud_caps_to_syndicate_caps( caps ):
    """
    Convert OpenCloud capability bits from the UI into Syndicate's capability bits.
    """
    OPENCLOUD_CAP_READ = 1
    OPENCLOUD_CAP_WRITE = 2
    OPENCLOUD_CAP_HOST = 4

    syn_caps = 0
    
    if (caps & models.Volume.CAP_READ_DATA) != 0:
        syn_caps |= (msconfig.GATEWAY_CAP_READ_DATA | msconfig.GATEWAY_CAP_READ_METADATA)
    if (caps & models.Volume.CAP_WRITE_DATA) != 0:
        syn_caps |= (msconfig.GATEWAY_CAP_WRITE_DATA | msconfig.GATEWAY_CAP_WRITE_METADATA)
    if (caps & models.Volume.CAP_HOST_DATA) != 0:
        syn_caps |= (msconfig.GATEWAY_CAP_COORDINATE)

    return syn_caps
    
    
#-------------------------------
def create_and_activate_user( client, user_email ):
    """
    Create, and then activate a Syndicate user account,
    given an OpenCloud user record.
    
    Return the newly-created user, if the user did not exist previously.
    Return None if the user already exists.
    Raise an exception on error.
    """

    user_id = user_email
    user_openid_url = openid_url( user_id )
    user_activate_pw = registration_password()
    try:
        # NOTE: allow for lots of UGs and RGs, since we're going to create at least one for each sliver
        new_user = client.create_user( user_id, user_openid_url, user_activate_pw, is_admin=False, max_UGs=1100, max_RGs=1100 )
    except Exception, e:
        # transport error, or the user already exists (rare, but possible)
        logger.exception(e)
        if not exc_user_exists( e ):
            # not because the user didn't exist already, but due to something more serious
            raise e
        else:
            return None     # user already existed

    else:
        # activate the user
        try:
            activate_rc = client.register_account( user_id, user_activate_pw )
        except Exception, e:
            # transport error, or the user diesn't exist (rare, but possible)
            logger.exception(e)
            raise e
            
        else:
            return new_user     # success!


#-------------------------------
def ensure_user_exists( user_email ):
    """
    Given an OpenCloud user, ensure that the corresponding
    Syndicate user exists.

    Return the new user if a user was created.
    Return None if the user already exists.
    Raise an exception on error.
    """
    
    client = connect_syndicate()
    
    try:
        user = client.read_user( user_email )    
    except Exception, e:
        # transport error
        logger.exception(e)
        raise e

    if user is None:
        # the user does not exist....try to create it
        user = create_and_activate_user( client, user_email )
        return user          # user exists now 
    
    else:
        return None         # user already exists


#-------------------------------
def ensure_user_absent( user_email ):
    """
    Ensure that a given OpenCloud user's associated Syndicate user record
    has been deleted.

    Returns True on success
    Raises an exception on error
    """

    client = connect_syndicate()

    return client.delete_user( user_email )
 

#-------------------------------
def ensure_volume_exists( user_email, opencloud_volume, user=None ):
    """
    Given the email address of a user, ensure that the given
    Volume exists and is owned by that user.
    Do not try to ensure that the user exists.

    Return the Volume if we created it, or return None if we did not.
    Raise an exception on error.
    """
    client = connect_syndicate()

    try:
        volume = client.read_volume( opencloud_volume.name )
    except Exception, e:
        # transport error 
        logger.exception(e)
        raise e

    if volume is None:
        # the volume does not exist....try to create it 
        vol_name = opencloud_volume.name
        vol_blocksize = opencloud_volume.blocksize
        vol_description = opencloud_volume.description
        vol_private = opencloud_volume.private
        vol_archive = opencloud_volume.archive 
        vol_default_gateway_caps = opencloud_caps_to_syndicate_caps( opencloud_volume.default_gateway_caps )

        try:
            vol_info = client.create_volume( user_email, vol_name, vol_description, vol_blocksize,
                                             private=vol_private,
                                             archive=vol_archive,
                                             active=True,
                                             default_gateway_caps=vol_default_gateway_caps,
                                             store_private_key=False,
                                             metadata_private_key="MAKE_METADATA_KEY" )

        except Exception, e:
            # transport error
            logger.exception(e)
            raise e

        else:
            # successfully created the volume!
            return vol_info

    else:
        # volume already exists.  Verify its owned by this user.

        if user is None:
           try:
               user = client.read_user( volume['owner_id'] )
           except Exception, e:
               # transport error, or user doesn't exist (either is unacceptable)
               logger.exception(e)
               raise e

        if user is None or user['email'] != user_email:
            raise Exception("Volume '%s' already exists, but is NOT owned by '%s'" % (opencloud_volume.name, user_email) )

        # we're good!
        return None



#-------------------------------
def ensure_volume_absent( volume_name ):
    """
    Given an OpenCloud volume, ensure that the corresponding Syndicate
    Volume does not exist.
    """

    client = connect_syndicate()

    # this is idempotent, and returns True even if the Volume doesn't exist
    return client.delete_volume( volume_name )
    
    
#-------------------------------
def update_volume( opencloud_volume ):
    """
    Update a Syndicate Volume, from an OpenCloud Volume model.
    Fails if the Volume does not exist in Syndicate.
    """

    client = connect_syndicate()

    vol_name = opencloud_volume.name
    vol_description = opencloud_volume.description
    vol_private = opencloud_volume.private
    vol_archive = opencloud_volume.archive
    vol_default_gateway_caps = opencloud_caps_to_syndicate_caps( opencloud_volume.default_gateway_caps )

    try:
        rc = client.update_volume( vol_name,
                                   description=vol_description,
                                   private=vol_private,
                                   archive=vol_archive,
                                   default_gateway_caps=vol_default_gateway_caps )

        if not rc:
            raise Exception("update_volume(%s) failed!" % vol_name )

    except Exception, e:
        # transort or method error 
        logger.exception(e)
        raise e

    else:
        return True


#-------------------------------
def ensure_volume_access_right_exists( user_email, volume_name, caps ):
    """
    Ensure that a particular user has particular access to a particular volume.
    Do not try to ensure that the user or volume exist, however!
    """
    client = connect_syndicate()
    
    try:
        rc = client.set_volume_access( user_email, volume_name, caps )
    except Exception, e:
        # transport error 
        logger.exception(e)
        raise e
    
    return True


#-------------------------------
def ensure_volume_access_right_absent( user_email, volume_name ):
    """
    Ensure that acess to a particular volume is revoked.
    """
    client = connect_syndicate()
    
    return client.remove_volume_access( user_email, volume_name )
    

#-------------------------------
def slice_user_email( slice_name ):
    """
    Generate a user email for a slice, i.e. in order to make the slice the principal that owns the Volume.
    """

    return slice_name + "@opencloud.us"


#-------------------------------
def sign_data( private_key_pem, data_str ):
   """
   Sign a string of data with a PEM-encoded private key.
   """
   try:
      key = CryptoKey.importKey( private_key_pem )
   except Exception, e:
      logging.error("importKey %s", traceback.format_exc() )
      return None
   
   h = HashAlg.new( data_str )
   signer = CryptoSigner.new(key)
   signature = signer.sign( h )
   return signature


#-------------------------------
def create_signed_blob( private_key_pem, data ):
    """
    Create a signed message to syndicated.
    """
    signature = sign_data( private_key_pem, data )
    if signature is None:
       logger.error("Failed to sign data")
       return None
    
    # create signed credential
    msg = {
       "data":  base64.b64encode( data ),
       "sig":   base64.b64encode( signature )
    }
    
    return json.dumps( msg )


def create_sealed_and_signed_blob( private_key_pem, shared_secret, data ):
    """
    Create a sealed and signed message.
    """
    
    # seal it with the password 
    logger.info("Sealing credential data")
    
    rc, sealed_data = c_syndicate.password_seal( data, shared_secret )
    if rc != 0:
       logger.error("Failed to seal data with the OpenCloud secret, rc = %s" % rc)
       return None
    
    msg = create_signed_blob( private_key_pem, sealed_data )
    if msg is None:
       logger.error("Failed to sign credential")
       return None 
    
    return msg 


#-------------------------------
def create_volume_list_blob( private_key_pem, shared_secret, volume_list ):
    """
    Create a sealed volume list, signed with the private key.
    """
    list_data = {
       "volumes": volume_list
    }
    
    list_data_str = json.dumps( list_data )
    
    msg = create_sealed_and_signed_blob( private_key_pem, shared_secret, list_data_str )
    if msg is None:
       logger.error("Failed to seal volume list")
       return None 
    
    return msg
 

#-------------------------------
def create_credential_blob( private_key_pem, shared_secret, syndicate_url, volume_name, volume_owner, volume_password, UG_port, RG_port ):
    """
    Create a sealed, signed, encoded credentials blob.
    """
    
    # create and serialize the data 
    cred_data = {
       "syndicate_url":   syndicate_url,
       "volume_name":     volume_name,
       "volume_owner":    volume_owner,
       "volume_password": volume_password,
       "volume_peer_port": UG_port,
       "volume_replicate_port": RG_port
    }
    
    cred_data_str = json.dumps( cred_data )
    
    msg = create_sealed_and_signed_blob( private_key_pem, shared_secret, cred_data_str )
    if msg is None:
       logger.error("Failed to seal volume list")
       return None 
    
    return msg 
    

#-------------------------------
def store_volumeslice_credentials( volumeslice, sealed_credentials_json ):
    """
    Store the sealed Volume credentials into the database,
    so the sliver-side Syndicate daemon syndicated.py can get them later
    (in case pushing them out fails).
    """
    volumeslice.credentials_blob = sealed_credentials_json
    volumeslice.save( updated_fields=['credentials_blob'] )


#-------------------------------
def get_volumeslice_names( slice_name ):
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
def get_volumeslice( volume_name, slice_name ):
    """
    Get a volumeslice record from the datastore.
    """
    try:
        vs = models.VolumeSlice.objects.get( volume_id__name = volume_name, slice_id__name = slice_name )
        return vs
    except Exception, e:
        logger.exception(e)
        logger.error("Failed to query for volume %s mounted in %s" % (volume_name, slice_name))
        return None
                                    
 
#-------------------------------
def do_push( sliver_hosts, payload ):
    """
    Push a payload to a list of slivers.
    NOTE: this has to be done in one go, since we can't import grequests
    into the global namespace (without wrecking havoc on the poll server),
    but it has to stick around for the push to work.
    """
    
    # make gevents runnabale from multiple threads (or Django will complain)
    from gevent import monkey
    #monkey.patch_all(socket=True, dns=True, time=True, select=True,thread=False, os=True, ssl=True, httplib=False, aggressive=True)
    monkey.patch_all()

    import grequests
    
    # fan-out 
    requests = []
    for sh in sliver_hosts:
      rs = grequests.post( sh, data={"observer_message": payload} )
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
          
    return 0
   
   
#-------------------------------
class ObserverServerHandler( BaseHTTPServer.BaseHTTPRequestHandler ):
   """
   HTTP server handler that allows syndicated.py instances to poll
   for volume state.
   
   NOTE: this is a fall-back mechanism.  The observer should push new 
   volume state to the slices' slivers.  However, if that fails, the 
   slivers are configured to poll for volume state periodically.  This 
   server allows them to do just that.
   
   Responses:
      GET /<slicename>              -- Reply with the signed sealed list of volume names
      GET /<slicename>/<volumename> -- Reply with the signed sealed volume access credentials
   
   
   NOTE: We want to limit who can learn which volumes a sliver can access.  It's not 
   feasible to give each sliver or each slice its own pre-shared authentication token,
   so our strategy  is to seal the volume list with the Syndicate-wide observer pre-shared secret.
   However, sealing the listing is a time-consuming process (on the order of 10s), so we only want 
   to do it when we have to.  Since *anyone* can ask for the ciphertext of the volume list,
   we will cache the list ciphertext for each slice for a long-ish amount of time, so we don't
   accidentally DDoS this server.  This necessarily means that the sliver might see a stale
   volume listing, but that's okay, since the Observer is eventually consistent anyway.
   """
   
   cached_volumes_json = {}             # map slice_name --> (volume name, timeout)
   cached_volumes_json_lock = threading.Lock()
   
   CACHED_VOLUMES_JSON_LIFETIME = 3600          # one hour
   
   def parse_request_path( self, path ):
      """
      Parse the URL path into a slice name and (possibly) a volume name.
      """
      path_parts = path.strip("/").split("/")
      
      if len(path_parts) == 0:
         # invalid 
         return (None, None)
      
      if len(path_parts) > 2:
         # invalid
         return (None, None)
      
      slice_name = path_parts[0]
      if len(slice_name) == 0:
         # empty string is invalid 
         return (None, None)
      
      volume_name = None
      
      if len(path_parts) > 1:
         volume_name = path_parts[1]
         
      return slice_name, volume_name
   
   
   def reply_data( self, data, datatype="application/json" ):
      """
      Give back a 200 response with data.
      """
      self.send_response( 200 )
      self.send_header( "Content-Type", datatype )
      self.send_header( "Content-Length", len(data) )
      self.end_headers()
      
      self.wfile.write( data )
      return 
   
   
   def get_volumes_message( self, private_key_pem, observer_secret, slice_name ):
      """
      Get the json-ized list of volumes this slice is attached to.
      Check the cache, evict stale data if necessary, and on miss, 
      regenerate the slice volume list.
      """
      self.cached_volumes_json_lock.acquire()
      
      ret = None
      volume_list_json, cache_timeout = self.cached_volumes_json.get( slice_name, (None, None) )
      
      if cache_timeout is not None and cache_timeout > time.time():
         # expired 
         volume_list_json = None
      
      if volume_list_json is None:
         # generate a new list and cache it.
         # BUT, don't starve other requests!
         self.cached_volumes_json_lock.release()
         
         volume_names = get_volumeslice_names( slice_name )
         if volume_names is None:
            # nothing to do...
            return None
         else:
            # seal and sign 
            ret = create_volume_list_blob( private_key_pem, observer_secret, volume_names )
         
         self.cached_volumes_json_lock.acquire()
         
         # cache this 
         cached_volumes_json[ slice_name ] = (ret, time.time() + self.CACHED_VOLUMES_JSON_LIFETIME )
      
      self.cached_volumes_json_lock.release()
      
      return ret
      
   
   def do_GET( self ):
      """
      Handle one GET
      """
      slice_name, volume_name = self.parse_request_path( self.path )
      
      # valid request?
      if volume_name is None and slice_name is None:
         self.send_error( 400 )
      
      # volume list request?
      elif volume_name is None and slice_name is not None:
         
         # get the list of volumes for this slice
         ret = self.get_volumes_message( self.server.private_key_pem, self.server.observer_secret, slice_name )
         
         if ret is not None:
            self.reply_data( ret )
            return
         else:
            self.send_error( 404 )
      
      # volume credential request?
      elif volume_name is not None and slice_name is not None:
         
         # get the VolumeSlice record
         vs = get_volumeslice( volume_name, slice_name )
         if vs is None:
            # not found
            self.send_error( 404 )
            return
         
         else:
            self.reply_data( vs.credentials_blob )
            return
         
      else:
         # shouldn't get here...
         self.send_error( 500 )
         return 
   
   
#-------------------------------
class ObserverServer( BaseHTTPServer.HTTPServer ):
   
   def __init__(self, private_key_pem, observer_secret, server, req_handler ):
      self.private_key_pem = private_key_pem
      self.observer_secret = observer_secret
      BaseHTTPServer.HTTPServer.__init__( self, server, req_handler )


#-------------------------------
def load_private_key( key_path ):
   try:
      key_text = syndicate_storage.read_file( key_path )
   except Exception, e:
      logger.error("Failed to read public key '%s'" % key_path )
      return None

   try:
      key = CryptoKey.importKey( key_text )
      assert key.has_private()
         
   except Exception, e:
      logger.error("Failed to load public key %s" % key_path )
      return None
   
   return key


#-------------------------------
def poll_server_spawn( old_exit_status ):
   """
   Start our poll server (i.e. in a separate process, started by the watchdog)
   """
   
   setproctitle.setproctitle( "syndicate-pollserver" )
      
   private_key = load_private_key( CONFIG.SYNDICATE_PRIVATE_KEY )
   if private_key is None:
      # exit code 255 will be ignored...
      logger.error("Cannot load private key.  Exiting...")
      sys.exit(255)
      
   srv = ObserverServer( private_key.exportKey(), CONFIG.SYNDICATE_OPENCLOUD_SECRET, ('', CONFIG.SYNDICATE_HTTP_PORT), ObserverServerHandler)
   srv.serve_forever()


#-------------------------------
def ensure_poll_server_running():
   """
   Instantiate our poll server and keep it running.
   """
   
   # is the watchdog running?
   pids = syndicate_watchdog.find_by_attrs( "syndicate-pollserver-watchdog", {} )
   if len(pids) > 0:
      # it's running
      return True
   
   # not running; spawn a new one 
   try:
      watchdog_pid = os.fork()
   except OSError, oe:
      logger.error("Failed to fork, errno = %s" % oe.errno)
      return False
   
   if watchdog_pid != 0:
      
      setproctitle.setproctitle( "syndicate-pollserver-watchdog" )
      
      # child--become a daemon and run the watchdog
      syndicate_daemon.daemonize( lambda: syndicate_watchdog.main( poll_server_spawn, respawn_exit_statuses=range(1,254) ) )


#-------------------------------
# Begin functional tests.
# Any method starting with ft_ is a functional test.
#-------------------------------

# for testing 
class FakeObject(object):
    def __init__(self):
        pass
     
#-------------------------------
def ft_syndicate_access():
    """
    Functional tests for ensuring objects exist and don't exist in Syndicate.
    """
    
    fake_user = FakeObject()
    fake_user.email = "fakeuser@opencloud.us"

    print "\nensure_user_exists(%s)\n" % fake_user.email
    ensure_user_exists( fake_user.email )

    print "\nensure_user_exists(%s)\n" % fake_user.email
    ensure_user_exists( fake_user.email )

    fake_volume = FakeObject()
    fake_volume.name = "fakevolume"
    fake_volume.description = "This is a fake volume, created for funtional testing"
    fake_volume.blocksize = 1024
    fake_volume.default_gateway_caps = 3
    fake_volume.archive = False
    fake_volume.private = True
    
    print "\nensure_volume_exists(%s)\n" % fake_volume.name
    ensure_volume_exists( fake_user.email, fake_volume )

    print "\nensure_volume_exists(%s)\n" % fake_volume.name
    ensure_volume_exists( fake_user.email, fake_volume )
    
    print "\nensure_volume_access_right_exists(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_exists( fake_user.email, fake_volume.name, 31 )
    
    print "\nensure_volume_access_right_exists(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_exists( fake_user.email, fake_volume.name, 31 )
    
    print "\nensure_volume_access_right_absent(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_absent( fake_user.email, fake_volume.name )
    
    print "\nensure_volume_access_right_absent(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_absent( fake_user.email, fake_volume.name )
 
    print "\nensure_volume_absent(%s)\n" % fake_volume.name
    ensure_volume_absent( fake_volume )

    print "\nensure_volume_absent(%s)\n" % fake_volume.name
    ensure_volume_absent( fake_volume )

    print "\nensure_user_absent(%s)\n" % fake_user.email
    ensure_user_absent( fake_user.email )

    print "\nensure_user_absent(%s)\n" % fake_user.email
    ensure_user_absent( fake_user.email )


#-------------------------------
def ft_volumeslice( slice_name ):
    """
    Functional tests for reading VolumeSlice information
    """
    print "slice: %s" % slice_name
    
    volumes = get_volumeslice_names( slice_name )
    
    print "volumes mounted in slice %s:" % slice_name
    for v in volumes:
       print "   %s:" % v
      
       vs = get_volumeslice( v, slice_name )
       
       print "      %s" % dir(vs)
          

#-------------------------------
def ft_pollserver():
   """
   Functional test for the pollserver
   """
   ensure_poll_server_running()


# run functional tests
if __name__ == "__main__":
    sys.path.append("/opt/planetstack")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

    if len(sys.argv) < 2:
      print "Usage: %s testname [args]" % sys.argv[0]
      
    # call a method starting with ft_, and then pass the rest of argv as its arguments
    testname = sys.argv[1]
    ft_testname = "ft_%s" % testname
    
    try:
       test_call = "%s(%s)" % (ft_testname, ",".join(sys.argv[2:]))
       
       print "calling %s" % test_call
       
       rc = eval( test_call )
       
       print "result = %s" % rc
       
    except NameError, ne:
       raise ne
    
