#!/usr/bin/env python

"""
   Copyright 2016 The Trustees of Princeton University

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
import subprocess
import hashlib 
import base64
import shutil
import binascii
import json 

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import syndicate.util.objects as object_stub
import syndicate.util.crypto as crypto
import client
import config as conf
import syndicate.protobufs.ms_pb2 as ms_pb2
import syndicate.protobufs.sg_pb2 as sg_pb2

log = conf.log

def syndicate_public_key_name( ms_url ):
    """
    Get the name of a syndicate public key, given
    the MS url it was fetched from.
    """
    host, port, no_tls = client.parse_url( ms_url )
    return host + ":" + str(port)
    

def syndicate_public_key_fetch( ms_url, downloader_path ):
   """
   Use a helper program to go and fetch the Syndicate public key.
   Return the key itself on success.
   Return None on error
   """

   if not os.path.exists( downloader_path ):
      log.error("'%s' does not exist" % downloader_path )
      return None 
  
   downloader = subprocess.Popen( [downloader_path, ms_url], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
   pubkey_out, pubkey_err = downloader.communicate()
   downloader.wait()
   
   if len(pubkey_err.strip()) != 0:
       log.error("Syndicate public key downloader errors:\n%s\n" % pubkey_err)

   pubkey_pem = pubkey_out.strip()
   
   # validate 
   try:
      pubkey = CryptoKey.importKey( pubkey_pem )
   except Exception, e:
      log.error("Invalid Syndicate public key (from %s)" % ms_url)
      return None
   
   return pubkey


def get_syndicate_public_key( config ):
    """
    Load up the syndicate public key.
    If it is not local, then go fetch it.
    Return the key on success
    Raise an exception on error
    """

    ms_url = config['MS_url']
    pubkey_name = syndicate_public_key_name( ms_url )
    pubkey = storage.load_public_key( config, "syndicate", pubkey_name )

    if pubkey is None:
        downloader = config['helpers']['fetch_syndicate_pubkey']
        pubkey = syndicate_public_key_fetch( ms_url, downloader )
        
        if pubkey is None:
            raise Exception("Failed to obtain syndicate public key")

    return pubkey


def gateway_cert_fetch( ms_url, gateway_name_or_id, downloader_path ):
    """
    Use a helper program to go and fetch a gateway certificate.
    Return the cert on success.
    Return None on error.
    """
    if not os.path.exists( downloader_path ):
       log.error("'%s' does not exist" % downloader_path )
       return None 

    downloader = subprocess.Popen( [downloader_path, ms_url, str(gateway_name_or_id)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    cert_out, cert_err = downloader.communicate()
    downloader.wait()
   
    if len(cert_err.strip()) != 0:
       log.error("Gateway cert downloader errors:\n%s" % cert_err)

    try:
        gateway_cert = ms_pb2.ms_gateway_cert()
        gateway_cert.ParseFromString( cert_out )
    except Exception, e:
        log.error("Invalid gateway certificate for %s (from %s)" % (str(gateway_name_or_id), ms_url))
        return None 

    return gateway_cert


def get_gateway_cert( config, gateway_name_or_id, check_cache=True ):
    """
    Load a gateway certificate from disk.
    If it is not local, then go fetch it.
    Return the cert on success
    Raise an exception on error.
    """

    gateway_cert = None
    if check_cache:
        gateway_cert = object_stub.load_gateway_cert( config, gateway_name_or_id )

    if gateway_cert is None:

        downloader = config['helpers']['fetch_gateway_cert']
        ms_url = config['MS_url']
        gateway_cert = gateway_cert_fetch( ms_url, gateway_name_or_id, downloader )

        if gateway_cert is None:
            raise Exception("Failed to obtain gateway certificate")

    return gateway_cert


def volume_cert_fetch( ms_url, volume_name_or_id, downloader_path ):
    """
    Use a helper program to go and fetch a volume certificate.
    Return the cert on success.
    Return None on error.
    """

    if not os.path.exists( downloader_path ):
        log.error("'%s' does not exist" % downloader_path )
        return None 

    downloader = subprocess.Popen( [downloader_path, ms_url, str(volume_name_or_id)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    cert_out, cert_err = downloader.communicate()
    downloader.wait()

    if len(cert_err.strip()) != 0:
       log.error("Volume cert downloader errors:\n%s" % cert_err)

    try:
        volume_cert = ms_pb2.ms_volume_metadata()
        volume_cert.ParseFromString( cert_out )

        if volume_cert.HasField("root"):
            volume_cert.ClearField("root")

    except Exception, e:
        log.exception(e)
        log.error("Invalid volume certificate for %s (from %s)" % (str(volume_name_or_id), ms_url))
        return None 

    return volume_cert


def get_volume_cert( config, volume_name_or_id, check_cache=True, download=True ):
    """
    Load a volume cert from local disk.
    If not local, then go fetch it.
    Return the volume cert on success.
    Raise an exception on error.
    """

    volume_cert = None 

    if check_cache:
        volume_cert = object_stub.load_volume_cert( config, volume_name_or_id )

    if volume_cert is None and download:

        downloader = config['helpers']['fetch_volume_cert']
        ms_url = config['MS_url']
        volume_cert = volume_cert_fetch( ms_url, volume_name_or_id, downloader )

        if volume_cert is None:
            raise Exception("Failed to obtain volume certificate")

    return volume_cert


def user_cert_fetch( ms_url, user_name_or_id, downloader_path ):
    """
    Use a helper program to go and fetch a user certificate.
    Return the cert on success.
    Return None on error.
    """
    
    if not os.path.exists( downloader_path ):
        log.error("'%s' does not exist" % downloader_path )
        return None 

    downloader = subprocess.Popen( [downloader_path, ms_url, str(user_name_or_id)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    cert_out, cert_err = downloader.communicate()
    downloader.wait()

    if len(cert_err.strip()) != 0:
       log.error("User cert downloader errors:\n%s" % cert_err)

    try:
        user_cert = ms_pb2.ms_user_cert()
        user_cert.ParseFromString( cert_out )
    except Exception, e:
        log.error("Invalid user certificate for %s (from %s)" % (str(user_name_or_id), ms_url))
        return None 

    return user_cert


def get_user_cert( config, user_name_or_id, check_cache=True ):
    """
    Load a user cert from local disk.
    If not local, then go fetch it.
    Return the user cert on success.
    Raise an exception on error.
    """

    user_cert = None 

    if check_cache:
        user_cert = object_stub.load_user_cert( config, user_name_or_id )

    if user_cert is None:

        downloader = config['helpers']['fetch_user_cert']
        ms_url = config['MS_url']
        user_cert = user_cert_fetch( ms_url, user_name_or_id, downloader )

        if user_cert is None:
            raise Exception("Failed to obtain user certificate")

    return user_cert


def cert_bundle_fetch( ms_url, volume_name_or_id, cert_version, downloader_path ):
    """
    Use a helper program to go and fetch the certificate bundle for the given volume.
    Return the cert bundle on success.
    Return None on error.
    """

    if not os.path.exists( downloader_path ):
        log.error("'%s' does not exist" % downloader_path )
        return None 

    downloader = subprocess.Popen( [downloader_path, ms_url, str(volume_name_or_id), str(cert_version)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    cert_out, cert_err = downloader.communicate()
    downloader.wait()

    if len(cert_err.strip()) != 0:
       log.error("Cert bundle downloader errors:\n%s" % cert_err)

    try:
        cert_bundle = sg_pb2.Manifest()
        cert_bundle.ParseFromString( cert_out )
    except Exception, e:
        log.exception(e)
        log.error("Invalid cert bundle for %s.%s (from %s)" % (str(volume_name_or_id), str(cert_version), ms_url))
        return None 

    return cert_bundle


def driver_fetch( ms_url, driver_hash, downloader_path ):
    """
    Use a helper program to go and fetch a gateway's driver.
    Return the serialized driver on success.
    Return None on error.
    """

    if not os.path.exists( downloader_path ):
        log.error("'%s' does not exist" % downloader_path )
        return None 

    downloader = subprocess.Popen( [downloader_path, ms_url, str(driver_hash)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    driver_out, driver_err = downloader.communicate()
    downloader.wait()

    if len(driver_err.strip()) != 0:
       log.error("Driver downloader errors:\n%s" % driver_err)

    try:
        # must be valid JSON
        driver_json = json.loads( driver_out )
    except Exception, e:
        log.error("Invalid driver %s (from %s)" % (str(driver_hash), ms_url))
        return None 

    return driver_out


def cert_cache_dir( config, volume_name_or_id, gateway_name_or_id ):
    """
    Calculate the path to the cached certificate state for this gateway.
    """
   
    cert_dir = conf.object_file_path( config, "certs", "" )
    cert_dir = os.path.join( cert_dir, str(volume_name_or_id), str(gateway_name_or_id) )
    return cert_dir


def cache_cert_path( config, volume_name_or_id, gateway_name_or_id, cert_name, suffix='.cert' ):
    """
    Get the cached path to a certificate
    """
    cert_dir = cert_cache_dir( config, volume_name_or_id, gateway_name_or_id )
    return os.path.join( cert_dir, ("%s" + suffix) % cert_name )


def cert_bundle_version_path( config, volume_name_or_id, gateway_name_or_id ):
    """
    Calculate the path to the volume certificate bundle
    """

    cert_dir = cert_cache_dir( config, volume_name_or_id, gateway_name_or_id )
    version_path = os.path.join( cert_dir, "bundle.version" )
    return version_path


def load_cert_bundle_version( config, volume_name_or_id, gateway_name_or_id ):
    """
    Get the cached volume cert bundle version.
    Return the int on success
    Return None on error
    """
    
    cert_bundle_path = cert_bundle_version_path( config, volume_name_or_id, gateway_name_or_id )
    if not os.path.exists( cert_bundle_path ):
        log.error("No such file or directory: %s" % cert_bundle_path)
        return None 

    try:
        with open( cert_bundle_path, "r" ) as f:
            txt = f.read()
            cert_bundle_version = int(txt.strip() )

        return cert_bundle_version
    
    except Exception, e:
        log.exception(e)
        log.error("Failed to read '%s'" % cert_bundle_path )
        return None


def store_cert_bundle_version( config, volume_name_or_id, gateway_name_or_id, cert_bundle_version ):
    """
    Store the cert bundle version.
    Return True on success
    Return False on error
    """
    
    cert_bundle_path = cert_bundle_version_path( config, volume_name_or_id, gateway_name_or_id )

    try:
        with open( cert_bundle_path, "w" ) as f:
            f.write( str(cert_bundle_version) )
            f.flush()

        return True

    except:
       log.error("Failed to store volume cert bundle version for '%s'" % volume_name_or_id )
       return False
    

def get_cert_bundle( config, volume_name_or_id, volume_version, cert_bundle_version ):
    """
    Get the cert bundle for the volume.
    Always download it, but use the given cert bundle version on disk if present.
    Verify that the fetched cert bundle has an equal or greater version.
    Return the cert bundle on success.
    Return None on error.
    """
    ms_url = config['MS_url']
    downloader_path = config['helpers']['fetch_cert_bundle']

    cert_bundle = cert_bundle_fetch( ms_url, volume_name_or_id, cert_bundle_version, downloader_path )
    if cert_bundle is None:
        raise Exception("Failed to fetch cert bundle for '%s' (version %s)" % (volume_name_or_id, str(cert_bundle_version)))

    # version must be greater or equal
    if cert_bundle.file_version < volume_version:
        raise Exception("Got stale volume version (%s, expected >= %s)" % (str(cert_bundle.file_version), volume_version))
    if cert_bundle.mtime_sec < cert_bundle_version:
        raise Exception("Got stale cert bundle version (%s, expected >= %s)" % (str(cert_bundle.mtime_sec), cert_bundle_version))

    return cert_bundle


def user_cert_validate( config, user_cert ):
    """
    Given an unprotobuf'ed user certificate,
    verify that it is authentic with an external helper.
    Return True on success.
    Return False if not.
    Raise an exception on error.
    """

    validator = config['helpers']['validate_user_cert']
    assert os.path.exists( validator ), ("No such file or directory: %s" % validator)

    certpb = user_cert.SerializeToString()
    validate_proc = subprocess.Popen( [validator], shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    validate_out, validate_err = validate_proc.communicate(certpb)
    validate_proc.wait()

    if validate_proc.returncode != 0:
        # failed
        log.error("Validator rejected user cert for %s" % user_cert.email)
        return False

    else:
        return True


def verify_user_signature( user_cert, object_cert ):
    """
    Verify that a user signed a particular object.
    The object_cert must have a 'signature' attribute.
    The user_cert must have a 'public_key' attribute.
    """
    
    sigb64 = object_cert.signature
    sig = base64.b64decode( sigb64 )
    object_cert.signature = ""
    data = object_cert.SerializeToString()

    try:
        rc = crypto.verify_data( user_cert.public_key, data, sig )
    except ValueError, ve:
        log.error("Signature valuation failed; likely due to the wrong public key")
        rc = False

    object_cert.signature = sigb64
    return rc


def volume_cert_verify( volume_cert, volume_id, volume_owner_cert ):
    """
    Verify that the volume owner (a user) signed the volume cert.
    """
    if not verify_user_signature( volume_owner_cert, volume_cert ):
        log.error("Volume signature mismatch")
        return False 

    if volume_cert.owner_id != volume_owner_cert.user_id:
        log.error("Volume cert owner mismatch")
        return False

    if volume_cert.volume_id != volume_id:
        log.error("Volume ID mismatch")
        return False

    return True


def gateway_cert_verify( gateway_cert, gateway_name, user_cert ):
    """
    Verify that the user signed its gateway cert
    """
    if not verify_user_signature( user_cert, gateway_cert ):
        log.error("Gateway signature mismatch")
        return False 

    if gateway_cert.owner_id != user_cert.user_id:
        log.error("Gateway owner mismatch")
        return False 

    if gateway_cert.name != gateway_name:
        log.error("Gateway name mismatch")
        return False 

    return True


def cert_bundle_verify( cert_bundle, volume_owner_cert ):
    """
    Verify that the cert bundle came from the volume owner
    """
    if not verify_user_signature( volume_owner_cert, cert_bundle ):
        log.error("Cert bundle signature mismatch")
        return False 

    if cert_bundle.owner_id != volume_owner_cert.user_id:
        return False

    return True


def is_volume_cert_in_bundle( cert_bundle, volume_cert ):
    """
    Is the volume certificate in the cert bundle?
    """

    # hash must correspond to the first block 
    if len(cert_bundle.blocks) == 0:
        log.error("Empty cert bundle")
        return False

    cert_hash = crypto.hash_data( volume_cert.SerializeToString() )
    if cert_bundle.blocks[0].hash != cert_hash:
        log.error("Volume block hash mismatch: %s != %s" % (cert_bundle.blocks[0].hash, cert_hash))
        return False 
   
    if cert_bundle.blocks[0].block_version != volume_cert.volume_version:
        log.error("Volume version mismatch (%s != %s)" % (cert_bundle.blocks[0].block_version, volume_cert.volume_version))
        return False 

    if cert_bundle.blocks[0].owner_id != volume_cert.owner_id:
        log.error("Volume owner ID mismatch (%s != %s)" % (cert_bundle.blocks[0].owner_id, volume_cert.owner_id))
        return False
    
    return True


def is_gateway_cert_in_bundle( cert_bundle, gateway_cert ):
    """
    Is the gateway certificate in the cert bundle?
    """
    
    for block in cert_bundle.blocks:

        if block.block_id != gateway_cert.gateway_id:
            # not for this gateway
            continue 

        if block.owner_id != gateway_cert.owner_id:
            # owner mismatch 
            log.error("Gateway '%s' owner mismatch: %s != %s" % (gateway_cert.name, str(block.owner_id), str(gateway_cert.owner_id)))
            return False

        if (gateway_cert.caps | block.caps) != block.caps:
            # caps escalation
            log.error("Gateway '%s' capability escalation: %x != %x" % (gateway_cert.name, block.caps, gateway_cert.caps))
            return False 

        if gateway_cert.version < block.block_version:
            # stale 
            log.error("Gateway '%s' is stale (%s < %s)" % (gateway_cert.name, gateway_cert.version, block.block_version))
            return False 

        # found!
        return True 

    log.error("Gateway '%s' (%s) not found" % (gateway_cert.name, gateway_cert.gateway_id))
    return False


def gateway_certs_load_cached( config, cert_bundle, exclude=[] ):
    """
    Load and return the set of valid cached gateway certificates from the cert bundle.
    Don't load the ones in exclude
    """
    ret = []
    for block in cert_bundle.blocks:

        if block.block_id in exclude:
            continue

        gateway_cert = object_stub.load_gateway_cert( config, block.block_id )
        if gateway_cert is None:
            continue

        if is_gateway_cert_in_bundle( cert_bundle, gateway_cert ):
            ret.append( gateway_cert )

    return ret 


def user_certs_load_cached( config, user_names_or_ids ):
    """
    Load and return the list of valid cached user certificates from the cert bundle.
    """
    ret = []
    for uid in user_names_or_ids:
        user_cert = object_stub.load_user_cert( config, uid )
        if user_cert is None:
            continue

        ret.append( user_cert )

    return ret


def list_cert_paths( config, object_type ):
    """
    List all certs that we have generated ourselves, for a particular object type
    """
    cert_dir = conf.object_file_path( config, object_type, "" )
    listing = os.listdir( cert_dir )
    ret = []

    for name in listing:
        if name in [".", ".."]:
            continue 

        if not name.endswith(".cert"):
            continue 

        object_id_str = name[:-5]
        try:
            object_id = int(object_id_str)
        except:
            continue

        ret.append( os.path.join(cert_dir, name ) )

    return ret


def list_pkey_paths( config, object_type ):
    """
    List all private keys that we have generated ourselves, for a particular object type
    """
    pkey_dir = conf.object_file_path( config, object_type, "" )
    listing = os.listdir( pkey_dir )
    ret = []

    for name in listing:
        if name in [".", ".."]:
            continue 

        if not name.endswith(".pkey"):
            continue 

        ret.append( os.path.join(pkey_dir, name))

    return ret


def list_gateway_cert_paths( config ):
    """
    List the paths to all locally-generated gateway certs
    """
    return list_cert_paths( config, "gateway" )


def list_volume_cert_paths( config ):
    """
    List the paths to all locally-generated volume certs
    """
    return list_cert_paths( config, "volume" )


def list_user_cert_paths( config ):
    """
    List the paths to all locally-generated user certs
    """
    return list_cert_paths( config, "user" )


def list_gateway_pkey_paths( config ):
    """
    List the paths to all locally-generated gateway private keys
    """
    return list_pkey_paths( config, "gateway" )


def list_volume_pkey_paths( config ):
    """
    List the paths to all locally-generated volume private keys
    """
    return list_pkey_paths( config, "volume" )


def list_user_pkey_paths( config ):
    """
    List the paths to all locally-generated user private keys
    """
    return list_pkey_paths( config, "user" )


def get_all_gateway_certs( config, cert_bundle, exclude=[] ):
    """
    Get the set of gateway certs for a cert bundle:
    * load up the fresh cached ones from disk
    * fetch any missing ones
    * verify that the newly-downloaded ones are in the cert bundle
    * verify that each downloaded cert is newer or equal to the cached one
    Don't fetch the ones in exclude.
    Return the list of certs on success.
    Raise an Exception on error.

    Do not call this method directly.
    """

    cached_gateway_certs = gateway_certs_load_cached( config, cert_bundle, exclude=exclude )
    cached_gateways = dict( [ (c.gateway_id, c) for c in cached_gateway_certs ] )

    remote_gateway_ids = []

    for block in cert_bundle.blocks:

        gateway_id = block.block_id
        gateway_version = block.block_version 

        if gateway_id in exclude:
            continue 

        if gateway_id in cached_gateways.keys():
            gateway_cert = cached_gateways[ block.block_id ]
            if gateway_cert.version < gateway_version:
                # cached cert is stale
                del cached_gateways[ gateway_id ]
                remote_gateway_ids.append( gateway_id )

        else:
            # not cached 
            remote_gateway_ids.append( gateway_id )

    # fetch remote
    for gateway_id in remote_gateway_ids:

        if gateway_id in exclude:
            continue

        gateway_cert = get_gateway_cert( config, gateway_id, check_cache=False )
        if gateway_cert is None:
            raise Exception("Failed to fetch gateway cert for %s" % gateway_id )

        if not is_gateway_cert_in_bundle( cert_bundle, gateway_cert ):
            # NOTE: includes the case where the downloaded cert is stale
            raise Exception("Gateway cert for %s not in the cert bundle" % gateway_id )
             
        cached_gateways[ gateway_id ] = gateway_cert

    return [ cached_gateways[gateway_id] for gateway_id in cached_gateways.keys() ]


def get_all_user_certs( config, user_ids ):
    """
    Given a list of user IDs
    * load up fresh cached ones from disk
    * fetch any missing ones
    * verify that they are valid 
    Return the list of user certs on success
    Raise an exception on error.
    """

    cached_user_certs = user_certs_load_cached( config, user_ids )
    cached_users = dict( [(c.user_id, c) for c in cached_user_certs] )

    remote_user_ids = []

    # make sure they're all valid (re-download them otherwise)
    for user_id in user_ids:
        
        if user_id in cached_users.keys():
            user_cert = cached_users[user_id]
            rc = user_cert_validate( config, user_cert )
            if not rc:
                # no longer valid 
                log.warn("Cached cert for user '%s' is no longer valid" % user_cert.email)
                remote_user_ids.append( user_id )
                del cached_users[user_id]

        else:
            remote_user_ids.append( user_id )

    # fetch any non-local certs 
    for user_id in remote_user_ids:

        user_cert = get_user_cert( config, user_id, check_cache=False )
        if user_cert is None:
            raise Exception("Failed to fetch user cert %s" % user_id )

        rc = user_cert_validate( config, user_cert )
        if not rc:
            raise Exception("Failed to validate cert for %s" % user_id )

        cached_users[ user_id ] = user_cert

    return [ cached_users[user_id] for user_id in cached_users.keys() ]


def verify_all_gateway_certs( gateway_certs, user_certs ):
    """
    Given a list of all gateway certs and user certs,
    verify that each user signed their respetive
    gateway certs.
    """
    user_dict = {}
    for user_cert in user_certs:
        user_dict[ user_cert.user_id ] = user_cert

    for gateway_cert in gateway_certs:
        user_cert = user_dict.get( gateway_cert.owner_id, None )
        assert user_cert is not None, "Gateway '%s' has no owner" % (gateway_cert.name)

        rc = verify_user_signature( user_cert, gateway_cert )
        assert rc, "Gateway '%s' not signed by '%s'" % (gateway_cert.name, user_cert.email)

    return True


def clear_cert_cache( config, volume_name_or_id, gateway_name_or_id ):
    """
    Clear out the cached certs for this gateway.
    """

    cache_dir = cert_cache_dir( config, volume_name_or_id, gateway_name_or_id )
    if os.path.exists( cache_dir ):
        shutil.rmtree( cache_dir )

    os.makedirs( cache_dir )


def user_is_anonymous( user_name_or_id ):
    """
    Determine if the user name or ID corresponds to
    the anonymous user.
    """

    try:
        user_name_or_id = int(user_name_or_id)
    except:
        pass

    if type(user_name_or_id) in [str, unicode]:
        if user_name_or_id.lower() == "anonymous":
            return True
        else:
            return False

    elif type(user_name_or_id) in [int, long]:
        if user_name_or_id == 0xFFFFFFFFFFFFFFFFL:
            return True
        else:
            return False

    else:
        raise "Failed to interpret '%s'" % (str(user_name_or_id))


def store_cert( path, cert, link_path=None ):
    """
    Store a certificate
    Return True on success.
    Return False on error.
    """
     
    cert_pb = cert.SerializeToString()
    try:
        with open( path, "w" ) as f:
            f.write( cert_pb )
            f.flush()

    except Exception, e:
        log.exception(e)
        log.error("Failed to store cert to '%s'" % (path))
        return False

    if link_path is not None:
        if os.path.exists( link_path ):
            log.error("Link exists: '%s'" % link_path )
            return False 

        try:
            os.link( path, link_path )
        except Exception, e:
            log.exception(e)
            log.error("Failed to link '%s' to '%s'" % (path, link_path))
            return False
            
    return True


def certs_reload( config, user_name_or_id, volume_name_or_id, gateway_name_or_id ):
    """
    Reload the set of certificates for this volume.
    Succeeds if:
    * the volume owner and each gateway owner can be validated
    * the volume owner signed the cert_bundle
    * the volume is in the cert bundle
    * the volume is sigend by its owner
    * each gateway is present in the cert bundle
    * each gateway is signed by its owner
    * this gateway's cert is signed by this user's cert
    Return True on success
    Raise an exception on error
    """

    user_cert = None 

    # get user cert, if not anonymous
    if not user_is_anonymous( user_name_or_id ):
        user_cert = get_user_cert( config, user_name_or_id )
        assert user_cert is not None, "Failed to get user cert for %s" % user_name_or_id

        # validate 
        rc = user_cert_validate( config, user_cert )
        assert rc, "Failed to validate user cert for %s" % user_name_or_id

    # get cached cert bundle version 
    volume_cert_bundle_version = load_cert_bundle_version( config, volume_name_or_id, gateway_name_or_id )
    if volume_cert_bundle_version is None:
        log.warning("No cached cert bundle version for volume '%s'" % volume_name_or_id)
        volume_cert_bundle_version = 1

    # get cached volume version 
    volume_cert_version = 1
    cached_volume_cert = get_volume_cert( config, volume_name_or_id, download=False )
    if cached_volume_cert is not None:
        volume_cert_version = cached_volume_cert.volume_version

    # get cert bundle
    cert_bundle = get_cert_bundle( config, volume_name_or_id, volume_cert_version, volume_cert_bundle_version )
    assert cert_bundle is not None, "Failed to get cert bundle for %s.%s" % (volume_name_or_id, volume_cert_bundle_version)

    # get volume owner 
    volume_owner_cert = get_user_cert( config, cert_bundle.owner_id )
    assert volume_owner_cert is not None, "Failed to get volume owner cert for %s" % (volume_name_or_id)

    # verify volume owner 
    rc = user_cert_validate( config, volume_owner_cert )
    assert rc, "Failed to verify volume owner certificate"

    # verify cert bundle 
    rc = cert_bundle_verify( cert_bundle, volume_owner_cert )
    assert rc, "Failed to verify cert bundle"

    # get the volume cert
    volume_cert = get_volume_cert( config, volume_name_or_id )
    if volume_cert is None:
        raise Exception("Failed to fetch volume cert")

    if volume_cert.volume_id != cert_bundle.volume_id:
        raise Exception("Volume cert mismatch")

    # verify that it's in the bundle 
    rc = is_volume_cert_in_bundle( cert_bundle, volume_cert )
    assert rc, "Volume '%s' not present in cert bundle" % volum_cert.name

    # verify the owner signed it 
    rc = verify_user_signature( volume_owner_cert, volume_cert )
    assert rc, "Volume '%s' not signed by owner '%s'" % (volume_cert.name, volume_owner_cert.email)

    # sanity check... 
    assert volume_owner_cert.user_id == volume_cert.owner_id, "Volume cert owner mismatch"
    assert volume_owner_cert.email == volume_cert.owner_email, "Volume cert email mismatch"

    # get gateway cert 
    gateway_cert = get_gateway_cert( config, gateway_name_or_id )
    assert gateway_cert is not None, "Failed to get gateway certificate"

    # verify this user signed it, if not anonymous
    if user_cert is not None:
        rc = verify_user_signature( user_cert, gateway_cert )
        assert rc, "Failed to verify gateway cert for '%s' signed by '%s'" % (gateway_cert.name, user_cert.email)

    # otherwise, verify the volume owner did
    else:
        rc = verify_user_signature( volume_owner_cert, gateway_cert )
        assert rc, "Failed to verify anonymous gateway cert for '%s' signed by '%s'" % (gateway_cert.name, volume_owner_cert.email)

    # verify that our gateway cert is present in the cert bundle
    rc = is_gateway_cert_in_bundle( cert_bundle, gateway_cert )
    assert rc, "Gateway '%s' not present in cert bundle" % gateway_cert.name

    # get the gateway certs in the bundle, verifying them along the way
    gateway_certs = get_all_gateway_certs( config, cert_bundle, exclude=[volume_cert.volume_id, gateway_cert.gateway_id] )

    # get and verify the user certs for each gateway
    user_certs = get_all_user_certs( config, list( set( [cert.owner_id for cert in gateway_certs] ) ) )

    # verify that each user signed their gateway 
    rc = verify_all_gateway_certs( gateway_certs, user_certs )
    assert rc, "Failed to verify that users own their gateways"

    # clear the cache 
    clear_cert_cache( config, volume_name_or_id, gateway_name_or_id )

    # generate new cache data
    volume_cert_bundle_version = cert_bundle.mtime_sec
    store_cert_bundle_version( config, volume_name_or_id, gateway_name_or_id, volume_cert_bundle_version )

    volume_name = volume_cert.name
    gateway_name = gateway_cert.name 

    # cache each user, volume, and gateway cert
    # do so by name and by id
    all_user_certs = user_certs
    if volume_owner_cert.user_id not in [c.user_id for c in all_user_certs]:
        all_user_certs.append( volume_owner_cert )
    
    for cert in all_user_certs:
        path = cache_cert_path( config, volume_name, gateway_name, "user-%s" % cert.email )
        link_path = cache_cert_path( config, volume_name, gateway_name, "user-%s" % cert.user_id )
        
        rc = store_cert( path, cert, link_path=link_path )
        assert rc, "Failed to store cert for '%s' to '%s' and '%s'" % (cert.email, path, link_path)

    for cert in gateway_certs + [gateway_cert]:
        path = cache_cert_path( config, volume_name, gateway_name, "gateway-%s" % cert.name )
        link_path = cache_cert_path( config, volume_name, gateway_name, "gateway-%s" % cert.gateway_id )

        rc = store_cert( path, cert, link_path=link_path )
        assert rc, "Failed to store cert for '%s' to '%s' and '%s'" % (cert.name, path, link_path )

    path = cache_cert_path( config, volume_name, gateway_name, "volume-%s" % volume_cert.name )
    link_path = cache_cert_path( config, volume_name, gateway_name, "volume-%s" % volume_cert.volume_id )
    
    rc = store_cert( path, volume_cert, link_path=link_path )
    assert rc, "Failed to store cert for '%s' to '%s' and '%s'" % (volume_cert.name, path, link_path )

    # store the bundle as well
    path = cache_cert_path( config, volume_name, gateway_name, "%s.bundle" % volume_cert.name, suffix='' )
    with open( path, "w" ) as f:
        f.write( cert_bundle.SerializeToString() )
        f.flush()

    return True


def driver_reload( config, volume_name, gateway_name ):
    """
    Download a gateway's driver and cache it locally.
    Looks in the cached cert directory for the gateway cert.
    This method is meant to be called immediately after 
    a successful call to certs_reload().

    Return True on success
    Raise an exception on failure 
    """

    cached_path = cache_cert_path( config, volume_name, gateway_name, "gateway-%s" % gateway_name )
    gateway_cert = object_stub.load_gateway_cert( config, gateway_name, path=cached_path )
    if gateway_cert is None:
        raise Exception("No such cert for gateway %s/%s" % (volume_name, gateway_name) )

    ms_url = config['MS_url']
    driver_downloader = config['helpers']['fetch_driver']
    driver_hash = binascii.hexlify( gateway_cert.driver_hash )
    driver_text = driver_fetch( ms_url, driver_hash, driver_downloader )
    if driver_text is None:
        raise Exception("Failed to fetch driver %s from %s" % ( driver_hash, ms_url ) )

    # save it 
    cached_path = cache_cert_path( config, volume_name, gateway_name, "driver-%s" % driver_hash, suffix='' )
    try:
        with open(cached_path, "w") as f:
            f.write( driver_text )
            f.flush()

    except Exception, e:
        log.exception(e)
        raise

    return True
