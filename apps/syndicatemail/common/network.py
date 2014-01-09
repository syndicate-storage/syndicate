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

import urllib2
import httplib
from urlparse import urlparse
import json

import contact
import message

from Crypto.Hash import SHA256 as HashAlg
from Crypto.Hash import HMAC
from Crypto.PublicKey import RSA as CryptoKey
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Signature import PKCS1_PSS as CryptoSigner
from Crypto import Random


import syndicate.client.common.log as Log

log = Log.get_logger()

STORAGE_DIR = "/tmp"

# -------------------------------------
def download( url ):
   conn = None
   host = urlparse( url ).netloc
   path = urlparse( url ).path
   
   port = 80
   
   if ":" in host:
      # FIXME: RFC, proxies
      host, port = host.split(":")
      port = int(port)
      
   if url.lower.startswith( "https" ):
      conn = httplib.HTTPSConnection( host, port )
   else:
      conn = httplib.HTTPConnection( host, port )
   
   conn.request( 'GET', path )
   resp = conn.getresponse()
   if resp.status == 200:
      data = resp.read()
      resp.close()
      return data
   
   else:
      log.error("GET %s HTTP %d" % url, resp.status )
      return None
   

# -------------------------------------
def download_pubkey( url ):
   data = download( url )
   
   try:
      pubkey = CryptoKey.importKey( data )
      assert not pubkey.has_private()
      data = pubkey.exportKey()
      
      return data 
   
   except Exception, e:
      log.exception(e)
      log.error("Failed to download public key from %s" % url)
      return None
   

# -------------------------------------
def download_user_pubkey_from_MS( addr, use_http=False ):
   try:
      parsed_addr = contact.parse_addr( addr )
   except Exception, e:
      log.exception(e)
      log.error("Invalid email address %s" % addr)
      return None
   
   scheme = "https://"
   if use_http:
      scheme = "http://"
      
   MS_url = scheme + parsed_addr.MS 
   #MS_pubkey_url = os.path.join( MS_url, # how do we deal with this?
                                
   
# -------------------------------------
def download_user_pubkey_from_SyndicateMail( addr ):
   pass

# -------------------------------------
def download_user_pubkey( addr ):
   print "FIXME: download_user_pubkey stub"
      
   pubkey_str = """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxwhi2mh+f/Uxcx6RuO42
EuVpxDHuciTMguJygvAHEuGTM/0hEW04Im1LfXldfpKv772XrCq+M6oKfUiee3tl
sVhTf+8SZfbTdR7Zz132kdP1grNafGrp57mkOwxjFRE3FA23T1bHXpIaEcdhBo0R
rXyEnxpJmnLyNYHaLN8rTOig5WFbnmhIZD+xCNtG7hFy39hKt+vNTWK98kMCOMsY
QPywYw8nJaax/kY5SEiUup32BeZWV9HRljjJYlB5kMdzeAXcjQKvn5y47qmluVmx
L1LRX5T2v11KLSpArSDO4At5qPPnrXhbsH3C2Z5L4jqStdLYB5ZYZdaAsaRKcc8V
WpsmzZaFExJ9Nj05sDS1YMFMvoINqaPEftS6Be+wgF8/klZoHFkuslUNLK9k2f65
A7d9Fn/B42n+dCDYx0SR6obABd89cR8/AASkZl3QKeCzW/wl9zrt5dL1iydOq2kw
JtgiKSCt6m7Hwx2kwHBGI8zUfNMBlfIlFu5CP+4xLTOlRdnXqYPylT56JQcjA2CB
hGBRJQFWVutrVtTXlbvT2OmUkRQT9+P5wr0c7fl+iOVXh2TwfaFeug9Fm8QWoGyP
GuKX1KO5JLQjcNTnZ3h3y9LIWHsCTCf2ltycUBguq8Mwzb5df2EkOVgFeLTfWyR2
lPCia/UWfs9eeGgdGe+Wr4sCAwEAAQ==
-----END PUBLIC KEY-----
""".strip()

   return pubkey_str

# -------------------------------------
def post_message( encrypted_incoming_message ):
   # called by the endpoint
   # start to inform the recipient's server that they have a new message
   print "FIXME: post_message stub"
   return True

# -------------------------------------
def get_incoming_messages( addr ):
   # called by the endpoint
   # get the list of new messages from the server and store them locally
   pass

# -------------------------------------
def clear_incoming_messages( addr, timestamp ):
   # called by the endpoint
   # tell server that we have all new messages up to a timestamp
   pass


# -------------------------------------
def send_legacy_email( dest_addr, message, attachments ):
   # send legacy email
   return True
