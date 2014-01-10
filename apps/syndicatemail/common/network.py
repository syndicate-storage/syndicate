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
import base64
import urllib
import os

import contact
import message
import storage
import keys

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
      
   if url.lower().startswith( "https" ):
      conn = httplib.HTTPSConnection( host, port )
   else:
      conn = httplib.HTTPConnection( host, port )
   
   try:
      conn.request( 'GET', path )
      resp = conn.getresponse()
      if resp.status == 200:
         data = resp.read()
         resp.close()
         return data
      
      else:
         log.error("GET %s HTTP %d" % (url, resp.status) )
         return None
      
   except Exception, e:
      log.exception(e)
      log.error("Download %s failed" % url)
      return None
   

# -------------------------------------
def upload( url, forms ):
   forms_encoded = urllib.urlencode( forms )
   headers = {
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "text/plain"
   }
   
   conn = None
   host = urlparse( url ).netloc
   path = urlparse( url ).path
   
   port = 80
   
   if ":" in host:
      # FIXME: RFC, proxies
      host, port = host.split(":")
      port = int(port)
      
   if url.lower().startswith( "https" ):
      conn = httplib.HTTPSConnection( host, port )
   else:
      conn = httplib.HTTPConnection( host, port )
   
   try:
      conn.request( 'POST', path, forms_encoded, headers )
      resp = conn.getresponse()
      if resp.status == 200:
         data = resp.read()
         resp.close()
         return data
      
      else:
         data = resp.read()
         log.error("POST %s HTTP %d (%s)" % (url, resp.status, data) )
         return None
      
   except Exception, e:
      log.exception(e)
      log.error("Upload to %s failed" % url )

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
def download_user_syndicate_pubkey( volume_name, MS_host, use_http=False ):
   scheme = "https://"
   if use_http or "localhost" in MS_host:
      scheme = "http://"
   
   MS_pubkey_url = storage.path_join( scheme + MS_host, "VOLUMEOWNER", volume_name )
   user_json_str = download( MS_pubkey_url )
   if user_json_str is None:
      log.error("Download %s returned None" % MS_pubkey_url)
      return None
   
   try:
      user_json = json.loads( user_json_str )
   except Exception, e:
      log.exception(e)
      log.error("Bad JSON '%s'" % user_json_str )
      return None
   
   if user_json.get("pubkey", None) is None:
      log.error("No public key returned from %s" % MS_host)
      return None
                
   user_pubkey = user_json.get("pubkey", None)
   return user_pubkey
                                
   
# -------------------------------------
def download_user_mail_pubkey( addr, use_http=False ):
   try:
      parsed_addr = contact.parse_addr( addr )
   except Exception, e:
      log.exception(e)
      log.error("Invalid email address %s" % addr)
      return None
   
   scheme = "https://"
   if use_http or "localhost" in parsed_addr.MS:
      scheme = "http://"
   
   server_url = storage.path_join( scheme + parsed_addr.server, "/USERKEY", addr )
   pubkey_str = download( server_url )
   
   return pubkey_str

# -------------------------------------
def download_user_mail_pubkey_sig( addr, use_http=False ):
   try:
      parsed_addr = contact.parse_addr( addr )
   except Exception, e:
      log.exception(e)
      log.error("Invalid email address %s" % addr)
      return None
   
   scheme = "https://"
   if use_http or "localhost" in parsed_addr.MS:
      scheme = "http://"
   
   server_url = storage.path_join( scheme + parsed_addr.server, "/USERSIG", addr )
   sig_b64 = download( server_url )
   
   if sig_b64 != None:
      sig = base64.b64decode( sig_b64 )
      return sig
   
   else:
      return None

# -------------------------------------
def download_user_pubkey( addr, allow_mismatch=False, use_http=False ):
   try:
      parsed_addr = contact.parse_addr( addr )
   except Exception, e:
      log.exception(e)
      log.error("Failed to read contact %s" % addr )
      return None
   
   pubkey_str_MS = download_user_syndicate_pubkey( parsed_addr.volume, parsed_addr.MS, use_http=use_http )
   pubkey_str_mail = download_user_mail_pubkey( addr, use_http=use_http )
   pubkey_sig = download_user_mail_pubkey_sig( addr, use_http=use_http )
   
   if pubkey_str_MS is None:
      log.error("No public key from MS for '%s'" % addr)
      return None
   
   if pubkey_str_mail is None:
      log.error("No public key from server for '%s'" % addr)
      return None
   
   if pubkey_sig is None:
      log.error("No public key signature from server for '%s'" % addr)
      return None
      
   pubkey_str_MS = "%s" % CryptoKey.importKey(pubkey_str_MS).exportKey()
   pubkey_str_mail = "%s" % CryptoKey.importKey(pubkey_str_mail).exportKey()
   
   rc = keys.verify_data( pubkey_str_MS, pubkey_str_mail, pubkey_sig )
   if not rc:
      log.error("!!! Signature verification FAILED!  It is possible that your MS and/or mail server is compromised !!!")
      
      if not allow_mismatch:
         raise Exception("!!! Signature verification FAILED!  It is possible that your MS and/or mail server is compromised !!!" )
   
   return pubkey_str_mail

# -------------------------------------
def post_message( sender_privkey_pem, encrypted_incoming_message, use_http=False ):
   # called by the endpoint
   # start to inform the recipient's server that they have a new message
   # EncryptedIncomingMessage = collections.namedtuple( "EncryptedIncomingMessage", ["incoming_message_ciphertext", "sender_addr", "receiver_addr", "signature"] )
   # serialize this 
   try:
      data = storage.tuple_to_json( encrypted_incoming_message )
      data = base64.b64encode( data )
   except Exception, e:
      log.exception(e)
      log.error("Failed to serialize")
      return False
   
   sig = keys.sign_data( sender_privkey_pem, data )
   
   sig_b64 = base64.b64encode( sig )
   
   forms = {
      "sender": encrypted_incoming_message.sender_addr,
      "receiver": encrypted_incoming_message.receiver_addr,
      "signature": sig_b64,
      "message": data
   }
   
   try:
      parsed_receiver_addr = contact.parse_addr( encrypted_incoming_message.receiver_addr )
   except Exception, e:
      log.exception(e)
      log.error("Failed to parse '%s'" % encrypted_incoming_message.receiver_addr )
      return False
   
   # get receiver syndicatemail
   receiver_server = parsed_receiver_addr.server
   scheme = "https://"
   if use_http or "localhost" in receiver_server:
      scheme = "http://"
   
   server_url = storage.path_join( scheme + receiver_server, "/MAIL" )
   resp_data = upload( server_url, forms )
   
   if resp_data is not None:
      log.info("Server response: '%s'" % resp_data)
   
   else:
      log.error("Failed to post message")
      return False
                      
   return True

# -------------------------------------
def get_incoming_messages( addr, use_http=False ):
   # called by the endpoint.
   # get all incoming messages.
   # FIXME: streaming/paging solution
   # FIXME: sign/verify messages?
   
   try:
      parsed_addr = contact.parse_addr( addr )
   except Exception, e:
      log.exception(e)
      log.error("Invalid address '%s'" % addr)
      return None
   
   scheme = "https://"
   if use_http or "localhost" in parsed_addr.server:
      scheme = "http://"
      
   server_url = storage.path_join( scheme + parsed_addr.server, "/MAIL/%s" % addr )
   message_json = download( server_url )
   
   if message_json is None:
      log.error("Failed to read incoming messages")
      return None
   
   try:
      incoming_messages = json.loads( message_json )
   except Exception, e:
      log.exception(e)
      log.error("Failed to parse incoming messages")
      return None
      
   if "messages" not in incoming_messages:
      log.error("Invalid JSON '%s'" % message_json )
      return None
   
   return incoming_messages["messages"]

# -------------------------------------
def clear_incoming_messages( privkey_str, addr, use_http=False ):
   # called by the endpoint
   # tell server that we have all new messages up to a timestamp
   # FIXME: pagination, streaming, ?
   
   try:
      parsed_addr = contact.parse_addr( addr )
   except Exception, e:
      log.exception(e)
      log.error("Invalid address '%s'" % addr)
      return None
   
   nonce = os.urandom(64)
   nonce_sig = keys.sign_data( privkey_str, nonce )
   nonce_sig_b64 = base64.b64encode( nonce_sig )
   
   forms = {
      "username": addr,
      "nonce": base64.b64encode( nonce ),
      "signature": nonce_sig_b64
   }
   
   # send clear hint
   receiver_server = parsed_addr.server
   scheme = "https://"
   if use_http or "localhost" in receiver_server:
      scheme = "http://"
   
   server_url = storage.path_join( scheme + receiver_server, "/CLEAR" )
   resp_data = upload( server_url, forms )
   
   if resp_data is not None:
      log.info("Server response: '%s'" % resp_data)
   
   else:
      log.error("Failed to clear messages")
      return False
   
   return True


# -------------------------------------
def send_legacy_email( dest_addr, message, attachments ):
   # send legacy email
   return True


if __name__ == "__main__":
   import message
   
   
   emsg = message.EncryptedIncomingMessage( sender_addr="alice.mail.localhost:8080@localhost:33334", receiver_addr="bob.mail2.localhost:8080@localhost:33334", signature="haha", incoming_message_ciphertext="fooey" )
   
   alice_privkey_pem = open( "../../../../syndicatemail/test/alice.pem" ).read().strip()
   bob_privkey_pem = open( "../../../../syndicatemail/test/bob.pem").read().strip()
   
   alice_pubkey_pem = open( "../../../../syndicatemail/test/alice.pub" ).read().strip()
   bob_pubkey_pem = open("../../../../syndicatemail/test/bob.pub" ).read().strip()
   
   print "------ post message -----"
   rc = post_message( alice_privkey_pem, emsg, use_http=True )
   assert rc, "post_message failed"
   
   print "----- get messages -----"
   enc_msgs = get_incoming_messages( "bob.mail2.localhost:8080@localhost:33334", use_http=True )
   
   assert len(enc_msgs) != 0, "get_incoming_messages failed"
   
   print "----- clear messages -----"
   rc = clear_incoming_messages( bob_privkey_pem, "bob.mail2.localhost:8080@localhost:33334", use_http=True )
   assert rc, "clear_incoming_messages failed"
   