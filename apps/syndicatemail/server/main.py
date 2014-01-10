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

import os
import sys
import errno
import base64
import shutil
import uuid

import syndicatemail.common.storage as storage
import syndicatemail.common.keys as keys

from Crypto.Hash import SHA256 as HashAlg
from Crypto.Hash import HMAC
from Crypto.PublicKey import RSA as CryptoKey
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Signature import PKCS1_PSS as CryptoSigner
from Crypto import Random

import hashlib

import syndicate.client.common.log as Log

log = Log.get_logger()

import bottle
from bottle import Bottle, request, response, abort, static_file, HTTPError

USER_ACCOUNTS_DIR = "/tmp/syndicatemail-user-accounts"
INCOMING_MAIL_DIR = "/tmp/syndicatemail-incoming-mail"

BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890=+/"


#-------------------------
app = Bottle()
config = None          # initialized at runtime

#-------------------------
def user_mail_dir( user_addr ):
   global INCOMING_MAIL_DIR
   return storage.path_join( INCOMING_MAIL_DIR, user_addr )

#-------------------------
def user_account_dir( user_addr ):
   global USER_ACCOUNTS_DIR
   return storage.path_join( USER_ACCOUNTS_DIR, user_addr )

#-------------------------
def read_user_pubkey( user_addr ):
   user_dir = user_account_dir( user_addr )
   user_key_path = storage.path_join( user_dir, "pubkey" )
   
   try:
      fd = open( user_key_path )
      pubkey_str = fd.read()
      fd.close()
      
      return pubkey_str
   
   except Exception, e:
      log.exception(e)
      log.error("Failed to read public key for %s" % user_addr )
      return None
   
#-------------------------
def read_user_pubkey_sig( user_addr ):
   user_dir = user_account_dir( user_addr )
   user_key_path = storage.path_join( user_dir, "pubkey.sig" )
   
   try:
      fd = open( user_key_path )
      pubkey_str = fd.read()
      fd.close()
      
      return pubkey_str
   
   except Exception, e:
      log.exception(e)
      log.error("Failed to read public key signature for %s" % user_addr )
      return None

#-------------------------
def add_or_replace_user( user_addr, pubkey_pem, pubkey_sig ):
   salt = os.urandom(64)
   
   user_dir = user_account_dir( user_addr )
   try:
      os.makedirs( user_dir )
   except OSError, oe:
      if oe.errno != errno.EEXIST:
         raise oe
      else:
         log.info("Replace user %s" % user_addr )

   pubkey_path = storage.path_join( user_dir, "pubkey" )
   pubkey_sig_path = storage.path_join( user_dir, "pubkey.sig")
   
   try:
      pubkey_fd = open( pubkey_path, "w" )
      pubkey_fd.write( pubkey_pem )
      pubkey_fd.close()
      
      pubkey_sig_fd = open( pubkey_sig_path, "w" )
      pubkey_sig_fd.write( pubkey_sig )
      pubkey_sig_fd.close()
   
      return True
   
   except Exception, e:
      log.exception(e)
      log.error("Failed to add or replace user info for %s" % user_addr)
      return False


#-------------------------
def store_user_incoming_mail( user_addr, incoming_message ):
   storage_root = user_mail_dir( user_addr )
   
   if not os.path.exists( storage_root ):
      try:
         os.makedirs( storage_root )
      except OSError, oe:
         if oe.errno != errno.EEXIST:
            log.exception(oe)
            raise oe
      except Exception, e:
         log.exception(e)
         raise e
      
   h = hashlib.sha256()
   h.update( incoming_message )
   msg_hash = h.hexdigest()
   
   m_path = storage.path_join( storage_root, msg_hash )
   if os.path.exists( m_path ):
      log.warning("Duplicate message")
      return True 
   
   try:
      m_fd = open( m_path, "w" )
      m_fd.write( incoming_message )
      m_fd.close()
      
      return True
   
   except Exception, e:
      log.exception(e)
      log.error("Failed to store incoming message for %s" % user_addr )
      return False


#-------------------------
def clear_user_incoming_mail( user_addr ):
   storage_root = user_mail_dir( user_addr )
   
   present = os.listdir(storage_root)
   for m in present:
      m_path = storage.path_join( storage_root, m )
      try:
         os.unlink( m_path )
      except Exception, e:
         log.exception(e)
         log.error("Failed to remove %s" % m_path )

   return True

#-------------------------
def authenticate_user_content( user_addr, data, signature ):
   user_pubkey_pem = read_user_pubkey( user_addr )
   if user_pubkey_pem is None:
      log.error("failed to load user public key for %s" % user_addr)
      return False
   
   return keys.verify_data( user_pubkey_pem, data, signature )

      
#-------------------------
@app.get('/USERKEY/<user_addr:path>')
def get_user_pubkey( user_addr ):
   response.content_type = "application/text-plain"
   pubkey = read_user_pubkey( user_addr )
   if not pubkey:
      raise HTTPError( status=404 )
   else:
      return pubkey

#-------------------------
@app.get('/USERSIG/<user_addr:path>')
def get_user_pubkey_sig( user_addr ):
   response.content_type = "application/text-plain"
   sig = read_user_pubkey_sig( user_addr )
   if sig is not None:
      sig = base64.b64encode( sig )
      return sig
   
   else:
      raise HTTPError( status=404 )

#-------------------------
@app.get("/MAIL/<user_addr:path>")
def get_incoming_mail( user_addr ):
   # FIXME: streaming/paging solution
   storage_root = user_mail_dir( user_addr )
   
   if not os.path.exists( storage_root ):
      return {"messages": []}
   
   # get size and listing
   mail_list = os.listdir( storage_root )
   mail_list.sort()
   
   ret = {"messages": []}
   
   messages = []
   
   for m in mail_list:
      m_path = storage.path_join( storage_root, m )
      m_fd = open( m_path, "r" )
      m_data = m_fd.read()
      m_fd.close()
      
      messages.append( m_data )
   
   ret['messages'] = messages
   return ret
   
   
#-------------------------
@app.post('/MAIL')
def store_incoming_mail():
   # FIXME verify that these messages are coming from legitimate users, somehow.
   global INCOMING_MAIL_DIR
   
   sender_addr = request.forms.get("sender")
   receiver_addr = request.forms.get("receiver")

   # TODO: validate sender somehow?
   
   message_data = request.forms.get("message")
   if message_data is None:
      raise HTTPError( status=400, message="Message required" )
   
   # is this bas64? swap all the characters to None and see if the length is 0
   if len( unicode(message_data).translate(dict( [(ord(char), None) for char in BASE64_CHARS] ) ) ) != 0:
      # not base64
      raise HTTPError( status=400, body="Message data is not base64 encoded" )
   
   # store it for us 
   rc = store_user_incoming_mail( receiver_addr, message_data )
   
   if not rc:
      raise HTTPError( status=500, body="Failed to store message")
   
   return "OK"
   
   
#-------------------------
@app.post('/CLEAR')
def clear_incoming_mail():
   
   username = request.forms.get("username")
   sig_b64 = request.forms.get("signature")
   nonce_b64 = request.forms.get("nonce")
   
   if sig_b64 is None or username is None or nonce_b64 is None:
      raise HTTPError( status=400, message="Username, signature and nonce required") 

   sig = base64.b64decode( sig_b64 )
   nonce = base64.b64decode( nonce_b64 )
   
   rc = authenticate_user_content( username, nonce, sig )
   if not rc:
      raise HTTPError( status=403, body="Signature verification failure" )
   
   # clear our incoming box
   rc = clear_user_incoming_mail( username )
   
   if not rc:
      raise HTTPError( status=500, body="Failed to clear incoming message cache")
   
   return "OK"

#-------------------------
def setup_debug():
   for d in [USER_ACCOUNTS_DIR, INCOMING_MAIL_DIR]:
      try:
         shutil.rmtree(d)
         os.makedirs(d)
      except:
         pass
      
      
   alice_syndicate_privkey_str = open(os.path.expanduser("~/.syndicate/user_keys/signing/testuser@gmail.com.pkey")).read()
   bob_syndicate_privkey_str = open(os.path.expanduser("~/.syndicate/user_keys/signing/judecn@gmail.com.pkey")).read()
   
   pubkey_str_alice = open("../../syndicatemail/test/alice.pub").read().strip()
   pubkey_str_bob = open("../../syndicatemail/test/bob.pub").read().strip()
   
   # signature = sign_public_key( pubkey_str, syndicate_user_privkey )
   pubkey_sig_alice = keys.sign_data( alice_syndicate_privkey_str, pubkey_str_alice )
   pubkey_sig_bob = keys.sign_data( bob_syndicate_privkey_str, pubkey_str_bob )
   
   add_or_replace_user( "alice.mail.localhost:8080@localhost:33334", pubkey_str_alice, pubkey_sig_alice )
   add_or_replace_user( "bob.mail2.localhost:8080@localhost:33334", pubkey_str_bob, pubkey_sig_bob )
   

if __name__ == "__main__":
   # start serving
   
   setup_debug()
   
   for d in [USER_ACCOUNTS_DIR, INCOMING_MAIL_DIR]:
      try:
         os.makedirs(d)
      except:
         pass
   
   app.run( host="localhost", port=33334, debug=True )