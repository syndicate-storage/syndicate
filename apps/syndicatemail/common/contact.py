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

import collections
import re
import os
import pickle
import base64
import urllib

import storage
import singleton

from Crypto.PublicKey import RSA as CryptoKey

import syndicate.client.common.log as Log

log = Log.get_logger()

STORAGE_DIR = "/contacts"

# for setup
VOLUME_STORAGE_DIRS = [
   STORAGE_DIR
]

LOCAL_STORAGE_DIRS = []

CACHED_CONTACT_LIST = "contacts.cache"

# RFC-822 compliant, as long as there aren't any comments in the address.
# taken from http://chrisbailey.blogs.ilrt.org/2013/08/19/validating-email-addresses-in-python/
email_regex_str = r"^(?=^.{1,256}$)(?=.{1,64}@)(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22)(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22))*\x40(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d])(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d]))*$"

email_regex = re.compile( email_regex_str )

# -------------------------------------
SyndicateMailAddr = collections.namedtuple( "SyndicateMailAddr", ["addr", "volume", "MS", "username", "server"] )
SyndicateContact = collections.namedtuple( "SyndicateMailContact", ["addr", "pubkey_pem", "extras"] )

# -------------------------------------
def is_valid_email( addr_str ):
   return email_regex.match( addr_str )

# -------------------------------------
def quote_addr( addr_str ):
   addr_parts = addr_str.split(".")
   
   
   if len(addr_parts) < 3:
      raise Exception("Invalid Syndicate email address: '%s'" % addr_str)
   
   username = addr_parts[0]
   volume = addr_parts[1]
   
   host_parts = (".".join(addr_parts[2:])).split("@")
   
   if len(host_parts) < 2:
      raise Exception("Invalid Syndicate email address: '%s'" % addr_str)
   
   return make_addr_str( urllib.quote(username), urllib.quote(volume), urllib.quote(host_parts[0]), urllib.quote(host_parts[1]))

# -------------------------------------
def parse_addr( addr_str ):
   addr_str_quoted = quote_addr( addr_str )
   # does it match?
   if not is_valid_email( addr_str_quoted ):
      raise Exception("Invalid email address: '%s'" % addr_str)
   
   # syndicatemail format:
   # username.volumename.MS-host@server
   
   addr_parts = addr_str.split(".")
   
   if len(addr_parts) < 3:
      raise Exception("Invalid Syndicate email address: '%s'" % addr_str)
   
   username = addr_parts[0]
   volume = addr_parts[1]
   
   host_parts = (".".join(addr_parts[2:])).split("@")
   
   if len(host_parts) < 2:
      raise Exception("Invalid Syndicate email address: '%s'" % addr_str)
   
   MS_host = host_parts[0]
   email_server = host_parts[1]
   
   ret = SyndicateMailAddr( addr=addr_str, volume=volume, MS=MS_host, server=email_server, username=username )
   return ret


# -------------------------------------
def make_addr_str( uid, volume_name, MS, server ):
   assert '.' not in uid, "Invalid user ID %s: cannot contain '.'" % uid
   return "%s.%s.%s@%s" % (uid, volume_name, MS, server)

# -------------------------------------
def make_contact_path( pubkey_str, email_addr ):
   global STORAGE_DIR
   
   """
   enc_filename = storage.encrypt_data( pubkey_str, email_addr )
   enc_filename_salted = storage.salt_string( enc_filename )
   enc_filename_b64 = base64.b64encode( enc_filename_salted )
   """
   # TODO: make this scheme IND-CPA
   filename_salted = storage.salt_string( email_addr )
   filename_b64 = base64.b64encode( filename_salted )
   return storage.volume_path( STORAGE_DIR, filename_b64 )


# -------------------------------------
def read_contact_from_path( privkey_str, contact_path ):
   
   contact_json_str = storage.read_encrypted_file( privkey_str, contact_path )
   if contact_json_str is None:
      log.error( "Could not read contact %s" % contact_path )
      return None
   
   try:
      contact = storage.json_to_tuple( SyndicateContact, contact_json_str )
   except Exception, e:
      log.error("Failed to load contact %s" % contact_path )
      log.exception(e)
      return None
   
   return contact

# -------------------------------------
def read_contact( pubkey_str, privkey_str, email_addr ):
   try:
      mail_addr = parse_addr( email_addr )
   except Exception, e:
      log.error("Invalid email '%s'" % email_addr)
      log.exception(e)
      return None
   
   contact_path = make_contact_path( pubkey_str, email_addr )
   
   contact = read_contact_from_path( privkey_str, contact_path )
   if contact is None:
      log.error("Failed to read contact %s" % email_addr)
   
   return contact

# -------------------------------------
def write_contact( pubkey_str, contact ):
   global STORAGE_DIR
   
   contact_path = make_contact_path( pubkey_str, contact.addr )
   
   try:
      contact_json = storage.tuple_to_json( contact )
   except Exception, e:
      log.error("Failed to serialize contact")
      log.exception(e)
      return False
   
   rc = storage.write_encrypted_file( pubkey_str, contact_path, contact_json )
   if not rc:
      return False
   
   # purge contacts cache
   storage.purge_cache( CACHED_CONTACT_LIST )
   return True
   

# -------------------------------------
def contact_exists( pubkey_str, email_addr ):
   contact_path = make_contact_path( pubkey_str, email_addr )
   return storage.path_exists( contact_path )
   
# -------------------------------------
def add_contact( pubkey_str, email_addr, contact_pubkey_str, contact_fields ):
   try:
      parsed_addr = parse_addr( email_addr )
   except:
      raise Exception("Invalid email address %s" % email_addr )
   
   try:
      pubkey = CryptoKey.importKey( contact_pubkey_str )
      assert not pubkey.has_private()
   except:
      raise Exception("Invalid public key")
   
   contact = SyndicateContact( addr=email_addr, pubkey_pem=contact_pubkey_str, extras = contact_fields )
   if contact_exists( pubkey_str, email_addr ):
      log.error( "Contact '%s' already exists" % email_addr )
      return None
   
   return write_contact( pubkey_str, contact )
   
   
# -------------------------------------
def update_contact( pubkey_str, privkey_str, email_addr, extras ):
   contact_path = make_contact_path( pubkey_str, email_addr )
   if not storage.path_exists( contact_path ):
      log.error("No such contact '%s'" % email_addr)
      return False
   
   try:
      contact = read_contact( pubkey_str, privkey_str, email_addr )
   except Exception, e:
      log.error("Failed to read contact '%s'" % email_addr)
      log.exception(e)
      return False 
   
   contact.extras.update( extras )
   
   return write_contact( pubkey_str, contact )
   
# -------------------------------------
def delete_contact( pubkey_str, email_addr ):
   global STORAGE_DIR, CACHED_CONTACT_LIST
   
   contact_path = make_contact_path( pubkey_str, email_addr )
   
   rc = storage.delete_file( contact_path )
   if not rc:
      log.exception( Exception("Failed to detete contact") )
      return False
   
   else:
      storage.purge_cache( CACHED_CONTACT_LIST )
      
   return True

# -------------------------------------
def list_contacts( pubkey_str, privkey_str, start_idx=None, length=None ):
   global STORAGE_DIR, CACHED_CONTACT_LIST
   
   cached_contacts = storage.get_cached_data( privkey_str, CACHED_CONTACT_LIST )
   if cached_contacts == None:
      log.info("No cached contacts")
   else:
      return cached_contacts
   
   contact_dir = storage.volume_path( STORAGE_DIR )   
   dir_ents = storage.listdir( contact_dir )
   dir_ents.sort()
   
   if start_idx == None:
      start_idx = 0
      
   if length == None:
      length = len(dir_ents)
   
   if start_idx + length > len(dir_ents):
      length = len(dir_ents) - start_idx
      
   dir_ents = dir_ents[start_idx:start_idx + length]
   
   contact_emails = []
   
   for contact_filename in dir_ents:
      contact_path = storage.path_join( contact_dir, contact_filename )
      contact = read_contact_from_path( privkey_str, contact_path )
      if contact == None:
         log.warning("Failed to read contact file %s" % contact_path)
         continue
   
      contact_emails.append( contact.addr )
   
   storage.cache_data( pubkey_str, CACHED_CONTACT_LIST, contact_emails )
   return contact_emails
   
      
if __name__ == "__main__":
   import session 
   
   fake_module = collections.namedtuple( "FakeModule", ["VOLUME_STORAGE_DIRS", "LOCAL_STORAGE_DIRS"] )
   fake_vol = session.do_test_volume( "/tmp/storage-test/volume" )
   singleton.set_volume( fake_vol )
   
   fake_mod = fake_module( LOCAL_STORAGE_DIRS=LOCAL_STORAGE_DIRS, VOLUME_STORAGE_DIRS=VOLUME_STORAGE_DIRS )
   assert storage.setup_storage( "/apps/syndicatemail/data", "/tmp/storage-test/local", [fake_mod] ), "setup_storage failed"
   
   
   test_email_addrs = [
      "jude.mailvolume.syndicate.com@email.princeton.edu",
      "wathsala.mailvolume.foo.syndicate.com@bar.com",
      "muneeb.volume.localhost@localhost"
   ]
   
   print "-------- parse_addr -----------"
   for addr in test_email_addrs:
      print addr
      print "%s" % str(parse_addr( addr ))
     
   print "-------- contact DB -----------"
   
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
   
   privkey_str = """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
""".strip()
   
   contact_jude = SyndicateContact( addr="jude.mailvolume.syndicate.com@example.com", pubkey_pem = pubkey_str, extras = {"Phone number": "5203318323"} )
   contact_wathsala = SyndicateContact( addr="wathsala.mailvolume.foo.syndicate.com@princeton.edu", pubkey_pem = pubkey_str, extras = {"City": "Princeton"} )
   
   print "-------- Test existing -------------"
   
   print "jude: %s" % str(contact_jude)
   print "wathsala: %s" % str(contact_wathsala)
   
   add_contact( pubkey_str, contact_jude.addr, contact_jude.pubkey_pem, contact_jude.extras )
   
   contact_list = list_contacts( pubkey_str, privkey_str )
   print "listing: %s" % str(contact_list)
   
   add_contact( pubkey_str, contact_wathsala.addr, contact_jude.pubkey_pem, contact_wathsala.extras )
   
   contact_list = list_contacts( pubkey_str, privkey_str )
   print "listing: %s" % str(contact_list)
   
   contact_jude2 = read_contact( pubkey_str, privkey_str, "jude.mailvolume.syndicate.com@example.com" )
   contact_wathsala2 = read_contact( pubkey_str, privkey_str, "wathsala.mailvolume.foo.syndicate.com@princeton.edu" )
   
   print "-------- Test nonexistant -------------"
   try:
      none = read_contact( pubkey_str, privkey_str, "no.one.syndicate.com@nowhere.com" )
      assert none is None
   except Exception, e:
      log.exception(e)
      pass
   
   print "jude2: %s" % str(contact_jude2)
   print "wathsala2: %s" % str(contact_wathsala2)
   
   print "-------- Test update -------------"
   
   update_contact( pubkey_str, privkey_str, "jude.mailvolume.syndicate.com@example.com", {"City": "Princeton"} )
   update_contact( pubkey_str, privkey_str, "wathsala.mailvolume.foo.syndicate.com@princeton.edu", {"City": "San Francisco", "State": "CA"} )
   
   contact_jude3 = read_contact( pubkey_str, privkey_str, "jude.mailvolume.syndicate.com@example.com" )
   contact_wathsala3 = read_contact( pubkey_str, privkey_str, "wathsala.mailvolume.foo.syndicate.com@princeton.edu" )
   
   print "jude3: %s" % str(contact_jude3)
   print "wathsala3: %s" % str(contact_wathsala3)
   
   print "-------- Test listing -------------"
   
   contact_list = list_contacts( pubkey_str, privkey_str )
   print "listing: %s" % str(contact_list)
   
   contact_list = list_contacts( pubkey_str, privkey_str )
   print "listing: %s" % str(contact_list)
   
   print "-------- Test Delete -------------"
   
   delete_contact( pubkey_str, "jude.mailvolume.syndicate.com@example.com" )
   
   contact_list = list_contacts( pubkey_str, privkey_str )
   print "listing: %s" % str(contact_list)
   
   delete_contact( pubkey_str, "wathsala.mailvolume.foo.syndicate.com@princeton.edu" )
   
   contact_list = list_contacts( pubkey_str, privkey_str )
   print "listing: %s" % str(contact_list)
   
   print "-------- Read after delete -------------"
   try:
      none = read_contact( pubkey_str, privkey_str, "jude.mailvolume.syndicate.com@example.com" )
      assert none is None 
      
      none = read_contact( pubkey_str, privkey_str, "wathsala.mailvolume.foo.syndicate.com@princeton.edu" )
      assert none is None
   except Exception, e:
      log.exception(e)
      pass
   