#!/usr/bin/env python

from openid.association import Association
from openid.store.interface import OpenIDStore
from openid.store import nonce

from google.appengine.ext import ndb
import logging
import time

from Crypto.Hash import SHA256


def url_hash( server_url ):
   # generate hash
   sha256 = SHA256.new()
   sha256.update( server_url )
   server_url_hash = sha256.hexdigest()
   return server_url_hash


class OpenIDAssociation( ndb.Model ):
   server_url_hash = ndb.StringProperty() # used as key
   server_url = ndb.TextProperty()
   association_bits = ndb.TextProperty()
   handle = ndb.TextProperty()            # association.handle
   issued = ndb.IntegerProperty()         # when was the association issued? (association.issue)
   expires = ndb.IntegerProperty()
   
   @classmethod
   def make_key_name( cls, server_url, handle ):
      if handle == None:
         handle = ""
      
      return "server_url=%s,handle=%s" % (server_url, handle)
      
class OpenIDNonce( ndb.Model ):
   salt = ndb.StringProperty()
   server_url = ndb.TextProperty()
   timestamp = ndb.IntegerProperty()

   @classmethod
   def make_key_name( cls, server_url, timestamp, salt ):
      return "server_url=%s,timestamp=%s,salt=%s" % (server_url, timestamp, salt)
   
      
class GAEStore( OpenIDStore ):
   
   def storeAssociation( self, server_url, association ):
      """
      Stuff a (server_url, association) into ndb
      """
      handle = association.handle
      gae_assoc = OpenIDAssociation.get_or_insert( OpenIDAssociation.make_key_name( server_url, handle ) )

      gae_assoc.server_url_hash = url_hash( server_url )
      gae_assoc.server_url = server_url
      gae_assoc.handle = handle
      gae_assoc.issued = association.issued
      gae_assoc.expires = association.issued + association.lifetime
      gae_assoc.association_bits = association.serialize()

      gae_assoc.put()

   def getAssociation( self, server_url, handle=None ):
      """
      Get an association from ndb
      """
      if handle == None:
         # find the newest association
         qry = OpenIDAssociation.query( OpenIDAssociation.server_url_hash == url_hash( server_url ) ).order( -OpenIDAssociation.issued )
         results = qry.fetch(1)

         if results == None or len(results) == 0:
            return None

         gae_assoc = results[0]
         if gae_assoc == None:
            return None

         association = None
         
         try:
            association = Association.deserialize( gae_assoc.association_bits )
         except:
            gae_assoc.key.delete()
            return None

         return association
         
      else:
         # find the specific association
         gae_assoc_key = ndb.Key( OpenIDAssociation, OpenIDAssociation.make_key_name( server_url, handle ) )
         gae_assoc = gae_assoc_key.get()

         if gae_assoc == None:
            return None

         try:
            association = Association.deserialize( gae_assoc.association_bits )
         except:
            gae_assoc_key.delete()
            return None

         return association

         
   def removeAssociation( self, server_url, handle ):
      """
      Delete an association from ndb
      """
      gae_assoc_key = ndb.Key( OpenIDAssociation.make_key_name( server_url, handle ) )
      gae_assoc_key.delete()
      

   def useNonce(self, server_url, timestamp, salt):
      """
      See if we can use this nonce.
      """
      if abs(time.time() - timestamp) > nonce.SKEW:
         return False

      if not server_url:
         server_url = ""
         
      gae_nonce = OpenIDNonce.get_or_insert( OpenIDNonce.make_key_name( server_url, timestamp, salt ) )
      if gae_nonce.server_url == None:
         # this is new
         gae_nonce.server_url = server_url
         gae_nonce.timestamp = timestamp
         gae_nonce.salt = salt

         gae_nonce.put()
         
         return True
         
      else:
         # in use
         return False
         

   def cleanupNonces(self):
      """
      Remove expired nonces
      """
      qry = OpenIDNonce.query( OpenIDNonce.timestamp > time.time() - nonce.SKEW )

      cnt = qry.count()

      expired_nonce_keys = qry.fetch( cnt, keys_only=True )
      
      ndb.delete_multi( expired_nonce_keys )
      

   def cleanupAssociations(self):
      """
      Remove expired associations
      """
      qry = OpenIDAssociation.query( OpenIDAssociation.expires > time.time() )

      cnt = qry.count()

      expired_assoc_keys = qry.fetch( cnt, keys_only=True )

      ndb.delete_multi( expired_assoc_keys )
      

