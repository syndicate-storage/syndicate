#!/usr/bin/python

from SMDS.method import Method
from SMDS.content import *
from SMDS.user import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *
import SMDS.logger as logger

class AddContentServer( Method ):
   """
   Add a content server under the control of a user.  Register the server
   on the underlying CDN as well.
   
   The owner of this content will be the user that calls this method.
   """
   
   accepts = [
         Auth(),
         dict([(k,Content.fields[k]) for k in ['host_url']])
   ]
   roles = ['admin','user']
   returns = Parameter(int, "The content's ID (positive number) if successful")
   
   def call(self, auth, content_fields):
      assert self.caller is not None
      
      users = Users(self.api, {'username': auth['Username']})
      user = users[0]
      
      # how many contents has this user created?
      num_contents = len( user['content_ids'] )
      
      if num_contents > user['max_contents']:
         raise MDResourceExceeded( 'User(%s)' % (user['username']), 'content server' )
      
      content_fields['owner'] = user['user_id']
      
      
      # register this content on the CDN
      remote_content_id = self.api.cdn.add_content( user, content_fields['host_url'] )
      if remote_content_id == None or remote_content_id < 0:
         raise MDInternalError("AddContentServer: CDN could not add '%s' to the CDN with user '%s'" % (content_fields.get('host_url'), content_fields.get('username')))
      
      c = Content( self.api, content_fields )
      c['content_id'] = remote_content_id
      c.sync( insert=True )
      
      user.add_content( c )  # will synchronize itself
      user.sync()
      
      return c['content_id']