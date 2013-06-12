#!/usr/bin/python

from SMDS.method import Method
from SMDS.mdserver import *
from SMDS.user import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *

import urllib2

class UpdateMetadataServer( Method ):
   """
   Update a metadata server. A user can only update a metadata server that he/she owns.  An admin can update any metadata server.
   If the server is running, this method restarts it on successful application of the updated fields
   """
   
   accepts = [
         Auth(),
         Mixed( MDServer.fields['server_id'], MDServer.fields['name'] ),
         dict([(n,MDServer.fields[n]) for n in ['auth_read', 'auth_write', 'portnum']])
   ]
   roles = ['admin','user']
   returns = Parameter(int, "1 if successful; negative error code otherwise")
   
   
   def load_mdserver( self, mdserver_name_or_id ):
      mdserver = None
      try:
         mdservers = []
         if isinstance(mdserver_name_or_id, str):
            mdservers = MDServers( self.api, {'name': mdserver_name_or_id} )
         else:
            mdservers = MDServers( self.api, {'server_id': mdserver_name_or_id} )
            
         mdserver = mdservers[0]
      except Exception, e:
         raise MDObjectNotFound( "MDServer(%s)" % mdserver_name_or_id )
      
      return mdserver
      
      
   def call(self, auth, mdserver_name_or_id, mdserver_fields):
      assert self.caller is not None
      
      roles = self.caller['roles']
      
      # look up the user ID
      users = Users( self.api, {'username': auth['Username']} )
      user = users[0]
      
      # look up this metadata server
      mdserver = self.load_mdserver( mdserver_name_or_id )
      
      # sanity check--this server must be owned by the caller, or the caller must be admin
      if ('admin' not in roles) and mdserver['server_id'] not in user['my_mdserver_ids']:
         raise MDUnauthorized( "User(%s) cannot update MDServer(%s)" % (auth['Username'], mdserver_name_or_id) )
      
      # url-encode the name
      if mdserver_fields.get('name'):
         mdserver_fields['name'] = urllib2.quote( mdserver_fields['name'] )
      
      mdserver.update( mdserver_fields )
      mdserver.sync()
      
      rc = 1
      if mdserver['status'] == 'running':
         rc = mdserver.restart_server()
         
      if rc != 1:
         raise MDMetadataServerError( "Could not restart metadata server" )
      else:
         return 1