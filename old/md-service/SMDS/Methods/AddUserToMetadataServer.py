#!/usr/bin/python

from SMDS.method import Method
from SMDS.user import *
from SMDS.mdserver import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *

class AddUserToMetadataServer( Method ):
   """
   Add a user to a metadata server.  The caller must be the owner of the metadata server, or an admin.
   """
   
   accepts = [
         Auth(),
         Mixed(Parameter(int, "User ID of the user who will be added to this server"),
               Parameter(str, "Username of the user who will be added to this server")),
         
         Mixed(MDServer.fields['server_id'], MDServer.fields['name'])
   ]
   roles = ['admin','user']
   returns = Parameter(int, "1 if successful; otherwise a negative error code from failing to update the metadata server")
   
   
   def call(self, auth, username_or_id, metadata_server_name_or_id):
      assert self.caller is not None
      
      roles = self.caller['roles']
      
      owner = None
      try:
         owners = Users(self.api, {'username': auth['Username']})
         owner = owners[0]
      except:
         raise MDObjectNotFound( auth['Username'], "Caller not found" )
      
      # look up this metadata server
      mdservers = None
      mdserver = None
      try:
         if isinstance( metadata_server_name_or_id, int ):
            mdservers = MDServers( self.api, {'server_id': metadata_server_name_or_id} )
         else:
            mdservers = MDServers( self.api, {'name': metadata_server_name_or_id} )
            
         mdserver = mdservers[0]
      except Exception, e:
         raise MDObjectNotFound( 'MDServer(%s)' % (metadata_server_name_or_id), str(e))
      
      # make sure this server is owned by the caller, or that the caller is an admin
      if 'admin' not in roles and mdserver['owner'] != owner['user_id']:
         raise MDUnauthorized( "User(%s) is not the owner of MDServer(%s)" % (owner['username'], metadata_server_name_or_id) )
         
      # look up this user to be added
      users = None
      user = None
      try:
         if isinstance(username_or_id, str):
            users = Users( self.api, {'username':username_or_id, 'enabled': True})
         else:
            users = Users( self.api, {'user_id':username_or_id, 'enabled': True})
         
         user = users[0]
      except Exception, e:
         raise MDObjectNotFound( 'User(%s)' % (username_or_id), str(e) )
         
      user = users[0]
      
      # make sure this user isn't already registered on this server
      if user['user_id'] in mdserver['user_ids']:
         raise MDInvalidArgument( "User(%s) is already registered on MDServer(%s)" % (username_or_id, metadata_server_name_or_id ) )
      
      # add the user to the metadata server
      mdserver.add_user( user )
      server_id = mdserver['server_id']
      
      # reload
      mdserver = MDServers( self.api, [server_id])[0]
      
      rc = mdserver.restart_server( force_start = False )
      if rc == 1:
         return 1
      else:
         raise MDMetadataServerError("Could not restart metadata server")
      