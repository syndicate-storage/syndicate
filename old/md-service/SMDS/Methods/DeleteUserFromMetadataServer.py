#!/usr/bin/python

from SMDS.method import Method
from SMDS.user import *
from SMDS.mdserver import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *

class DeleteUserFromMetadataServer( Method ):
   """
   Remove one or more users from a metadata server.  The caller is only allowed to call this method on his/her own metadata server;
   but an admin can call this method on any metadata server.
   
   The caller is not allowed to remove a user from the metadata server owned by that user.
   """
   
   accepts = [
         Auth(),
         Mixed(Parameter(int, "User ID of the user to be removed from this server"),
               Parameter(str, "Username of the user to be removed from this server"),
               Parameter([int], "User IDs of the users to be removed from this server")),
         
         Mixed(Parameter(int, "Metadata server ID"),
               Parameter(str, "Metadata name"))
   ]
   roles = ['admin','user']
   returns = Parameter(int, "1 if successful; otherwise a negative error code from failure to update the metadata server")
   
   
   def load_mdserver( self, metadata_server_id ):
      mdservers = None
      if isinstance(metadata_server_id, str):
         mdservers = MDServers( self.api, {'name': metadata_server_id} )
      else:
         mdservers = MDServers( self.api, {'server_id': metadata_server_id} )
         
      mdserver = mdservers[0]
      return mdserver
   
   
   def call(self, auth, username_or_id, metadata_server_id):
      assert self.caller is not None
      
      roles = self.caller['roles']
         
      owners = Users( self.api, {'username': auth['Username']})
      owner = owners[0]
      
      # look up this metadata server
      mdservers = None
      mdserver = None
      try:
         mdserver = self.load_mdserver( metadata_server_id )
      except Exception, e:
         raise MDObjectNotFound( 'MDServer(%s)' % (metadata_server_id), str(e))
      
      # make sure this server is owned by the caller, or that the caller is an admin
      if ('admin' not in roles) and mdserver['owner'] != owner['user_id']:
         raise MDUnauthorized( "User(%s) is not the owner of MDServer(%s)" % (owner['username'], mdserver['server_id']) )
      
      # look up this user to be removed
      users = None
      user = None
      remove_many = False
      try:
         if isinstance(username_or_id, str):
            users = Users(self.api, {'username':username_or_id})
         elif isinstance(username_or_id, int) or isinstance(username_or_id, long):
            users = Users(self.api, {'user_id':username_or_id})
         elif isinstance(username_or_id, list):
            users = Users(self.api, username_or_id)
            remove_many = True
         
         user = users[0]
      except Exception, e:
         raise MDObjectNotFound( 'User(%s)' % (username_or_id), str(e) )
         
      # sanity check--the user to remove can't be the metadata server's owner
      if (not remove_many and user['user_id'] == mdserver['owner']) or (remove_many and mdserver['owner'] in [u['user_id'] for u in users]):
         raise MDInvalidArgument( "User(%s) owns MDServer(%s), so it can't be removed" % (auth['Username'], metadata_server_id ) )
      
      # actually remove the user
      if remove_many:
         mdserver.remove_users( [u['user_id'] for u in users] )
      else:
         mdserver.remove_user( user )
      
      # reload
      mdserver = self.load_mdserver( metadata_server_id )
      
      rc = mdserver.restart_server(force_start=False)
      if rc == 1:
         return 1
      else:
         raise MDMetadataServerError( "Could not restart server" )
      