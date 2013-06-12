#!/usr/bin/python

from SMDS.method import Method
from SMDS.mdserver import *
from SMDS.user import *
from SMDS.mdserver import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *

class DeleteMetadataServer( Method ):
   """
   Delete a metadata server.  This will also stop it.  Unregister all other users from this server.
   The caller can delete only their own metadata servers, unless they are an admin.
   """
   
   accepts = [
         Auth(),
         Mixed( MDServer.fields['name'], MDServer.fields['server_id'] )
   ]
   roles = ['admin','user']
   returns = Parameter(int, "1 if successful; otherwise a negative error code resulting from a failure to shut down the metadata server")
   
   def call(self, auth, mdserver_name_or_id):
      assert self.caller is not None
      
      roles = self.caller['roles']
         
      # look up the mdserver
      md = None
      try:
         if isinstance(mdserver_name_or_id, str):
            mds = MDServers( self.api, {'name': mdserver_name_or_id} )
         else:
            mds = MDServers( self.api, {'server_id': mdserver_name_or_id} )
         md = mds[0]
      except Exception, e:
         raise MDObjectNotFound( "MDServer(%s)" % (mdserver_name_or_id), str(e) )
      
      # look up the user
      user = None
      try:
         user_identifier = None
         if 'admin' not in roles:
            users = Users( self.api, {'username': auth['Username']} )
            user_identifier = auth['Username']
         else:
            users = Users( self.api, {'user_id': md['owner']} )
            user_identifier = md['owner']
         user = users[0]
      except Exception, e:
         raise MDObjectNotFound( "User(%s)" % user_identifier, str(e) )
      
      # if we're not an admin and we don't own this server, then we're unauthorized
      if ('admin' not in roles) and user['user_id'] != md['owner']:
         raise MDUnauthorized( "User(%s) is not allowed to delete MDServer(%s)" % (auth['Username'], mdserver_name_or_id) )
      
      # remove this mdserver from the user
      user.remove_mdserver( md )
      
      # unregister all users from this server
      md.remove_users( md['user_ids'] )
      
      rc = md.destroy_server()
      
      md.delete()
      
      return rc