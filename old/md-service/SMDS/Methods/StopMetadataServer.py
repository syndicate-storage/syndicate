#!/usr/bin/python

from SMDS.method import Method
from SMDS.mdserver import *
from SMDS.user import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.filter import *
from SMDS.faults import *

class StopMetadataServer( Method ):
   """
   Deactivate an existing metadata server.  The caller must own the metadata server, unless they are an admin.
   """
   
   accepts = [
         Auth(),
         Mixed( MDServer.fields['server_id'], MDServer.fields['name'] )
   ]
   roles = ['admin','user']
   returns = Parameter([str], "The read and write URLs of the metadata server (on success)")
   
   def call( self, auth, mdserver_name_or_id ):
      roles = []
      if self.caller:
         roles = self.caller['roles']
         
      users = Users( self.api, {'username': auth['Username']} )
      user = users[0]
      
      mdserver = None
      try:
         mdservers = None
         if isinstance( mdserver_name_or_id, str ):
            mdservers = MDServers( self.api, {'name': mdserver_name_or_id})
         else:
            mdservers = MDServers( self.api, {'server_id': mdserver_name_or_id} )
            
         mdserver = mdservers[0]
      except Exception, e:
         raise MDObjectNotFound( "MDServer(%s)", mdserver_name_or_id, e )
      
      if (self.caller == None or 'admin' not in roles) and mdserver['owner'] != user['user_id']:
         raise MDUnauthorized( "MDServer(%s) cannot be stopped by User(%s)" % (mdserver_name_or_id, user['username']) )
      
      rc = mdserver.stop_server()
      if rc != 1:
         raise MDMetadataServerError( "Could not stop metadata server" )
      else:
         return rc
   