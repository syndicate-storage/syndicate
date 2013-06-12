#!/usr/bin/python

from SMDS.method import Method
from SMDS.content import *
from SMDS.user import *
from SMDS.mdserver import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *

class DeleteUser( Method ):
   """
   Remove a user account.  A user may remove himself/herself, but no one else.  An admin may remove anyone.
   All of the user's registered content and metadata servers will be removed as well.  The user will be
   removed from all metadata servers.
   """
   
   accepts = [
         Auth(),
         Mixed(User.fields['user_id'], User.fields['username'] )
   ]
   
   roles = ['admin', 'user']
   returns = Parameter(int, "1 if successful")
   
   def call(self, auth, username_or_id ):
      assert self.caller is not None
      
      roles = self.caller['roles']
      
      user = None
      try:
         if isinstance(username_or_id, str):
            users = Users(self.api, {'username':username_or_id})
         else:
            users = Users(self.api, {'user_id':username_or_id})
         
         user = users[0]
      except Exception, e:
         raise MDObjectNotFound( 'User(%s)' % (username_or_id), str(e) )
      
      
      # user can only delete himself/herself, unless admin
      if ('admin' not in roles) and user['username'] != auth['Username']:
         raise MDUnauthorized( "User(%s) cannot be deleted by User(%s)" % (username_or_id, auth['Username']) )
      
      # unregister this user from every metadata server it's subscribed to
      for md_id in user['sub_mdserver_ids']:
         md = None
         try:
            mds = MDServers( self.api, {'server_id': md_id} )
            md = mds[0]
         except:
            continue
         
         try:
            md.remove_user( user )
         except MDException, mde:
            raise MDMethodFailed( "Could not remove User(%s) from MDServer(%s)" % (user['username'], md_id), mde )
         except Exception, e:
            raise MDMethodFailed( "Failed to destroy User(%s)" % username_or_id, str(e))
         
      
      # unregister all other users from each of the user's metadata servers and delete them
      md_ids = user['my_mdserver_ids']
      if len(md_ids) > 0:
         mdservers = MDServers( self.api, md_ids )
         for mdserver in mdservers:
            
            # destroy this server locally.
            # This removes all registered users as well
            mdserver.delete()
            
            # destroy this server remotely
            rc = mdserver.destroy_server()
            if rc != 1:
               logger.error("Could not destory MDServer(%s)" % mdserver['name'])
            
      content_ids = user['content_ids']
      if len(content_ids) > 0:
         contents = Contents( self.api, content_ids )
         for content in contents:
            rc = self.api.cdn.rm_content( content )
            if rc != 1:
               logerr.error( "Could not unregister Content(%s)" % c['host_url'] )
            
            user.remove_content( content )
            content.delete()
      
      # unregister this user on the CDN
      rc = self.api.cdn.rm_user( user )
      user.delete()
      
      return rc
      