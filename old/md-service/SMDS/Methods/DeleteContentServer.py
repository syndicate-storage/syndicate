#!/usr/bin/python

from SMDS.method import Method
from SMDS.content import *
from SMDS.user import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *
import SMDS.logger as logger
import traceback

class DeleteContentServer( Method ):
   """
   Delete a content server.  This method will work either if this user is an admin
   or the user owns the content server
   """
   
   accepts = [
         Auth(),
         Content.fields['content_id']
   ]
   roles = ['admin','user']
   returns = Parameter(int, "1 if successful; negative error code if the content server could not be unregistered from the CDN")
   
   def call(self, auth, content_id):
      assert self.caller is not None
      
      roles = self.caller['roles']
      
      # look up user
      user = None
      try:
         users = Users(self.api, {'username': auth['Username']})
         user = users[0]
      except Exception, e:
         raise MDObjectNotFound( "User(%s)" % auth['Username'], str(e) )
      
      # sanity check
      if content_id not in user['content_ids']:
         raise MDInvalidArgument( "Content(%s) is not owned by User(%s)" % (content_id, auth['Username']), "DeleteContentServer" )
      
      # look up content
      content = None
      try:
         contents = Contents( self.api, {'content_id': content_id} )
         content = contents[0]
      except Exception, e:
         raise MDObjectNotFound( "Content(%s)" % content_id, str(e) )
      
      if (self.caller == None or 'admin' not in roles) and content['owner'] != user['user_id']:
         raise MDUnauthorized( "User(%s) is not allowed to delete Content(%s)" % (auth['Username'], content_id) )
      
      
      # do the deletion
      content_id = content['content_id']
      user.remove_content( content )
      content.delete()
      
      rc = self.api.cdn.rm_content( content_id )
      
      return rc