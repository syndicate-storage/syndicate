#!/usr/bin/python

from SMDS.method import Method
from SMDS.content import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *
from SMDS.filter import *
from SMDS.user import *
import SMDS.logger as logger

class GetContentServers( Method ):
   """
   Read zero or more content servers.  If no filter is given, every content server added by the caller is read.
   Admins can see all content servers; users can only see their own
   """
   
   accepts = [
         Auth(),
         Parameter( dict, "Fields to filter on", nullok = True ),
         Parameter( [str], "List of fields to return", nullok = True )
   ]
   roles = ['admin','user']
   returns = [Content.fields]
   
   """
   def call(self, auth, content_filter, return_fields):
      assert self.caller is not None
      
      
      # ask the CDN for the content 
      content = self.api.cdn.get_content( content_filter, return_fields )
      return content
  
   """
   
   def call(self, auth, content_filter, return_fields):
      assert self.caller is not None
      
      roles = self.caller['roles']
      
      if not content_filter:
         content_filter = {}
      
      if not return_fields:
         return_fields = [name for (name,param) in Content.fields.items()]
      
      users = Users(self.api, {'username': auth['Username']}) 
      user = users[0]
      
      if content_filter.get('owner') != None and content_filter['owner'] != user['user_id'] and (self.caller == None or 'admin' not in roles):
         raise MDUnauthorized( "User(%s) is not allowed to read content servers from User(%s)" % (user['username'], content_filter['owner']) )
      
      # if the caller isn't an admin, then only read content servers with the caller's user id
      if 'admin' not in roles:
         content_filter['owner'] = user['user_id']
      
      contents = Contents( self.api, content_filter )
      
      # remove non-specified return fields
      ret = []
      for c in contents:
         c_dict = {}
         for rf in return_fields:
            if rf in c:
               c_dict[rf] = c[rf]
            
         ret.append( c_dict )
            
      return ret
   