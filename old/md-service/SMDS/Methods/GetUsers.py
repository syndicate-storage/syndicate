#!/usr/bin/python

from SMDS.method import Method
from SMDS.mdserver import *
from SMDS.user import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *
from SMDS.filter import *

class GetUsers( Method ):
   """
   Get a list of users
   """
   
   accepts = [
         Auth(),
         Filter( User.fields ),
         Parameter([str], "List of fields to return", nullok = True )
   ]
   roles = ['admin','user']
   returns = [User.fields]
   
   def call(self, auth, user_fields, return_fields=None ):
      assert self.caller is not None
      
      if 'password' in user_fields:
         del user_fields['password']
      
      if not return_fields:
         return_fields = [name for (name,param) in User.fields.items()]
         
      if 'password' in return_fields:
         return_fields.remove( 'password' )
      
      users = Users( self.api, user_fields )
      
      ret = []
      for u in users:
         u_dict = {}
         for rf in return_fields:
            if rf in u.keys():
               u_dict[rf] = u[rf]
            
         ret.append( u_dict )
      
      return ret