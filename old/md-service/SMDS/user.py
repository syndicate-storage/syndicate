#!/usr/bin/python

"""
Borrowed from PLCAPI's Person.py.  Modifications by Jude Nelson
"""

from SMDS.db import Row, Table
import SMDS.logger as logger
from SMDS.parameter import Parameter, Mixed
from SMDS.filter import Filter
from SMDS.content import Content
import SMDS.mdserver

from types import *

class User(Row):
   
   table_name = 'users'
   primary_key = 'user_id'
   
   fields = {
      'user_id':           Parameter(long, "User ID"),
      'username':          Parameter(str, "Username", max=128),
      'password':          Parameter(str, "Password hash", max=254),
      'email':             Parameter(str, "User's email address", max=254),
      'enabled':           Parameter(bool, "Has the user been enabled?"),
      'roles':             Parameter([str], "List of roles this user fulfulls"),
      'max_mdservers':     Parameter(int, "Maximum number of metadata servers this user can own"),
      'max_contents':      Parameter(int, "Maximum number of content servers this user can register"),
      'my_mdserver_ids':   Parameter([int], "Server IDs of servers owned by this user"),
      'sub_mdserver_ids':  Parameter([int], "Server IDs of servers this user is subscribed to"),
      'content_ids':       Parameter([int], "Content IDs of content servers owned by this user")
   }
   
   related_fields = {
      'my_mdserver_ids': [Parameter(int, "Server ID")],
      'sub_mdserver_ids': [Parameter(int, "Server ID")],
      'content_ids': [Parameter(int, "Content ID")]
   }
   
   user_content_view = 'user_contents'
   user_mdserver_view = 'user_mdservers'
   
   join_tables = ['user_mdserver', 'user_content', 'mdserver_user']
   
   add_mdserver    = Row.add_object(SMDS.mdserver.MDServer, 'user_mdserver')
   remove_mdserver = Row.remove_object(SMDS.mdserver.MDServer, 'user_mdserver')
   
   add_content     = Row.add_object(Content, 'user_content')
   remove_content  = Row.remove_object(Content, 'user_content')
   
   public_fieldnames = filter( lambda f: f not in ['password'], fields.keys())
   
   register_fieldnames = ['username','password','email']
   
   def public(self):
      ret = {}
      for f in self.public_fieldnames:
         ret[f] = self.get(f)
      
      return ret
   
   @staticmethod
   def refresh( api, u):
      user = Users( api, [u['user_id']] )[0]
      u.update( user )
      

class Users(Table):
   
   def __init__(self, api, user_filter = None, columns = None ):
      Table.__init__(self, api, User, columns)

      db_name = "view_users"
      
      sql = "SELECT %s FROM %s WHERE True" % \
         (", ".join(self.columns.keys()),db_name)

      if user_filter is not None:
         if isinstance(user_filter, (list, tuple, set)):
               # Separate the list into integers and strings
               ints = filter(lambda x: isinstance(x, (int, long)), user_filter)
               strs = filter(lambda x: isinstance(x, StringTypes), user_filter)
               user_filter = Filter(User.fields, {'user_id': ints, 'username': strs})
               sql += " AND (%s) %s" % user_filter.sql(api, "OR")
         elif isinstance(user_filter, dict):
               user_filter = Filter(User.fields, user_filter)
               sql += " AND (%s) %s" % user_filter.sql(api, "AND")
         elif isinstance (user_filter, StringTypes):
               user_filter = Filter(User.fields, {'username':[user_filter]})
               sql += " AND (%s) %s" % user_filter.sql(api, "AND")
         elif isinstance (user_filter, int):
               user_filter = Filter(User.fields, {'user_id':[user_filter]})
               sql += " AND (%s) %s" % user_filter.sql(api, "AND")
         else:
               raise MDInvalidArgument, "Wrong person filter %r"%user_filter

      self.selectall(sql)
