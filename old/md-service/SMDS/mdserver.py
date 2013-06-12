#!/usr/bin/python

from SMDS.db import Row, Table
import SMDS.logger as logger
from SMDS.parameter import Parameter, Mixed
from SMDS.filter import Filter

from types import *
import random

class MDServer(Row):
   
   table_name = 'mdservers'
   primary_key = 'server_id'
   join_tables = ['user_mdserver', 'mdserver_user']
   
   fields = {
      'server_id':      Parameter(long, "Server ID"),
      'name':           Parameter(str, "Owner-given name of this metadata server (will be URL-encoded)"),
      'host':           Parameter(str, "Name of the host that hosts this metadata server"),
      'portnum':        Parameter(int, "Port on which this metadata server listens", min=1025, max=65534),
      'status':         Parameter(str, "Desired status of this metadata server"),
      'auth_read':      Parameter(bool, "Must a user authenticate with the server to read from it?"),
      'auth_write':     Parameter(bool, "Must a user authenticate with the server to write to it?"),
      'owner':          Parameter(int, "User ID of the user who created and controls this metadata server"),
      'user_ids':       Parameter([int], "User IDs of users subscribed to this metadata server")
   }
   
   related_fields = {
      'user_ids': [Parameter(int, "Subscribed user ID")]
   }
   
   def __init__(self, api, fields = {}):
      super(MDServer, self).__init__(api, fields)
      import SMDS.user
      self._add_user = Row.add_object(SMDS.user.User, 'mdserver_user')
      self._remove_user = Row.remove_object(SMDS.user.User, 'mdserver_user')
      
   def add_user( self, obj, commit = True ):
      self._add_user( self, obj, commit = commit )
   
   def remove_user( self, obj, commit = True ):
      self._remove_user( self, obj, commit = commit )
   
   @classmethod
   def refresh( api, m):
      md = MDServers( api, [m['server_id']] )[0]
      m.update( md )
      
   def remove_users( self, user_ids ):
      import SMDS.user 
      
      users = SMDS.user.Users( self.api, user_ids )
      if users:
         for user in users:
            self.remove_user( user, commit = False )
      
      self.commit()
      
   
   def create_server(self):
      # signal the controller on this metadata server's host to create this server
      self['status'] = 'stopped'
      server = self.api.connect_mdctl( self['host'] )
      
      import SMDS.user
      
      # get our users
      if self.get('user_ids') == None:
         self['user_ids'] = []
      
      user_ids = self['user_ids']
      users = []
      if user_ids:
         users = SMDS.user.Users( self.api, user_ids )
      
      logger.info("Creating metadata server '%s' with users '%s'" % (self['name'], [user['username'] for user in users]))
      
      rc = 0
      try:
         as_dict = {}
         as_dict.update( self )
         
         user_dicts = []
         for user in users:
            user_dict = {}
            user_dict.update( user )
            user_dicts.append( user_dict )
            
         rc = server.create_server( as_dict, user_dicts )
      except Exception, e:
         logger.exception( e, "Could not restart metadata server")
         rc = -500
         
      if rc == 1:
         self.sync()
      else:
         logger.error("Could not create metadata server, rc = %s" % rc)
         
      return rc
      
      
   
   def destroy_server(self):
      # signal the controller on this metadata server's host to destroy this server
      self['status'] = 'stopped'
      server = self.api.connect_mdctl( self['host'] )
      
      logger.info("Destroying metadata server '%s'" % self['name'])
      
      rc = 0
      try:
         as_dict = {}
         as_dict.update( self )
         rc = server.destroy_server( as_dict )
      except Exception, e:
         logger.exception( e, "Could not restart metadata server")
         rc = -500
      
      if rc == 1:
         self.sync()
      else:
         logger.error("Could not destroy metadata server, rc = %s" % rc)
         
      return rc
      
      
   def stop_server(self):
      # signal the controller on this metadata server's host to stop this metadata server
      old_status = self['status']
      self['status'] = 'stopped'
      
      server = self.api.connect_mdctl( self['host'] )
      
      logger.info("Stopping metadata server '%s'" % self['name'])
      
      rc = 0
      try:
         as_dict = {}
         as_dict.update( self )
         rc = server.stop_server( as_dict )
         if rc == 0:
            logger.warn( "Server '%s' was not running" % self['name'])
            rc = 1      # it's OK if it wasn't running
            
      except Exception, e:
         logger.exception( e, "Could not stop metadata server")
         rc = -500;
      
      if rc == 1:
         self.sync()
      else:
         logger.error("Could not stop metadata server, rc = %s" % rc)
         self['status'] = old_status
         
      return 1
   
   
   def start_server(self):
      # signal the controller on this metadata server's host to start this metadata server
      old_status = self['status']
      self['status'] = 'running'
      
      server = self.api.connect_mdctl( self['host'] )
      
      logger.info("Starting metadata server '%s'" % self['name'])
      
      ret = None
      try:
         as_dict = {}
         as_dict.update( self )
         ret = server.start_server( as_dict )
      except Exception, e:
         logger.exception( e, "Could not start metadata server")
         ret = None
      
      if ret != None:
         self.sync()
      else:
         logger.error("Could not start metadata server, rc = %s" % ret)
         self['status'] = old_status
         
      return ret
   
   
   def restart_server(self, force_start=True ):
      # signal the controller on this metadata server's host to restart this metadata server.
      # the state of the server (running/stopped) should not change, except on error
      
      server = self.api.connect_mdctl( self['host'] )
      
      import SMDS.user 
      
      # get our users
      user_ids = self['user_ids']
      users = []
      if user_ids:
         users = SMDS.user.Users( self.api, user_ids )
      
      
      logger.info("Restarting metadata server '%s' with users '%s'" % (self['name'], [user['username'] for user in users]))
      
      rc = 0
      try:
         as_dict = {}
         as_dict.update( self )
         
         user_dicts = []
         for user in users:
            user_dict = {}
            user_dict.update( user )
            user_dicts.append( user_dict )
            
         rc = server.restart_server( as_dict, user_dicts, force_start )
      except Exception, e:
         logger.exception( e, "Could not restart metadata server")
         rc = -500
         self['status'] = 'stopped'
         self.sync()
      
      if rc != 1:
         logger.error("Could not restart metadata server, rc = %s" % rc)
      
      return rc

      


class MDServers(Table):
   
   def __init__(self, api, md_filter = None, columns = None ):
      Table.__init__(self, api, MDServer, columns)

      db_name = "view_mdservers"
      
      sql = "SELECT %s FROM %s WHERE True" % \
         (", ".join(self.columns.keys()),db_name)

      if md_filter is not None:
         if isinstance(md_filter, (list, tuple, set)):
               # Separate the list into integers and strings
               ints = filter(lambda x: isinstance(x, (int, long)), md_filter)
               strs = filter(lambda x: isinstance(x, StringTypes), md_filter)
               md_filter = Filter(MDServer.fields, {'server_id': ints, 'host': strs})
               sql += " AND (%s) %s" % md_filter.sql(api, "OR")
         elif isinstance(md_filter, dict):
               md_filter = Filter(MDServer.fields, md_filter)
               sql += " AND (%s) %s" % md_filter.sql(api, "AND")
         elif isinstance (md_filter, int):
               md_filter = Filter(MDServer.fields, {'server_id':[md_filter]})
               sql += " AND (%s) %s" % md_filter.sql(api, "AND")
         else:
               raise MDInvalidArgument, "Wrong metadata server filter %r"%md_filter

      self.selectall(sql)
