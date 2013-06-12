#!/usr/bin/python

from SMDS.method import Method
from SMDS.mdserver import *
from SMDS.user import *
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *

import urllib2
import uuid

class AddMetadataServer( Method ):
   """
   Add a metadata server.  Does not start it, but adds an entry for it in our database.
   The owner of this metadata server will be the user that calls this method.
   """
   
   accepts = [
         Auth(),
         dict([(n,MDServer.fields[n]) for n in set(MDServer.fields.keys()).difference(set(['owner','status','user_ids']))])
   ]
   roles = ['admin','user']
   returns = Parameter(int, "The metadata server's ID (positive number) if successful")
   
   def call(self, auth, mdserver_fields):
      assert self.caller is not None
      
      # look up the user ID
      users = Users( self.api, {'username': auth['Username']} )
      user = users[0]
      
      # how many metadata servers does this user have?
      num_md_servers = len(user['my_mdserver_ids'])
      if num_md_servers > user['max_mdservers']:
         raise MDResourceExceeded('User(%s)' % user['username'], 'metadata server')
      
      mdserver_fields['owner'] = user['user_id']
      mdserver_fields['status']= 'stopped'
      
      # url-encode the name, if it exists.  Otherwise use a UUID
      if mdserver_fields.get('name'):
         mdserver_fields['name'] = urllib2.quote( mdserver_fields['name'] )
      else:
         mdserver_fields['name'] = urllib2.quote( uuid.uuid4().hex )
         
      # authenticate read/write by default
      if not mdserver_fields.get('auth_read'):
         mdserver_fields['auth_read'] = True
      
      if not mdserver_fields.get('auth_write'):
         mdserver_fields['auth_write'] = True
      
      # get a hostname, if one was not given
      # if we don't have a host, then pick one
      if not mdserver_fields.get('host'):
         host = self.api.next_server_host()
         mdserver_fields['host'] = host
      
      md = MDServer( self.api, mdserver_fields )
      md.sync()
      
      # update join tables
      user.add_mdserver( md )
      md.add_user( user )
      server_id = md['server_id']
      
      # reload
      md = MDServers( self.api, [server_id])[0]
      
      rc = md.create_server()
      if rc != 1:
         raise MDMethodFailed( "md.create_server()", "Could not create metadata server, rc = %s" % rc )
      
      return md['server_id']
      