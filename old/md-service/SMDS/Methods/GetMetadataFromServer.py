#!/usr/bin/python

from SMDS.method import Method
from SMDS.mdserver import *
from SMDS.user import User
from SMDS.parameter import *
from SMDS.auth import Auth
from SMDS.faults import *

import urllib2

class GetMetadataFromServer( Method ):
   """
   Get metadata from a running metadata server, given its URL, ID, or (hostname, portnum) tuple
   """
   
   accepts = [
         Auth(),
         Mixed(Mixed(MDServer.fields['host'], MDServer.fields['portnum']),
               Parameter(str, "Metadata server URL"),
               Parameter(int, "Metadata server ID")
               )
   ]
   roles = ['user', 'admin']
   returns = Parameter([str], "List of metadata entries as strings")
   
   def call(self, auth, host_portnum_or_url_or_id):
      assert self.caller is not None
      
      md_server_url = None
      
      if isinstance(host_portnum_or_url_or_id, list) or isinstance(host_portnum_or_url_or_id, tuple):
         md_server_url = host_portnum_or_url_or_id[0] + '/' + str(host_portnum_or_url_or_id[1])
      elif isinstance(host_portnum_or_url_or_id, str):
         md_server_url = host_portnum_or_url_or_id
      else:
         # look up metadata server and get its URL
         mdservers = MDServers( {'server_id': host_portnum_or_url_or_id} )
         if len(mdservers) <= 0:
            # not found
            raise MDObjectNotFound( 'mdserver', host_portnum_or_url_or_id )
      
      # perform a GET on the url, with the caller's credentials
      auth_header = urllib2.HTTPBasicAuthHandler()
      auth_header.add_password( realm=None, uri=md_server_url, user=auth.get( 'Username' ), passwd=auth.get( 'AuthString' ) )
      opener = urllib2.build_opener( auth_header )
      
      urllib2.install_opener( opener )
      
      metadata = None
      try:
         md_handle = urllib2.urlopen( md_server_url )
         metadata = md_handle.read()
         md_handle.close()
      except Exception, e:
         raise MDMethodFailed( "Could not open '%s'" % (md_server_url), e )
      
      return metadata.split("\n")[:-1]