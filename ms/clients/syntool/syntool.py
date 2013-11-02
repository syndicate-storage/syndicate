#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import os
import json

import sys

import base64

import common 
import conf 
import jsonrpc

import pprint 

CONFIG = None

log = common.log

# -------------------
def make_rpc_client( ms_url, username, password ):
   auth_header = "Basic=%s" % base64.b64encode( "%s:%s" % (username, password) )
   
   headers = {"Authorization": auth_header}
   
   json_client = jsonrpc.Client( ms_url, headers )
   
   if not ms_url.lower().startswith("https://"):
      log.warning("MS URL %s is NOT secure!" % ms_url )
   
   return json_client


# -------------------
def read_volume( client, volume_name_or_id ):
   try:
      volume_json = client.read_volume( volume_name_or_id )
      return volume_json 
   
   except Exception, e:
      log.exception( e )
      return None 
   
   

   
if __name__ == "__main__":
   client = make_rpc_client( "http://localhost:8080/jsonrpc", "foo", "sniff" )
   
   result = client.list_replica_gateways_by_host( "localhost" )
   
   pp = pprint.PrettyPrinter()
   
   pp.pprint( result )