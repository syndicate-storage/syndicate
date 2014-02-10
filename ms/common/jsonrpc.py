#!/usr/bin/env python 

"""
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

# Modified from https://code.google.com/p/app-engine-starter/source/browse/trunk/lib/jsonrpc.py
# added support for positional and keyword arguments, and message signing and verifying

import json, inspect
import logging
import traceback
import uuid
import urllib2
import sys
import base64
import hashlib

import msconfig

try:
   import syndicate.client.common.log as Log
   log = Log.get_logger()
except:
   import logging as log

from msconfig import JSON_SYNDICATE_CALLING_CONVENTION_FLAG

VERSION = '2.0'
ERROR_MESSAGE = {
    -32700: 'Parse error',
    -32600: 'Invalid Request',
    -32601: 'Method not found',
    -32602: 'Invalid params',
    -32603: 'Internal error',
    -32400: 'Signature verification error'              # unofficial, Syndicate-specific
}

# ----------------------------------
def insert_syndicate_json( json_data, api_version, username, sig ):
   data = {
      "api_version": str(api_version)
   }
   
   if sig:
      data['signature'] = base64.b64encode( sig )
   
   if username:
      data['username'] = str(username)
   
   json_data['Syndicate'] = data


# ----------------------------------
def extract_syndicate_json( json_data, api_version ):
   if not json_data.has_key( 'Syndicate' ):
      log.error("No Syndicate data given")
      return None
   
   syndicate_data = json_data['Syndicate']
   
   if not syndicate_data.has_key( 'api_version' ):
      log.error("No API version")
      return None
   
   if syndicate_data['api_version'] != api_version:
      log.error("Invalid API version '%s'" % api_version)
      return None
   
   del json_data['Syndicate']
   
   if syndicate_data.has_key('signature'):
      syndicate_data['signature'] = base64.b64decode( syndicate_data['signature'] )
      
   return syndicate_data


# ----------------------------------
def json_stable_serialize( json_data ):
   # convert a dict into json, ensuring that key-values are serialized in a stable order
   if isinstance( json_data, list ) or isinstance( json_data, tuple ):
      json_serialized_list = []
      for json_element in json_data:
         json_serialized_list.append( json_stable_serialize( json_element ) )
      
      json_serialized_list.sort()
      return "[" + ", ".join( json_serialized_list ) + "]"
   
   elif isinstance( json_data, dict ):
      json_serialized_dict = {}
      for key in json_data.keys():
         json_serialized_dict[key] = json_stable_serialize( json_data[key] )
      
      key_order = [k for k in json_serialized_dict.keys()]
      key_order.sort()
      
      return "{" + ", ".join( ['"%s": %s' % (k, json_serialized_dict[k]) for k in key_order] ) + "}"
   
   elif isinstance( json_data, str ) or isinstance( json_data, unicode ):
      return '"' + json_data + '"'
   
   return '"' + str(json_data) + '"'


# ----------------------------------
def check_positional_params( params, arg_len, def_len ):
   good = True
   if len(params) + 1 < arg_len - def_len:
      good = False
   
   return good

# ----------------------------------
def is_error_response( result ):
   return 'error' in result
   
# ----------------------------------
class Server(object):
    response = None

    def __init__(self, obj, api_version, signer=None, verifier=None ):
        self.obj = obj
        self.api_version = api_version
        self.signer = signer
        self.verifier = verifier

    def error(self, id, code, data=None):
        error_value = {'code': code, 'message': ERROR_MESSAGE.get(code, 'Server error'), 'data': data}
        return self.result({'jsonrpc': VERSION, 'error': error_value, 'id': id})

    def result(self, result, method=None):
        # sign a single result.
        # if it's a list of results, then each element is already signed.
        if not isinstance(result, list):
            result_sig = None
            if self.signer:
               data_to_sign = json_stable_serialize( result )
               
               """
               sh = hashlib.sha1()
               sh.update( data_to_sign )
               json_hash = sh.hexdigest()
               
               print "to sign:\n\n%s\n\nHash: %s\n\n" % (data_to_sign, json_hash)
               """
               
               result_sig = self.signer( method, data_to_sign )
            
            insert_syndicate_json( result, self.api_version, None, result_sig )
         
        if self.response is None:
            return result
        else:
            if hasattr(self.response, 'headers'):
                self.response.headers['Content-Type'] = 'application/json'
            if hasattr(self.response, 'write'):
                self.response.write(json.dumps(result))
        
        return result
     
    # get the list of RPC UUIDs from a result
    def get_result_uuids( self, result ):
       if isinstance(result, list):
          ret = []
          for r in result:
             ret.append( r['id'] )
          
          return ret
       
       elif isinstance(result, dict):
          return [result['id']]
       
       else:
          return None
       

    def handle(self, json_text, response=None, data=None, **verifier_kw):
        self.response = response

        if not data:
            try:
                data = json.loads(json_text)
            except ValueError, e:
                return self.error(None, -32700)
             
        # batch calls
        if isinstance(data, list):
            # todo implement async batch
            batch_result = [self.handle(json_text, None, None, d) for d in data]
            self.response = response
            return self.result(batch_result)


        if 'id' in data:
            id = data['id']
        else:
            id = None
            
        # early check--we'll need to get the signature
        syndicate_data = extract_syndicate_json( data, self.api_version )
        if syndicate_data == None:
           return self.error(id, -32600)
        
        """
        # verify that we have a signature
        if self.verifier:
          if not syndicate_data.has_key( 'signature' ):
            log.error("No signature field")
            return self.error(id, -32400)
        """
        
        # get the rest of the fields...
        if data.get('jsonrpc') != '2.0':
            return self.error(id, -32600)

        if 'method' in data:
            method = data['method']
        else:
            return self.error(id, -32600)

        syndicate_calling_convention = False
        syndicate_method_args = []
        syndicate_method_kw = {}
        
        if 'params' in data:
            params = data['params']
            
            if isinstance( params, dict ):
               if params.has_key(JSON_SYNDICATE_CALLING_CONVENTION_FLAG) and params[JSON_SYNDICATE_CALLING_CONVENTION_FLAG]:
                  # params is {args, kw}
                  try:
                     syndicate_method_args = params['args']
                     syndicate_method_kw = params['kw']
                     syndicate_calling_convention = True
                  except:
                     pass
                  
        else:
            params = {}
        
        if method.startswith('_'):
            return self.error(id, -32601)
        try:
            method = getattr(self.obj, method)
        except AttributeError:
            return self.error(id, -32601)
        
        method_args = []
        method_kw = {}
        
        method_info = inspect.getargspec(method.__call__)
        arg_len = len(method_info.args)
        def_len = 0
        if method_info.defaults is not None:
            def_len = len(method_info.defaults)

        # Check if params is valid and remove extra params
        if not syndicate_calling_convention:
            named_params = not isinstance(params, list)
            invalid_params = False
            if arg_len > 1 and params is None:
               invalid_params = True
                  
            else:
               rc = check_positional_params( params, arg_len, def_len )
               if not rc:
                  invalid_params = True

            if invalid_params:
               return self.error(id, -32602)
            else:
               if named_params:
                  method_kw = params
               else:
                  method_args = params
        
        else:
           invalid_params = False
           
           rc = check_positional_params( syndicate_method_args, arg_len, def_len )
           if not rc:
               invalid_params = True
            
           
           if invalid_params:
              return self.error(id, -32602)
           else:
              method_kw = syndicate_method_kw
              method_args = syndicate_method_args
           
        if self.verifier:
            data_text = json_stable_serialize( data )
            
            """
            sh = hashlib.sha1()
            sh.update( data_text )
            json_hash = sh.hexdigest()
            
            print "to verify:\n\n%s\n\nHash: %s\n\n" % (data_text, json_hash)
            """
            
            valid = self.verifier( method, method_args, method_kw, data_text, syndicate_data, data, **verifier_kw )
            if not valid:
               log.error("Verifier failed")
               return self.error(id, -32400)
            
        try:
            result = method( *method_args, **method_kw )
        except Exception, e:
            log.error(sys.exc_info())
            traceback.print_exc()
            return self.error(id, -32603, e.message)

        if id is not None:
            return self.result({'result': result, 'id': id, 'jsonrpc': VERSION}, method)


# ----------------------------------
class Client(object):
    # NOTE: this class will not be used on the MS.
    # only used in client endpoints.
    
    def __init__(self, uri, api_version, username=None, password=None, signer=None, verifier=None, headers={}):
        self.uri = uri
        self.api_version = api_version
        self.headers = headers
        self.signer = signer
        self.verifier = verifier
        self.username = username
        self.password = password
        self.syndicate_data = None      # stores syndicate data from the last call

    def set_signer( self, signer ):
      self.signer = signer
    
    def set_verifier( self, verifier ):
       self.verifier = verifier
       
    def __getattr__(self, key):
        try:
            return object.__getattr__(self, key)
        except AttributeError:
            return self.dispatch(key)

    def default(self, *args, **kw):
        self.params = { JSON_SYNDICATE_CALLING_CONVENTION_FLAG: True, "args": args, "kw": kw }
        return self.request()

    def dispatch(self, key):
        self.method = key
        return self.default
     
    def request(self):
        # sanity check: need a signer OR a username/password combo
        if self.signer is None and (self.username is None or self.password is None):
           raise Exception("Need either an RPC signing callback or a username/password pair!")
        
        parameters = {
            'id': str(uuid.uuid4()),
            'method': self.method,
            'params': self.params,
            'jsonrpc': VERSION
        }
        
        self.syndicate_data = None
        
        sig = None
        if self.signer != None:
            # sign this message and include it in the authentication field
            parameters_text = json_stable_serialize( parameters )
            
            """
            sh = hashlib.sha1()
            sh.update( parameters_text )
            json_hash = sh.hexdigest()
            
            print "to sign:\n\n%s\n\nHash: %s\n\n" % (parameters_text, json_hash)
            """
            
            sig = self.signer( self.method, str(parameters_text) )
            
        
        insert_syndicate_json( parameters, self.api_version, self.username, sig )
        
        data = json.dumps(parameters)
        
        headers = {
            "Content-Type": "application/json"
        }
        
        response = None
        if self.username is not None and self.password is not None:
           # openid authentication!
           import syndicate.syndicate as c_syndicate
           
           rc, response = c_syndicate.openid_rpc( self.uri, self.username, self.password, "json", data )
        
           if rc != 0:
              log.error("MS OpenID RPC rc = %s" % rc)
              return None
        
        else:
           # public-key authentication!
           headers = dict(headers.items() + self.headers.items())
           req = urllib2.Request(self.uri, data, headers)

           response = urllib2.urlopen(req).read()
        
        try:
            result = json.loads(response)
        except:
            log.error("Invalid response: Body: '%s'" % (self.response))
            return None
        
        # check fields
        missing = []
        for field in ['id', 'jsonrpc' ]:
            if not result.has_key(field):
               missing.append( field )
        
        if len(missing) > 0:
           log.error("Missing fields: %s" % (",".join(missing)))
           return None

        # process syndicate data
        syndicate_data = extract_syndicate_json( result, self.api_version )
        if syndicate_data == None:
           log.error("No Syndicate data returned")
           return None
        
        # check signature
        if self.verifier:
            result_text = json_stable_serialize( result )
            
            """
            sh = hashlib.sha1()
            sh.update( result_text )
            json_hash = sh.hexdigest()
            
            print "to verify:\n\n%s\n\nHash: %s\n\n" % (result_text, json_hash)
            """
            
            valid = self.verifier( self.method, self.params['args'], self.params['kw'], result_text, syndicate_data, result )
            
            if not valid:
               if is_error_response( result ):
                  log.warning("Server did not sign error reply")
               
               else:
                  log.error("Signature verification failure")
                  return None
         
         
        if is_error_response( result ):   
            data = None
            if "data" in result['error']:
               data = result['error']['data']
            raise Exception('%s Code: %s, Data: %s' % (result['error']['message'], result['error']['code'], data))
         
         
        if parameters['id'] == result['id'] and 'result' in result:
           
            # store syndicate data for later
            self.syndicate_data = syndicate_data
            return result['result']
        else:
            return None
         
         