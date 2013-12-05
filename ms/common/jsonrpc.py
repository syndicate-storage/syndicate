#!/usr/bin/env python 
# Modified from https://code.google.com/p/app-engine-starter/source/browse/trunk/lib/jsonrpc.py


import json, inspect
import logging
import traceback
import uuid
import urllib2
import sys
import base64
import msconfig

from Crypto.Hash import MD5

try:
   import syndicate.log as Log
   log = Log.log
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
    -32400: 'Signature verification error'
}

# ----------------------------------
def insert_syndicate_json( json_data, key_type, key_name, api_version, sig ):
   data = {
      "api_version": str(api_version)
   }
   
   if sig:
      data['signature'] = base64.b64encode( sig )
   
   if key_type:
      data['key_type'] = str(key_type)
   
   if key_name:
      data['key_name'] = str(key_name)
   
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
      
      return "{" + ", ".join( ["%s: %s" % (k, json_serialized_dict[k]) for k in key_order] ) + "}"
   
   elif isinstance( json_data, str ) or isinstance( json_data, unicode ):
      return '"' + json_data + '"'
   
   return str(json_data)


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

    def __init__(self, obj, api_version, signer=None, verifier=None):
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
               result_sig = self.signer( method, data_to_sign )
            
            insert_syndicate_json( result, None, None, self.api_version, result_sig )
         
        if self.response is None:
            return result
        else:
            if hasattr(self.response, 'headers'):
                self.response.headers['Content-Type'] = 'application/json'
            if hasattr(self.response, 'write'):
                self.response.write(json.dumps(result))

    def handle(self, json_text, response=None, data=None):
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
        
        # verify that we have a signature
        if self.verifier:
          if not syndicate_data.has_key( 'signature' ):
            log.error("No signature field")
            return self.error(id, -32400)
        
        
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
            valid = self.verifier( method, method_args, method_kw, data_text, syndicate_data, data )
            if not valid:
               log.error("Verifier failed")
               return self.error(id, -32400)
            
        try:
            result = method( *method_args, **method_kw )
        except Exception, e:
            logging.error(sys.exc_info())
            traceback.print_exc()
            return self.error(id, -32603, e.message)

        if id is not None:
            return self.result({'result': result, 'id': id, 'jsonrpc': VERSION}, method)


# ----------------------------------
class Client(object):

    def __init__(self, uri, api_version, signer=None, verifier=None, headers={}):
        self.uri = uri
        self.api_version = api_version
        self.headers = headers
        self.signer = signer
        self.verifier = verifier
        self.key_type = None 
        self.key_name = None

    def set_key_info( self, key_type, key_name ):
       self.key_type = key_type
       self.key_name = key_name
       
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
        parameters = {
            'id': str(uuid.uuid4()),
            'method': self.method,
            'params': self.params,
            'jsonrpc': VERSION
        }
        
        sig = None
        if self.signer != None:
            # sign this message and include it in the authentication field
            parameters_text = json_stable_serialize( parameters )
            sig = self.signer( self.method, str(parameters_text) )
            
        
        insert_syndicate_json( parameters, self.key_type, self.key_name, self.api_version, sig )
        
        data = json.dumps(parameters)
        
        headers = {
            "Content-Type": "application/json"
        }
        
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
            can_verify = True
            if 'signature' not in syndicate_data:
               
               # result or error?
               if is_error_response( result ):
                  log.warning("Server did not sign reply")
                  can_verify = False
               else:
                  log.error("Server did not sign reply")
                  return None 
            
            if can_verify:
               result_text = json_stable_serialize( result )
               
               valid = self.verifier( self.method, self.params['args'], self.params['kw'], result_text, syndicate_data, result )
               
               if not valid:
                  log.error("Signature verification failure")
                  return None
           
        if is_error_response( result ):   
            data = None
            if "data" in result['error']:
               data = result['error']['data']
            raise Exception('%s Code: %s, Data: %s' % (result['error']['message'], result['error']['code'], data))
         
        if parameters['id'] == result['id'] and 'result' in result:
            return result['result']
        else:
            return None
         
         