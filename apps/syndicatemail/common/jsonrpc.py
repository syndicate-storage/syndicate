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
# added support for positional and keyword arguments

import json, inspect
import logging
import traceback
import uuid
import urllib2
import sys
import base64

import syndicate.client.common.log as Log
log = Log.get_logger()

VERSION = '1.0'
ERROR_MESSAGE = {
    -32700: 'Parse error',
    -32600: 'Invalid Request',
    -32601: 'Method not found',
    -32602: 'Invalid params',
    -32603: 'Internal error',
}

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

    def __init__(self, obj, api_version=VERSION, signer=None, verifier=None):
        self.obj = obj
        self.api_version = api_version
        self.signer = signer
        self.verifier = verifier

    def error(self, id, code, data=None):
        error_value = {'code': code, 'message': ERROR_MESSAGE.get(code, 'Server error'), 'data': data}
        return self.result({'jsonrpc': VERSION, 'error': error_value, 'id': id})

    def result(self, result, method=None):
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
            
        
        # get the rest of the fields...
        if data.get('jsonrpc') != VERSION:
            return self.error(id, -32600)

        if 'method' in data:
            method = data['method']
        else:
            return self.error(id, -32600)

        method_args = []
        method_kw = {}
        
        if 'params' in data:
            params = data['params']
            
            if isinstance( params, dict ):
               # params is {args, kw}?
               try:
                  method_args = params['args']
                  method_kw = params['kw']
               except:
                  return self.error(id, -32600)
               
            else:
               return self.error(id, -32600)
                  
        else:
            params = {}
        
        if method.startswith('_'):
            return self.error(id, -32601)
        try:
            method = getattr(self.obj, method)
        except AttributeError:
            return self.error(id, -32601)
        
        method_info = inspect.getargspec(method)
        arg_len = len(method_info.args)
        def_len = 0
        
        if method_info.defaults is not None:
            def_len = len(method_info.defaults)

        rc = check_positional_params( method_args, arg_len, def_len )
        if not rc:
           return self.error(id, -32602)
        
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

    def __init__(self, uri, headers={}):
        self.uri = uri
        self.api_version = VERSION
        self.headers = headers

    def __getattr__(self, key):
        try:
            return object.__getattr__(self, key)
        except AttributeError:
            return self.dispatch(key)

    def default(self, *args, **kw):
        self.params = { "args": args, "kw": kw }
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
   
        if is_error_response( result ):   
            data = None
            if "data" in result['error']:
               data = result['error']['data']
            raise Exception('%s Code: %s, Data: %s' % (result['error']['message'], result['error']['code'], data))
         
        if parameters['id'] == result['id'] and 'result' in result:
            return result['result']
        else:
            return None
         
         