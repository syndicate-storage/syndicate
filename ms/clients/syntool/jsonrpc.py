# https://code.google.com/p/app-engine-starter/source/browse/trunk/lib/jsonrpc.py

import json, inspect
import logging
import traceback
import uuid
import urllib2
import sys

__author__ = 'faisal'

"""
Client Usage: (Passing headers help with authorization cookies)
client = Client('http://path/to/rpc', webapp2.request.headers)
client.method_name(args)
"""

VERSION = '2.0'
ERROR_MESSAGE = {
    -32700: 'Parse error',
    -32600: 'Invalid Request',
    -32601: 'Method not found',
    -32602: 'Invalid params',
    -32603: 'Internal error'
}


class Client(object):

    def __init__(self, uri, headers={}):
        self.uri = uri
        self.headers = headers

    def __getattr__(self, key):
        try:
            return object.__getattr__(self, key)
        except AttributeError:
            return self.dispatch(key)

    def default(self, *args, **kw):
        if len(kw) > 0:
            self.params = kw
        elif len(args) > 0:
            self.params = args
        else:
            self.params = {}

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
            return None

        if 'error' in result:
            data = None
            if "data" in result['error']:
               data = result['error']['data']
            raise Exception('%s Code: %s, Data: %s' % (result['error']['message'], result['error']['code'], data))
        if parameters['id'] == result['id'] and 'result' in result:
            return result['result']
        else:
            return None
         
         