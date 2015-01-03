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

import ConfigParser

import os
import argparse
import sys
import StringIO
import traceback

import syndicate.client.common.api as api
import syndicate.client.common.object_stub as object_stub
import syndicate.client.common.log as Log

import syndicate.util.config as modconf

log = Log.get_logger()

CONFIG_DIR = os.path.expanduser( "~/.syndicate" )
CONFIG_FILENAME = os.path.join(CONFIG_DIR, "syndicate.conf")

CONFIG_DESCRIPTION = "Syndicate Administration Tool"

KEY_DIR_NAMES = {
   "volume": "volume_keys",
   "user": "user_keys",
   "gateway": "gateway_keys",
   "syndicate": "syndicate_keys"
}

CONFIG_DEFAULTS = {
   "syndicate_port": 443,
   'trust_public_key': False,
   'no_verify_result': False,
   'debug': False,
   'no_tls': False
}

CONFIG_OPTIONS = {
   "syndicate_host":    ("-s", 1, "Hostname of your Syndicate instance."),
   "syndicate_port":    ("-p", 1, "Port number your Syndicate instance listens on (default=%s)." % CONFIG_DEFAULTS['syndicate_port']),
   "no_tls":            ("-T", 0, "If set, do not securely connect (via TLS) to Syndicate.  INSECURE!"),
   "user_id":           ("-u", 1, "The ID of the Syndicate user to run as."),
   "volume_keys":       ("-V", 1, "Path to the directory where you store your Volume's public keys."),
   "gateway_keys":      ("-G", 1, "Path to the directory where you store your Gateway's private keys."),
   "user_keys":         ("-K", 1, "Path to the directory where you store your User's private keys."),
   "syndicate_keys":    ("-S", 1, "Path to the directory where you store your Syndicate public keys."),
   "config":            ("-c", 1, "Path to your config file (default is %s)." % CONFIG_FILENAME),
   "debug":             ("-d", 0, "Verbose debugging output"),
   "trust_public_key":  ("-t", 0, "If set, automatically trust the Syndicate public key if it is not yet trusted."),
   "no_verify_result":  ("-n", 0, "If set, do not cryptographically verify data returned from Syndicate.  INSECURE!"),
   "url":               ("-l", 1, "URL to your Syndicate instance (overrides syndicate_host, syndicate_port, and no_verify_result)"),
   "params":            (None, "+", "Method name, followed by parameters (positional and keyword supported)."),
   "password":          ("-P", 1, "Syndicate OpenID password.  If not supplied, it will be prompted."),
   "gateway_password":  ("-g", 1, "Syndicate gateway private key password (for create_gateway).  If not supplied, it will be prompted.")
}

# -------------------
class ArgLib(object):
   def __init__(self):
      pass


# -------------------
def default_syndicate_port( syndicate_host, no_tls ):
   default_port = 80
   if no_tls:
      default_port = 443
   
   return default_port

# -------------------
def make_ms_url( syndicate_host, syndicate_port, no_tls, urlpath="API/" ):
   scheme = "https://"
   default_port = 80
   
   if no_tls:
      default_port = 443
      scheme = "http://"
      
   if syndicate_port != default_port:
      return scheme + os.path.join( syndicate_host.strip("/") + ":%s" % syndicate_port, urlpath )
   else:
      return scheme + os.path.join( syndicate_host.strip("/"), urlpath )


# -------------------
def make_syndicate_pubkey_name( syndicate_host, syndicate_port, no_tls ):
   default_port = 80
   
   if no_tls:
      default_port = 443
      
   if syndicate_port != default_port:
      return syndicate_host.strip("/") + (":%s" % syndicate_port) + ".pub"
   else:
      return syndicate_host.strip("/") + ".pub"
   
   
   
# -------------------   
def make_config_str( config_data ):
   return "[syndicate]\n" + "\n".join( ["%s=%s" % (config_key, config_value) for (config_key, config_value) in config_data.items()] )

# -------------------
def build_config( config ):
   global CONFIG_OPTIONS 
   
   # build up our config
   config.add_section("syndicate")
   
   for config_option in CONFIG_OPTIONS:
      config.set("syndicate", config_option, None)
   
   return config

# -------------------
def object_key_path( config, key_type, object_id, public=False, no_suffix=False ):
   suffix = ".pkey"
   if public:
      suffix = ".pub"
      
   if no_suffix:
      suffix = ""
      
   key_dir = config.get( KEY_DIR_NAMES.get( key_type, None ) )
   if key_dir == None:
      raise Exception("Could not get key directory for '%s'" % key_type)
   return os.path.join( key_dir, str(object_id) + suffix )


# -------------------
def parse_args( method_name, args, kw, lib ):
   method = api.get_method( method_name )
   try:
      if method.parse_args:
         args, kw, extras = method.parse_args( method.argspec, args, kw, lib )
         return args, kw, extras
      else:
         return args, kw, {}
   except Exception, e:
      traceback.print_exc()
      raise Exception("Failed to parse method arguments")
      
      
# -------------------
def validate_args( method_name, args, kw ):
   method = api.get_method( method_name )
   
   arg_len = len(method.argspec.args)
   def_len = 0
   
   if method.argspec.defaults is not None:
      def_len = len(method.argspec.defaults)
   
   if len(args) != arg_len - def_len:
      raise Exception("Method '%s' expects %s arguments; got %s" % (method_name, arg_len - def_len, len(args)))
   
   return True


# -------------------
def extend_paths( config, base_dir ):
   # extend all paths
   for (_, keydir) in KEY_DIR_NAMES.items():
      path = config.get( keydir, None )
      if path == None:
         path = keydir
      
      if not os.path.isabs( path ):
         # not absolute, so append with base dir 
         path = os.path.join( base_dir, path )
         config[keydir] = path


# -------------------
def load_config( config_path, config_str, opts ):
   
   ret = modconf.load_config( config_str, opts, "syndicate", CONFIG_OPTIONS )
         
   conf_dir = os.path.dirname( config_path )
   extend_paths( ret, conf_dir )
   
   if ret['debug']:
      Log.set_log_level( "DEBUG" )

   else:
      Log.set_log_level( "WARNING" )
   
   return ret


# -------------------
def fill_defaults( config ):
   
   global KEY_DIR_NAMES, CONFIG_DEFAULTS
   
   config.update( CONFIG_DEFAULTS )
   
   extend_paths( config, CONFIG_DIR )
   
   modconf.defaults( config )
   
   return 

   
# -------------------
def usage( progname ):
   parser = modconf.build_parser( progname, CONFIG_DESCRIPTION )
   parser.print_help()
   sys.exit(1)
   