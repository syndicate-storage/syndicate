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

log = Log.get_logger()

CONFIG_DIR = os.path.expanduser( "~/.syndicate" )
CONFIG_FILENAME = os.path.join(CONFIG_DIR, "syndicate.conf")

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
def build_parser( progname ):
   parser = argparse.ArgumentParser( prog=progname, description="Syndicate Control Tool" )
   
   for (config_option, (short_option, nargs, config_help)) in CONFIG_OPTIONS.items():
      if not isinstance(nargs, int) or nargs >= 1:
         if short_option:
            # short option means 'typical' argument
            parser.add_argument( "--" + config_option, short_option, metavar=config_option, nargs=nargs, help=config_help)
         else:
            # no short option (no option in general) means accumulate
            parser.add_argument( config_option, metavar=config_option, type=str, nargs=nargs, help=config_help)
      else:
         # no argument, but mark its existence
         parser.add_argument( "--" + config_option, short_option, action="store_true", help=config_help)
   
   return parser

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
   
   config = ConfigParser.SafeConfigParser()
   config_fd = StringIO.StringIO( config_str )
   config_fd.seek( 0 )
   
   try:
      config.readfp( config_fd )
   except Exception, e:
      log.exception( e )
      return None
   
   # verify that all attributes are given
   missing = []
   config_opts = config.options("syndicate")
   for opt in config_opts:
      if config.get("syndicate", opt) == None:
         missing.append( opt )
         
   if missing:
      log.error("Missing options: %s" % (",".join(missing)) )
      return None
   
   ret = {}
   ret["_in_argv"] = []
   ret["_in_config"] = []
   
   # convert to dictionary, merging in argv opts
   for arg_opt in CONFIG_OPTIONS.keys():
      if hasattr(opts, arg_opt) and getattr(opts, arg_opt) != None:
         ret[arg_opt] = getattr(opts, arg_opt)
         
         # force singleton...
         if isinstance(ret[arg_opt], list) and len(ret[arg_opt]) == 1 and CONFIG_OPTIONS[arg_opt][1] == 1:
            ret[arg_opt] = ret[arg_opt][0]
            
         ret["_in_argv"].append( arg_opt )
      
      elif config.has_option("syndicate", arg_opt):
         ret[arg_opt] = config.get("syndicate", arg_opt)
         
         ret["_in_config"].append( arg_opt )
         
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
   
   config['_in_argv'] = []
   config['_in_config'] = []
   
   return 

   
# -------------------
def usage( progname ):
   parser = build_parser( progname )
   parser.print_help()
   sys.exit(1)
   