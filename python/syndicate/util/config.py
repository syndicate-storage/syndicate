#!/usr/bin/env python

"""
   Copyright 2014 The Trustees of Princeton University

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
import argparse
import logging 

logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger()

CONFIG_DIR = os.path.expanduser( "~/.syndicate" )
CONFIG_FILENAME = os.path.join(CONFIG_DIR, "syndicate.conf")

CONFIG_DESCRIPTION = "Syndicate Administration Tool"

OBJECT_DIR_NAMES = {
   "volume": "volumes",
   "user": "users",
   "gateway": "gateways",
   "syndicate": "syndicate"
}

CONFIG_DEFAULTS = {
   "MS_url": "http://127.0.0.1:8080",
   'trust_public_key': False,
   'debug': False
}

# 'syndicate' section of the config file
CONFIG_OPTIONS = {
   "MS_url":            ("-m", 1, "URL to your Syndicate MS"),
   "username":          ("-u", 1, "The ID of the Syndicate user to run as."),
   "volume_keys":       ("-V", 1, "Path to the directory holding the Volume public keys."),
   "gateway_keys":      ("-G", 1, "Path to the directory holding the Gateway private keys."),
   "user_keys":         ("-K", 1, "Path to the directory holding the User private keys."),
   "syndicate_keys":    ("-S", 1, "Path to the directory holding the Syndicate public keys."),
   "helpers":           ("-l", 1, "Path to the directory holding the Syndicate helper programs."),
   "config":            ("-c", 1, "Path to your config file (default is %s)." % CONFIG_FILENAME),
   "debug":             ("-d", 0, "Verbose debugging output"),
   "trust_public_key":  ("-t", 0, "If set, automatically trust the Syndicate public key if it is not yet trusted."),
   "params":            (None, "+", "Method name, followed by parameters (positional and keyword supported)."),
}


# 'helpers' section of the config file 
HELPER_OPTIONS = {
   "fetch_user_cert":       (None, 0, "Program to fetch a user's certificate"),
   "fetch_gateway_cert":    (None, 0, "Program to fetch a gateway's certificate"),
   "fetch_volume_cert":     (None, 0, "Program to fetch a volume's certificate"),
   "fetch_cert_bundle":     (None, 0, "Program to fetch a volume's certificate bundle"),
   "fetch_driver":          (None, 0, "Program to fetch a gateway's driver"),
   "fetch_syndicate_pubkey":   (None, 0, "Program to fetch a Syndicate instance's signing key"),
   "validate_user_cert":    (None, 0, "Program to validate and authenticate a user's certificate")
}

class ArgLib(object):
   """
   Generic object to store temporary 
   data between argument pre- and post-processing.
   """
   def __init__(self):
      pass


def serialize_config( config_data ):
   """
   Generate a .ini-formatted configuration string to e.g. save to disk.
   """
   return "[syndicate]\n" + "\n".join( ["%s=%s" % (config_key, config_value) for (config_key, config_value) in config_data.items()] )


def load_config( config_path, config_str, opts, config_header, config_options ):
   """
   Load configuration options from an ini-formatted string and from parsed command-line options.
   Prefer command-line options to config str options.
   
   Return a dict with the data.
   """
   
   config = None 
   
   if config_str:
      config = ConfigParser.SafeConfigParser()
      config_fd = StringIO.StringIO( config_str )
      config_fd.seek( 0 )
      
      try:
         config.readfp( config_fd )
      except Exception, e:
         log.exception( e )
         return None
   
   ret = {}
   ret["_in_argv"] = []
   ret["_in_config"] = []
   
   # convert to dictionary, merging in argv opts
   for arg_opt in config_options.keys():
      if hasattr(opts, arg_opt) and getattr(opts, arg_opt) != None:
         ret[arg_opt] = getattr(opts, arg_opt)
         
         # force singleton...
         if isinstance(ret[arg_opt], list) and len(ret[arg_opt]) == 1 and config_options[arg_opt][1] == 1:
            ret[arg_opt] = ret[arg_opt][0]
         
         ret["_in_argv"].append( arg_opt )
      
      elif config is not None and config.has_option( config_header, arg_opt):
         ret[arg_opt] = config.get( config_header, arg_opt)
         
         ret["_in_config"].append( arg_opt )
   
   conf_dir = os.path.dirname( config_path )
   extend_key_paths( ret, conf_dir )
   return ret


def build_parser( progname, description, config_options ):
   """
   Make an argument parser for the program, given the name, description, and list of configuraiton options.
   """
   parser = argparse.ArgumentParser( prog=progname, description=description )
   
   for (config_option, (short_option, nargs, config_help)) in config_options.items():
      if not isinstance(nargs, int) or nargs >= 1:
         if short_option:
            # short option means 'typical' argument
            parser.add_argument( "--" + config_option, short_option, metavar=config_option, nargs=nargs, help=config_help)
         else:
            # no short option (no option in general) means accumulate
            parser.add_argument( config_option, metavar=config_option, type=str, nargs=nargs, help=config_help)
      else:
         if nargs >= 1:
            parser.add_argument( config_option, metavar=config_option, type=str, nargs=nargs, help=config_help)
         
         else:
            # no argument, but mark its existence
            parser.add_argument( "--" + config_option, short_option, action="store_true", help=config_help)
   
   return parser


def build_config( argv, description, config_header, config_options, conf_validator=None, opt_handlers={}, config_opt=None, allow_none=False ):
   """
   Construct and populate a config dict, using argv, a program description, a designated ini header that contains 
   the requested fields, a description of the config fields.
   
   Return the dict with the options loaded.
   """
   parser = build_parser( argv[0], description, config_options )
   opts = parser.parse_args( argv[1:] )
   
   for opt, opt_cb in opt_handlers.items():
      if config_options.has_key( opt ) and hasattr( opts, opt ):
         # generate this option argument
         result = opt_cb( getattr(opts, opt) )
         setattr( opts, opt, result )
      
   config_text = None 
   if config_opt is not None and hasattr(opts, config_opt):
      config_text = getattr(opts, config_opt)
      
   config = load_config( config_text, opts, config_header, config_options )
   
   if config == None:
      print >> sys.stderr, "Failed to load configuration"
      parser.print_help()
      return None

   if not allow_none:
      # remove any instances of None from config 
      ks = config.keys()
      for k in ks:
         if config[k] is None:
            del config[k]
      
   if conf_validator is not None:
      rc = conf_validator( config )
      if rc != 0:
         print >> sys.stderr, "Invalid arguments"
         parser.print_help()
         return None
      
   return config


def object_file_path( config, object_type, object_id ):
   """
   Get the path to an object's file.
   """
   dirname = config.get( OBJECT_DIR_NAMES.get( object_type, None ) )
   if dirname == None:
      raise Exception("Could not get directory for type '%s'" % object_type)
  
   return os.path.join( dirname, str(object_id) )


def object_key_path( config, object_type, object_id, public=False, no_suffix=False ):
   """
   Get the path to an object's public or private key.
   Add default suffixes, unless directed otherwise.
   Return the path to the key file.
   """
   suffix = ".pkey"
   if public:
      suffix = ".pub"
      
   if no_suffix:
      suffix = ""
      
   return object_file_path( config, object_type, object_id ) + suffix 


def parse_args( config, method_name, args, kw, lib ):
   """
   Parse API method arguments.
   Look up the method in the API, get its list of arguments and its 
   argument parser, and go forth and load them.
   Load positional arguments in order, and then load keyword arguments 
   in lexical order by argument name.
   Accumulate ancillary data from the parsing process as well, i.e. 
   to be used for subsequent processing.
   
   Return (parsed argument list, parsed keyword dictionary, ancillary data dictionary)
   Raise an exception if we failed somehow.
   """
   
   import syndicate.ms.api as api
   method = api.get_method( method_name )
   try:
      if method.parse_args:
         args, kw, extras = method.parse_args( config, method_name, method.argspec, args, kw, lib )
         return args, kw, extras
      else:
         return args, kw, {}
   except Exception, e:
      log.exception(e)
      log.error("Failed to parse method arguments for '%s'" % method_name)
      raise
      
      
def validate_args( method_name, args, kw ):
   """
   Validate an API method's arguments.
   * verify that we got the right number of positional arguments.
   
   Return True if valid 
   Raise an exception on error
   """
   import syndicate.ms.api as api
   method = api.get_method( method_name )
   
   arg_len = len(method.argspec.args)
   def_len = 0
   
   if method.argspec.defaults is not None:
      def_len = len(method.argspec.defaults)
   
   if len(args) != arg_len - def_len:
      raise Exception("Method '%s' expects %s arguments; got %s (%s)" % (method_name, arg_len - def_len, len(args), args))
   
   return True


def extend_key_paths( config, base_dir ):
   """
   Extend all key paths for all object types by ensuring 
   that they are all prefixed with base_dir.
   """
   
   for (_, keydir) in OBJECT_DIR_NAMES.items():
      path = config.get( keydir, None )
      if path == None:
         path = keydir
      
      if not os.path.isabs( path ):
         # not absolute, so append with base dir 
         path = os.path.join( base_dir, path )
         config[keydir] = path


def fill_defaults( config ):
   """
   Fill a config dict with default values.
   """
   global OBJECT_DIR_NAMES, CONFIG_DEFAULTS
   
   config.update( CONFIG_DEFAULTS )
   
   extend_key_paths( config, CONFIG_DIR )
   config['_in_argv'] = []
   config['_in_config'] = []
   
   return 

   
def usage( progname ):
   """
   Print usage and exit.
   """
   parser = build_parser( progname, CONFIG_DESCRIPTION )
   parser.print_help()
   sys.exit(1)
