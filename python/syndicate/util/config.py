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
import paths

logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger()

CONFIG_DIR = "~/.syndicate"
CONFIG_FILENAME = "~/.syndicate/syndicate.conf"

def default_config_dir():
    global CONFIG_DIR
    return os.path.expanduser(CONFIG_DIR)

def default_config_path():
    global CONFIG_FILENAME
    return os.path.expanduser(CONFIG_FILENAME)


CONFIG_DESCRIPTION = "Syndicate Administration Tool"

OBJECT_DIR_NAMES = {
   "volume": "volumes",
   "user": "users",
   "gateway": "gateways",
   "syndicate": "syndicate",
   "certs": "certs",
   "data": "data",
   "drivers": "drivers",
   "logs": "logs",
   "amd": "amd"
}

CONFIG_SYNDICATE_KEYS = [
   "MS_url",
   "username",
   "volumes",
   "users",
   "gateways",
   "syndicate",
   "drivers",
   "logs",
   "data",
   "certs"
]
   
CONFIG_DEFAULTS = {
   "MS_url": "http://127.0.0.1:8080",
   'trust_public_key': False,
   'debug': False,
   'helpers': {
       'fetch_user_cert': paths.fetch_user_cert,
       'fetch_gateway_cert': paths.fetch_gateway_cert,
       'fetch_volume_cert': paths.fetch_volume_cert,
       'fetch_cert_bundle': paths.fetch_cert_bundle,
       'fetch_driver': paths.fetch_driver,
       'fetch_syndicate_pubkey': paths.fetch_syndicate_pubkey,
       'validate_user_cert': paths.validate_user_cert,
       'certs_reload': paths.certs_reload,
       'driver_reload': paths.driver_reload
    }
}

# 'syndicate' section of the config file
CONFIG_OPTIONS = {
   "MS_url":            ("-m", 1, "URL to your Syndicate MS"),
   "username":          ("-u", 1, "The ID of the Syndicate user to run as."),
   "volume_keys":       ("-V", 1, "Path to the directory holding the Volume public keys."),
   "gateway_keys":      ("-G", 1, "Path to the directory holding the Gateway private keys."),
   "user_keys":         ("-U", 1, "Path to the directory holding the User private keys."),
   "syndicate_keys":    ("-S", 1, "Path to the directory holding the Syndicate public keys."),
   "certs_dir":         ("-C", 1, "Path to the directory to store remotely-fetched certificates."),
   "config":            ("-c", 1, "Path to your config file (default is %s)." % CONFIG_FILENAME),
   "debug":             ("-d", 0, "Verbose debugging output"),
   "trust_public_key":  ("-t", 0, "If set, automatically trust the Syndicate public key if it is not yet trusted."),
   "params":            (None, "*", "Method name, followed by parameters (positional and keyword supported)."),
}


# 'helpers' section of the config file 
HELPER_OPTIONS = {
   "fetch_user_cert":       (None, 0, "Program to fetch a user's certificate"),
   "fetch_gateway_cert":    (None, 0, "Program to fetch a gateway's certificate"),
   "fetch_volume_cert":     (None, 0, "Program to fetch a volume's certificate"),
   "fetch_cert_bundle":     (None, 0, "Program to fetch a volume's certificate bundle"),
   "fetch_driver":          (None, 0, "Program to fetch a gateway's driver"),
   "fetch_syndicate_pubkey":   (None, 0, "Program to fetch a Syndicate instance's signing key"),
   "validate_user_cert":    (None, 0, "Program to validate and authenticate a user's certificate"),
   "certs_reload":          (None, 0, "Program to repopulate the certificate cache"),
   "driver_reload":         (None, 0, "Program to repopulate the driver cache")
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
   
   if config_str is not None:
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

   ret['config_dir'] = conf_dir
   ret['config_path'] = config_path
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
      raise Exception("Could not get directory for type '%s' (config = %s, object_dir_names = %s)" % (object_type, config, OBJECT_DIR_NAMES))
  
   return os.path.join( dirname, str(object_id) )


def object_base_file_path( config, object_type, object_id ):
   """
   Get the path to an object's file, relative to the config directory
   """
   dirname = config.get( OBJECT_DIR_NAMES.get( object_type, None ) )
   if dirname == None:
      raise Exception("Could not get directory for type '%s' (config = %s, object_dir_names = %s)" % (object_type, config, OBJECT_DIR_NAMES))
  
   return os.path.join( os.path.basename(dirname), str(object_id) )


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
   global CONFIG_DEFAULTS
   
   config.update( CONFIG_DEFAULTS )
   
   extend_key_paths( config, default_config_path())
   config['_in_argv'] = []
   config['_in_config'] = []
   
   return 

   
def usage( progname ):
   """
   Print usage and exit.
   """
   global CONFIG_DESCRIPTION, CONFIG_OPTIONS 

   parser = build_parser( progname, CONFIG_DESCRIPTION, CONFIG_OPTIONS )
   parser.print_help()
   sys.exit(1)


def print_parser_help( progname, description, options ):
   """
   Print usage for a section
   """
   parser = build_parser( progname, description, options )
   parser.print_help()

   
def syndicate_object_name( config ):
   """
   Given the config, find the syndicate object name.
   """
   return config["syndicate_host"] + ":" + str(config["syndicate_port"])


def get_config_from_argv( argv ):
    """
    Load up our configuration dict, using
    either the default config file (if none is specified in argv),
    or loading the config and any overrides from
    command-line options.

    Return a dict with the config information on success.
    Return None on error.
    """

    global CONFIG_OPTIONS, HELPER_OPTIONS, CONFIG_DESCRIPTION

    import syndicate.util.storage as storage
    import syndicate.util.client as client

    parser = build_parser( argv[0], CONFIG_DESCRIPTION, CONFIG_OPTIONS )
    opts, _ = parser.parse_known_args( argv[1:] )
    if opts.debug:
        log.setLevel(logging.DEBUG)
   
    # load everything into a dictionary and return it
    config_str = None
    config_file_path = None
   
    if hasattr( opts, "config" ) and opts.config != None:
        config_file_path = opts.config[0]
    else:
        config_file_path = default_config_path()
   
    config_str = storage.read_file( config_file_path )
   
    # generate the actual config
    config = {}
    fill_defaults( config )

    # get syndicate options...
    syndicate_config = load_config( config_file_path, config_str, opts, "syndicate", CONFIG_OPTIONS )
    if syndicate_config is None:
        log.error("Failed to parse configuration section 'syndicate' from '%s'" % opts.config_file_path)
        return None

    config.update( syndicate_config )

    # helpers..
    helper_config = load_config( config_file_path, config_str, opts, "helpers", HELPER_OPTIONS )
    if helper_config is None:
        log.error("Failed to parse configuration section 'helpers' from '%s'" % opts.config_file_path )
        return None

    config['helpers'].update( helper_config )
 
    # generate syndicate_host and syndicate_port from URL, if needed
    if config.get('MS_url', None) is not None:
        # use https if no :// is given 
        url = config['MS_url']
      
        host, port, no_tls = client.parse_url( url )
      
        config['syndicate_host'] = host 
        config['syndicate_port'] = port
        config['no_tls'] = no_tls
      
    # trust public key?
    if opts.trust_public_key:
        config['trust_public_key'] = True 
    else:
        config['trust_public_key'] = False

    # obtain syndicate public key, if on file
    syndicate_pubkey = storage.load_public_key( config, "syndicate", syndicate_object_name( config ) )
    if syndicate_pubkey is None:
        config['syndicate_public_key'] = None 
        config['syndicate_public_key_pem'] = None 

    else:
        config['syndicate_public_key'] = syndicate_pubkey
        config['syndicate_public_key_pem'] = syndicate_pubkey.exportKey()

    config['params'] = getattr( opts, "params", [] ) 
    return config


def get_extra_config( argv, section_name, section_options ):
    """
    Load extra configuration from argv 
    and from the config file, under the header
    'section_name'.

    The returned dict will have the extra section information.

    Return None if there were no options found for
    this section.
    """
    
    global CONFIG_DESCRIPTION, CONFIG_OPTIONS

    import storage
    import client

    parser = build_parser( argv[0], CONFIG_DESCRIPTION, CONFIG_OPTIONS )
    opts, _ = parser.parse_known_args( argv[1:] )
   
    # load everything into a dictionary and return it
    config_str = None
    config_file_path = None
   
    if hasattr( opts, "config" ) and opts.config != None:
        config_file_path = opts.config[0]
    else:
        config_file_path = default_config_path()

    if not os.path.exists(config_file_path):
        log.error("No config file at '%s'" % config_file_path)
        return None 

    if not os.path.isfile(config_file_path):
        log.error("Not a file: '%s'" % config_file_path)
        return None 

    config_str = storage.read_file( config_file_path )

    # get the extra options 
    extra_config = load_config( config_file_path, config_str, opts, section_name, section_options )
    if extra_config is None:
        log.error("No configuration for '%s'" % section_name )
        return None 

    return extra_config
