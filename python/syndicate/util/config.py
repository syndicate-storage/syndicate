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

#-------------------------
def load_config( config_str, opts, config_header, config_options ):
   
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
      
      elif config != None and config.has_option( config_header, arg_opt):
         ret[arg_opt] = config.get( config_header, arg_opt)
         
         ret["_in_config"].append( arg_opt )
   
   return ret


#-------------------------
def build_parser( progname, description, config_options ):
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


#-------------------------
def build_config( argv, description, config_header, config_options, conf_validator=None, opt_handlers={}, config_opt=None, allow_none=False ):
   
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

#-------------------------
def defaults( config ):
   
   config['_in_argv'] = []
   config['_in_config'] = []
   
   return 0
