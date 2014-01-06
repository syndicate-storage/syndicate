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
import syndicate.client.common.log as Log

log = Log.get_logger()

# -------------------
def build_config( config, name, config_options ):
   
   # build up our config
   config.add_section(name)
   
   for config_option in config_options:
      config.set(name, config_option, None)
   
   return config

# -------------------
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
         # no argument, but mark its existence
         parser.add_argument( "--" + config_option, short_option, action="store_true", help=config_help)
   
   return parser

# -------------------
def extend_paths( config, base_dir, dirnames ):
   # extend all paths
   for dirname in dirnames:
      path = config.get( dirname, None )
      if path == None:
         raise Exception("Missing directory: %s" % dirname)
      
      if not os.path.isabs( path ):
         # not absolute, so append with base dir 
         path = os.path.join( base_dir, path )
         config[dirname] = path


# -------------------
def load_config( config_str, config_data, arg_opts ):
   
   name = "config"
   
   if config_str == None:
      config_str = "[%s]" % name
      
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
   config_opts = config.options(name)
   for opt in config_opts:
      if config.get(name, opt) == None:
         missing.append( opt )
         
   if missing:
      log.error("Missing options: %s" % (",".join(missing)) )
      return None
   
   ret = {}
   ret["_in_argv"] = []
   ret["_in_config"] = []
   
   # convert to dictionary, merging in argv opts
   for arg_opt in config_data.keys():
      if hasattr(arg_opts, arg_opt) and getattr(arg_opts, arg_opt) != None:
         ret[arg_opt] = getattr(arg_opts, arg_opt)
         
         # force singleton...
         if isinstance(ret[arg_opt], list) and len(ret[arg_opt]) == 1 and config_data[arg_opt][1] == 1:
            ret[arg_opt] = ret[arg_opt][0]
            
         ret["_in_argv"].append( arg_opt )
      
      elif config.has_option(name, arg_opt):
         ret[arg_opt] = config.get(name, arg_opt)
         
         ret["_in_config"].append( arg_opt )
         
   return ret
   
# -------------------
def usage( progname, description, opts ):
   parser = build_parser( progname, description, opts )
   parser.print_help()
   sys.exit(1)
   