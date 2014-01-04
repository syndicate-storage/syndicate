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

import contact
import message
import storage
import keys
import conf

import ar

def set_storage_root( storage_root ):
   storage.STORAGE_ROOT = storage_root

#-------------------------
def setup_storage( config ):
   
   storage_dirs = []
   for mod in [contact, message, storage, keys]:
      storage_dir_module = getattr(mod, STORAGE_DIRS, None)
      if storage_dir_module != None:
         storage_dirs.append( storage_dir_module )
   
   rc = storage.setup_dirs( storage.STORAGE_ROOT, storage_dirs )
   if not rc:
      log.error("Failed to set up storage directories")
      return False
   
   return True
   

#-------------------------
def load_options( argv ):
   
   parser = conf.build_parser( argv[0] )
   opts = parser.parse_args( argv[1:] )
   
   # load everything into a dictionary and return it
   config = None 
   config_str = None
   config_file_path = None
   
   if hasattr( opts, "config" ) and opts.config != None:
      config_file_path = opts.config[0]
   else:
      config_file_path = conf.CONFIG_FILENAME
   
   config_str = storage.read_file( config_file_path )
   if config_str == None:
      raise Exception("Failed to load configuration from %s" % config_file_path )
   
   config = conf.load_config( config_file_path, config_str, opts )
   if config == None:
      raise Exception("Failed to parse configuration from %s" % config_file_path)
   
   return config
