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

import logging

logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
global_log = logging.getLogger()
global_log.setLevel( logging.ERROR )

#-------------------------
def get_logger():
    
    global global_log
    
    if global_log == None:
        # DEPRICATED
        log = logging.getLogger()
        log.setLevel(logging.DEBUG)
        log.propagate = False

        formatter = logging.Formatter('[%(levelname)s] [%(module)s:%(lineno)d] %(message)s')
        handler_stream = logging.StreamHandler()
        handler_stream.setFormatter(formatter)
        log.addHandler(handler_stream)
        
        global_log = log

    return global_log
 
 
#-------------------------
def set_log_level( level ):
   global global_log
   
   level_dict = {
      "DEBUG": logging.DEBUG,
      "INFO": logging.INFO,
      "WARNING": logging.WARNING,
      "ERROR": logging.ERROR,
      "CRITICAL": logging.CRITICAL
   }
   
   if level not in level_dict.keys():
      raise Exception("Invalid log level '%s'" % level)
   
   if global_log != None:
      global_log.setLevel( level_dict[level] )