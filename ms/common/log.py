#!/usr/bin/env python


"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import logging

global_log = None 

#-------------------------
def get_logger():

    global global_log
    
    if global_log == None:
       
        log = logging.getLogger()
        log.setLevel(logging.DEBUG)
        log.propagate = False

        formatter = logging.Formatter('[%(levelname)s] %(message)s')
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