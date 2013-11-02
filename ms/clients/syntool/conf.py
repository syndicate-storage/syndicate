#!/usr/bin/env python


"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import ConfigParser

import common

log = common.log


# -------------------
def build_config( config ):
   # build up our config
   config.add_section("global")
   config.set("global", "MS", "http://localhost:8080")
   config.set("global", "email", "nobody@nowhere.com")
   


# -------------------
def load_config( conf_filename ):
   
   config = ConfigParser.SafeConfigParser()
   
   try:
      config.read( conf_filename )
   except Exception, e:
      log.exception( e )
      return None
   
   return config