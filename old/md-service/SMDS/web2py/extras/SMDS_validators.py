import sys
import re

sys.path.append('/usr/share/syndicate_md')

from SMDS.user import *
from SMDS.mdserver import *
from SMDS.mdapi import MDAPI

class IS_SMDS_USER:
    """
    Validator for username fields--make sure that the user is a valid SMDS user.
    """
    def __init__(self, error_message='Invalid User'):
        self.error_message = error_message
        self.api = MDAPI()
    
    def __call__(self, value):
        if value == None or len(value) == 0:
           return (value, None)

        try:
            smds_user = Users( self.api, {'username': value, 'enabled': True})[0]
            
            # yup
            return (value, None)
        except:
            # nope
            return (value, self.error_message)
    
    def formatter( self, value ):
        return value


class IS_FREE_VOLUME_NAME:
    """
    Validator for volume fields--make sure that the volume name is unused
    """
    def __init__(self, error_message='Invalid Volume Name'):
        self.error_message = error_message
        self.api = MDAPI()
    
    def __call__(self, value):
        if value == None or len(value) == 0:
           return (value, None)

        try:
            smds_volume = MDServers( self.api, {'name': value})[0]

            # not free
            return (value, self.error_message)
        except:
            # free
            return (value, None)
    
    def formatter( self, value ):
        return value


class IS_HOSTNAME:
   """
   Validator for hostname URL field--make ure it's a well-formed hostname
   """
   def __init__(self, error_message="Invalid hostname"):
      self.error_message = error_message
      self.api = MDAPI()
   
   def __call__(self, value):
      if value == None or len(value) == 0:
         return (value, None)
      
      if value.find("://") >= 0:
         # can't have a protocol
         return (value, self.error_message)
      
      names = value.split(".")
      for name in names:
         if len( re.findall("[\w\d:#@%/;$()~_?\+-=\\\.&]+", name) ) != 1:
            # have a non-alphanumeric character
            return (value, self.error_message)
         
      
      return (value,None)
   
   def formatter( self, value ):
      return value
      
            