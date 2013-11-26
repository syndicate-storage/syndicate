#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""


from msconfig import *

class StubObject:
   def __init__(self, *args, **kw):
      pass
   
   @classmethod
   def Authenticate( self, *args, **kw ):
      pass
   
   @classmethod
   def Sign( self, *args, **kw ):
      pass
   
class SyndicateUser( StubObject ):
   pass

class Volume( StubObject ):
   pass

class Gateway( StubObject ):
   pass

class VolumeAccessRequest( StubObject ):
   pass