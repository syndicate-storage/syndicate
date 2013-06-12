"""
Copyright 2013 The Trustees of Princeton University
All Rights Reserved
"""

import MS
from MS.volume import Volume
from MS.entry import *

import protobufs.ms_pb2 as ms_pb2

from storage import storage

import random
import os
import errno

import logging

def make_ms_reply( volume, error ):
   reply = ms_pb2.ms_reply()

   reply.volume_version = volume.version
   reply.error = error

   return reply
