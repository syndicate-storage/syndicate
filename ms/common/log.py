#!/usr/bin/env python


"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

DEBUG = True
import logging

#-------------------------
def get_logger( name ):

    if(DEBUG):
       
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)
        log.propagate = False

        formatter = logging.Formatter('[%(levelname)s] %(message)s')
        handler_stream = logging.StreamHandler()
        handler_stream.setFormatter(formatter)
        log.addHandler(handler_stream)

    else:
        log = None

    return log