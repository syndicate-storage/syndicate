#!/usr/bin/env python


"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

DEBUG = True

#-------------------------
def get_logger():

    import logging

    if(DEBUG):
        log = logging.getLogger()
        log.setLevel(logging.DEBUG)

        formatter = logging.Formatter('[%(levelname)s] %(message)s')
        handler_stream = logging.StreamHandler()
        handler_stream.setFormatter(formatter)
        log.addHandler(handler_stream)

    else:
        log = None

    return log

#-------------------------
log = get_logger()