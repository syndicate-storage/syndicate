#!/usr/bin/python
"""
Originally part of PLCAPI.  Modifications by Jude Nelson
"""

#
# PLCAPI configuration store. Supports XML-based configuration file
# format exported by MyPLC.
#
# Mark Huang <mlhuang@cs.princeton.edu>
# Copyright (C) 2004-2006 The Trustees of Princeton University
#
# $Id: Config.py 14587 2009-07-19 13:18:50Z thierry $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/trunk/PLC/Config.py $
#

import os
import sys

from SMDS.faults import *

class Config:
    """
    Parse the bash/Python/PHP version of the configuration file. Very
    fast but no type conversions.
    """
    DEFAULT_CONFIG_FILE="/etc/syndicate/syndicate-metadata-service.conf"

    def __init__(self, file = "/etc/syndicate/syndicate-metadata-service.conf"):
        # Load plc_config
        try:
            execfile(file, self.__dict__)
        except:
            raise MDException(900, "Could not find syndicate metadata service configuration")
