#!/usr/bin/python

import sys
sys.path.append("/usr/share/SMDS")

import SMDS.mdapi
import SMDS.logger as logger
logger.init()

api = SMDS.mdapi.MDAPI()
rc = api.bootstrap_node( "/home/jude/.ssh/id_rsa", "princeton_syndicate", "planetlab2.arizona-gigapop.net", True )
print rc
