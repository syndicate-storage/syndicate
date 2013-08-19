#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import sys

RG_STORE = "none"

#-------------------------
def debug():

    from rg_common import log, parse_block_request

    file_name = "test"

    data = 'GET /SYNDICATE-DATA/tmp/hello.1375782135401/0.8649607004776574730'

    parse_block_request(data)
    
    return True 

#-------------------------    
if __name__ == "__main__":
  
    debug()