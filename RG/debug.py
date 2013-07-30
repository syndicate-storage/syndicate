#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

RG_STORE = "none"

#-------------------------
def debug():

    from rg_common import *  

    log = get_logger() 

    file_name = "test"
    
    log.debug(u'ERROR: Connection to %s failed: %s', RG_STORE, file_name)

    return True 

#-------------------------    
if __name__ == "__main__":
  
    debug()