#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import syndicate.client.common.log as Log


# global volume instance
MY_VOLUME = None                # initialized on login

def get_volume():
   global MY_VOLUME
   return MY_VOLUME

def set_volume( vol_impl ):
   global MY_VOLUME
   MY_VOLUME = vol_impl