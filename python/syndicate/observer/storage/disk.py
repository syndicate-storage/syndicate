#!/usr/bin/python 

"""
   Copyright 2014 The Trustees of Princeton University

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

import sys 
import os 

#-------------------------------
def put_principal_data( principal_id, public_key_pem, sealed_private_key ):
   pass

#-------------------------------
def delete_principal_data( principal_id ):
   pass 

#-------------------------------
def get_principal_data( principal_id ):
   pass

#--------------------------------
def get_slice_secret( observer_pkey_pem, slice_name, slice_fk=None ):
   pass 

#-------------------------------
def put_slice_secret( observer_pkey_pem, slice_name, slice_secret, slice_fk=None, opencloud_slice=None ):
   pass 

#-------------------------------
def put_volumeslice_creds( volume_name, slice_name, creds ):
   pass 

#-------------------------------
def get_volumeslice_volume_names( slice_name ):
   pass 

#-------------------------------
def get_volumeslice( volume_name, slice_name ):
   pass 

#-------------------------------
def get_slice_hostnames( slice_name ):
   pass 
