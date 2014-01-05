#!/usr/bin/python

import os

gateway_name = "testvolume-UG-1"

settings_kw = {
      "volume_name" : "testvolume",
      "gateway_name" : gateway_name,
      "oid_username" : "testuser@gmail.com",
      "oid_password" : "sniff",
      "ms_url" : "http://localhost:8080",
      "my_key_filename" : os.path.expanduser("~/.syndicate/gateway_keys/runtime/%s.pkey" % gateway_name),
      "storage_root" : "/tmp/test-syndicate-python-volume"
}
