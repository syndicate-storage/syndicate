#!/usr/bin/python

import os

gateway_name = "mail"

settings_kw = {
      "volume_name" : "testvolume",
      "gateway_name" : gateway_name,
      "gateway_port" : 32780,
      "oid_username" : "testuser@gmail.com",
      "oid_password" : "sniff",
      "ms_url" : "http://localhost:8080",
      "gateway_key_pem" : open( os.path.expanduser("~/.syndicate/gateway_keys/runtime/%s.pkey" % gateway_name) ).read(),
      "storage_root" : "/tmp/test-syndicate-python-volume"
}

anonymous_settings_kw = {
      "volume_name" : "testvolume",
      "ms_url" : "http://localhost:8080",
      "gateway_key_pem" : open( os.path.expanduser("~/.syndicate/gateway_keys/runtime/%s.pkey" % gateway_name) ).read(),
      "storage_root" : "/tmp/test-syndicate-python-volume-anonymous",
}
