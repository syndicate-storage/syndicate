"""
   Copyright 2015 The Trustees of Princeton University

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

import random

from google.appengine.api import memcache
from google.appengine.ext import ndb

import backends as backend

UUID_KEY_TEMPLATE = 'uuid-{}-{}'


class GeneralUUID(ndb.Model):
    """
    Shards for each named counter.
    """
    uuid = ndb.StringProperty(default="")

def __uuid_key_names( uuids, namespace ):
   key_strings = [UUID_KEY_TEMPLATE.format(uuid, namespace) for uuid in uuids]
   return key_strings
   
   
@ndb.tasklet
def get_uuids_async( uuids, namespace, use_memcache=True ):
   """
   Get all UUIDs under a namespace
   """
   do_cache = False
   all_key_names = __uuid_key_names( uuids, namespace )
   all_keys = [ndb.Key( GeneralUUID, key_name ) for key_name in all_key_names]
   uuid_data = yield ndb.get_multi_async( all_keys, use_cache=False, use_memcache=use_memcache )
   
   # NOTE: uuid_data is ordered by uuids
   raise ndb.Return( [uuid.uuid if uuid is not None else None for uuid in uuid_data] )


def get_uuids( uuids, namespace, use_memcache=True ):
   uuid_data_fut = get_uuids_async( uuids, namespace, use_memcache )
   return uuid_data_fut.get_result()

@ndb.tasklet
def put_uuids_async( uuids, namespace ):
   """
   Put UUIDs to a namespace, asynchronously
   """
   all_key_names = __uuid_key_names( uuids, namespace )
   all_keys = [ndb.Key( GeneralUUID, key_name ) for key_name in all_key_names]
   
   uuid_data = [ GeneralUUID( key=key, uuid=uuid ) for (key, uuid) in zip( all_keys, uuids ) ]
   
   put_keys = yield ndb.put_multi_async( uuid_data )
   
   raise ndb.Return( put_keys )


def put_uuids( uuids, namespace ):
   """
   Put UUIDs to a namespace, synchronously
   """
   fut = put_uuids_async( uuids, namespace )
   fut.wait()
   return 
