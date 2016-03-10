#!/usr/bin/env python

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

import os
import irods
import logging

from os import O_RDONLY
from io import RawIOBase, BufferedRandom
from irods.session import iRODSSession
from irods.data_object import iRODSDataObject, iRODSDataObjectFileRaw
from retrying import retry
from timeout_decorator import timeout

logger = logging.getLogger('irods_client')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('irods_client.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)


MAX_ATTEMPT = 3         # 3 retries
ATTEMPT_INTERVAL = 5000 # 5 sec
TIMEOUT_SECONDS = 20    # 20 sec

"""
Timeout only works at a main thread.
"""

"""
Do not call these functions directly.
These functions are called by irods_client class!
"""
#@timeout(TIMEOUT_SECONDS)
def _getCollection(session, path):
    return session.collections.get(path)

#@timeout(TIMEOUT_SECONDS)
def _readLargeBlock(br):
    return br.read(1024*1024)

"""
Interface class to iRODS
"""
class irods_status(object):
    def __init__(self, directory=False, 
                       path=None,
                       name=None, 
                       size=0,
                       checksum=0,
                       create_time=0,
                       modify_time=0):
        self.directory = directory
        self.path = path
        self.name = name
        self.size = size
        self.checksum = checksum
        self.create_time = create_time
        self.modify_time = modify_time

    @classmethod
    def fromCollection(cls, col):
        return irods_status(directory=True, 
                            path=col.path,
                            name=col.name)

    @classmethod
    def fromDataObject(cls, obj):
        return irods_status(directory=False, 
                            path=obj.path,
                            name=obj.name, 
                            size=obj.size, 
                            checksum=obj.checksum, 
                            create_time=obj.create_time, 
                            modify_time=obj.modify_time)

    def __eq__(self, other): 
        return self.__dict__ == other.__dict__

    def __repr__(self): 
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        return "<irods_status %s %s %d %s>" % (rep_d, self.name, self.size, self.checksum) 

class irods_client(object):
    def __init__(self, host=None,
                       port=1247,
                       user=None,
                       password=None,
                       zone=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone
        self.session = None

    def connect(self):
        self.session = iRODSSession(host=self.host, 
                                    port=self.port, 
                                    user=self.user, 
                                    password=self.password, 
                                    zone=self.zone)

    def close(self):
        self.session.cleanup()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    """
    Returns directory entries in string
    """
    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def list(self, path):
        coll = _getCollection(self.session, path)
        entries = []
        for col in coll.subcollections:
            entries.append(col.name)

        for obj in coll.data_objects:
            entries.append(obj.name)
        return entries

    """
    Returns directory entries with status
    """
    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def listStats(self, path):
        coll = _getCollection(self.session, path)
        stats = []
        for col in coll.subcollections:
            stats.append(irods_status.fromCollection(col))

        for obj in coll.data_objects:
            stats.append(irods_status.fromDataObject(obj))
        return stats

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def isDir(self, path):
        parent = os.path.dirname(path)
        coll = _getCollection(self.session, parent)
        for col in coll.subcollections:
            if col.path == path:
                return True
        return False

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def isFile(self, path):
        parent = os.path.dirname(path)
        coll = _getCollection(self.session, parent)
        for obj in coll.data_objects:
            if obj.path == path:
                return True
        return False

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def exists(self, path):
        stat = self.getStat(path)
        if stat:
            return True
        return False

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def getStat(self, path):
        parent = os.path.dirname(path)
        coll = _getCollection(self.session, parent)
        for col in coll.subcollections:
            if col.path == path:
                return irods_status.fromCollection(col)

        for obj in coll.data_objects:
            if obj.path == path:
                return irods_status.fromDataObject(obj)

        return None

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def read(self, path, offset, size):
        buf = None
        br = None
        conn = None
        try:
            conn, desc = self.session.data_objects.open(path, O_RDONLY)
            raw = iRODSDataObjectFileRaw(conn, desc)
            br = BufferedRandom(raw)
            new_offset = br.seek(offset)
            
            if new_offset == offset:
                buf = br.read(size)
        finally:
            if br:
                br.close()
            if conn:
                conn.release(True)

        return buf

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def download(self, path, to):
        conn, desc = self.session.data_objects.open(path, O_RDONLY)
        raw = iRODSDataObjectFileRaw(conn, desc)
        br = BufferedRandom(raw)

        try:
            with open(to, 'w') as wf:
                while(True):
                    buf = _readLargeBlock(br)

                    if not buf:
                        break

                    wf.write(buf)
        finally:
            conn.release(True)
            br.close()

        return to



