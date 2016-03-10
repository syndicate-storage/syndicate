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

from abc import ABCMeta, abstractmethod

"""
Abstraction of filesystem insterface
"""
class fs_stat(object):
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

    def __eq__(self, other): 
        return self.__dict__ == other.__dict__

    def __repr__(self): 
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        return "<fs_stat %s %s %d %s>" % (rep_d, self.name, self.size, self.checksum) 


class fs_base(object):
    __metaclass__ = ABCMeta

    # connect to remote system if necessary
    @abstractmethod
    def connect(self):
        pass

    # disconnect and finalize
    @abstractmethod
    def close(self):
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    # check existence of path (a file or a directory) and return True/False
    @abstractmethod
    def exists(self, path):
        pass

    # list directory entries (files and sub-directories) and return names of found items
    @abstractmethod
    def list_dir(self, dirpath):
        pass

    # check if given path is a directory and return True/False
    @abstractmethod
    def is_dir(self, dirpath):
        pass

    # read bytes at given offset in given size from given path and return byte[]
    @abstractmethod
    def read(self, filepath, offset, size):
        pass

    # return a class of backend plugin
    @abstractmethod
    def backend(self):
        pass

    @abstractmethod
    def notification_supported(self):
        pass

    @abstractmethod
    def set_notification_cb(self, notification_cb):
        pass

