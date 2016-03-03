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
import sys
import logging
import time
import threading

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

def get_current_time():
    return int(round(time.time() * 1000))

"""
Interface class to dataset_tracker
"""
class dataset_directory(object):
    def __init__(self, path=None,
                       entries=[],
                       last_visit_time=0,
                       handler=None):
        self.path = path
        self.entries = {}
        for entry in entries:
            self.entries[entry.name] = entry

        if last_visit_time:
            self.last_visit_time = last_visit_time
        else:
            self.last_visit_time = get_current_time()
        self.lock = threading.RLock()
        self.handler = handler

    def __enter__(self):
        self._lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._unlock()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def getEntry(self, name):
        with self.lock:
            entry = self.entries.get(name)
        return entry 

    def getEntries(self):
        with self.lock:
            entry = self.entries.values()
        return entry

    def updateFully(self, entries=[], last_visit_time=0):
        with self.lock:
            # find removed/added/updated entries
            new_entries = {}
            for entry in entries:
                new_entries[entry.name] = entry
                
            set_prev = set(self.entries.keys())
            set_new = set(new_entries.keys())

            set_intersection = set_prev & set_new

            unchanged_entries = []
            updated_entries = []
            removed_entries = []
            added_entries = []

            # check update and unchanged
            for key in set_intersection:
                e_old = self.entries[key]
                e_new = new_entries[key]
                if e_old == e_new:
                    # unchanged
                    unchanged_entries.append(e_old)
                else:
                    # changed
                    updated_entries.append(e_new)

            # check removed
            for key in set_prev:
                if key not in set_intersection:
                    # removed
                    e_old = self.entries[key]
                    removed_entries.append(e_old)

            # check added
            for key in set_new:
                if key not in set_intersection:
                    # added
                    e_new = new_entries[key]
                    added_entries.append(e_new)

            # apply to existing dictionary
            for entry in removed_entries:
                del self.entries[entry.name]

            for entry in updated_entries:
                self.entries[entry.name] = entry

            for entry in added_entries:
                self.entries[entry.name] = entry

            if last_visit_time:
                self.last_visit_time = last_visit_time
            else:
                self.last_visit_time = get_current_time()

        if self.handler:
            self.handler(updated_entries, added_entries, removed_entries)

    def removeAllEntries(self):
        with self.lock:
            removed_entries = self.entries.values()
            self.entries.clear()

        if self.handler:
            self.handler([], [], removed_entries)

    def __repr__(self): 
        return "<dataset_directory %s %d>" % (self.path, self.last_visit_time) 

    def __eq__(self, other): 
        return self.__dict__ == other.__dict__

class dataset_tracker(object):
    def __init__(self, root_path="/", 
                       update_event_handler=None, request_for_update_handler=None):
        self.root_path = root_path
        self.directories = {}
        self.lock = threading.RLock()
        self.update_event_handler = update_event_handler
        self.request_for_update_handler = request_for_update_handler

    def __enter__(self):
        self._lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._unlock()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def getRootPath(self):
        return self.root_path

    def _onRequestUpdate(self, directory):
        if self.request_for_update_handler:
            self.request_for_update_handler(directory)

    def _onDirectoryUpdate(self, updated_entries, added_entries, removed_entries):
        # if directory is updated
        if removed_entries:
            for removed_entry in removed_entries:
                if removed_entry.directory:
                    with self.lock:
                        e_directory = self.directories.get(removed_entry.path)
                        if e_directory:
                            e_directory.removeAllEntries()

        if (updated_entries and len(updated_entries) > 0) or \
           (added_entries and len(added_entries) > 0) or \
           (removed_entries and len(removed_entries) > 0):
            # if any of these are not empty
            if self.update_event_handler:
                self.update_event_handler(updated_entries, added_entries, removed_entries)

        if added_entries:
            for added_entry in added_entries:
                if added_entry.directory:
                    self._onRequestUpdate(added_entry)

        if updated_entries:
            for updated_entry in updated_entries:
                if updated_entry.directory:
                    self._onRequestUpdate(updated_entry)

    def updateDirectory(self, path=None, entries=[]):
        with self.lock:
            e_directory = self.directories.get(path)
            if e_directory:
                e_directory.updateFully(entries)
            else:
                self.directories[path] = dataset_directory(path=path, entries=entries, handler=self._onDirectoryUpdate)
                self._onDirectoryUpdate([], entries, [])

    def getDirectory(self, path):
        with self.lock:
            directory = self.directories.get(path)
            return directory

    def _walk(self, directory):
        log.info("_walk %s", directory)
        entries = []
        print directory
        if directory:
            directory._lock()
            dirs = []
            for entry in directory.getEntries():
                if entry.directory:
                    dirs.append(entry.path)
                entries.append(entry.path)

            for path in dirs:
                sub_dir = self.directories.get(path)
                if sub_dir:
                    for sub_entry in self._walk(sub_dir):
                        entries.append(sub_entry)
            directory._unlock()
        return entries

    def walk(self):
        entries = []
        with self.lock:
            root_dir = self.directories.get(self.root_path)
            if root_dir:
                for entry in self._walk(root_dir):
                    entries.append(entry)
        return entries

