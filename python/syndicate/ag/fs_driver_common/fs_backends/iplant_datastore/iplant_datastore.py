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

"""
iPlant Data Store Backend
"""

import os
import time
import syndicate.ag.fs_driver_common.abstract_fs as abstract_fs
import syndicate.ag.fs_driver_common.metadata as metadata
import syndicate.ag.fs_driver_common.fs_backends.iplant_datastore.bms_client as bms_client
import syndicate.ag.fs_driver_common.fs_backends.iplant_datastore.irods_client as irods_client

class backend_impl(abstract_fs.fs_base):
    def __init__(self, config):
        if not config:
            raise ValueError("fs configuration is not given correctly")

        dataset_root = config.get("dataset_root")
        if not dataset_root:
            raise ValueError("dataset_root configuration is not given correctly")
        # config can have unicode strings
        dataset_root = dataset_root.encode('ascii','ignore')
        dataset_root = dataset_root.rstrip("/")

        secrets = config.get("secrets")
        if not secrets:
            raise ValueError("secrets are not given correctly")

        user = secrets.get("user")
        user = user.encode('ascii','ignore')
        if not user:
            raise ValueError("user is not given correctly")

        password = secrets.get("password")
        password = password.encode('ascii','ignore')
        if not password:
            raise ValueError("password is not given correctly")

        irods_config = config.get("irods")
        if not irods_config:
            raise ValueError("irods configuration is not given correctly")

        bms_config = config.get("bms")
        if not bms_config:
            raise ValueError("bms configuration is not given correctly")

        self.irods_config = irods_config
        self.bms_config = bms_config

        # init irods client
        # we convert unicode (maybe) strings to ascii since python-irodsclient cannot accept unicode strings
        irods_host = self.irods_config["host"]
        irods_host = irods_host.encode('ascii','ignore')
        irods_zone = self.irods_config["zone"]
        irods_zone = irods_zone.encode('ascii','ignore')
        
        self.irods = irods_client.irods_client(host=irods_host, 
                                               port=self.irods_config["port"], 
                                               user=user, 
                                               password=password, 
                                               zone=irods_zone)
        # init bms client
        path_filter = dataset_root.rstrip("/") + "/*"

        acceptor = bms_client.bms_message_acceptor("path", 
                                                   path_filter)
        self.bms = bms_client.bms_client(host=self.bms_config["host"], 
                                         port=self.bms_config["port"], 
                                         user=user, 
                                         password=password, 
                                         vhost=self.bms_config["vhost"],
                                         acceptors=[acceptor])


        self.bms.setCallbacks(on_message_callback=self._on_bms_message_receive)

        # init dataset tracker
        self.dataset_tracker = metadata.dataset_tracker(root_path=dataset_root,
                                                        update_event_handler=self._on_dataset_update, 
                                                        request_for_update_handler=self._on_request_update)

        self.notification_cb = None

    def _on_bms_message_receive(self, message):
        # parse message and update directory
        if not message or len(message) <= 0:
            return

        msg = json.loads(message)
        operation = msg.get("operation")
        if operation:
            if operation in ["collection.add", "collection.rm", "data-object.add", "data-object.rm", "data-object.mod"]:
                path = msg.get("path")
                if path:
                    parent_path = os.path.dirname(path)
                    entries = self.irods.listStats(parent_path)
                    stats = []
                    for e in entries:
                        stat = abstract_fs.fs_stat(directory=e.directory, 
                                                   path=e.path,
                                                   name=e.name, 
                                                   size=e.size,
                                                   checksum=e.checksum,
                                                   create_time=e.create_time,
                                                   modify_time=e.modify_time)
                        stats.append(stat)
                    self.dataset_tracker.updateDirectory(path=parent_path, entries=stats)

            elif operation in ["collection.mv", "data-object.mv"]:
                old_path = msg.get("old-path")
                old_parent_path = None
                if old_path:
                    old_parent_path = os.path.dirname(old_path)
                    entries = self.irods.listStats(old_parent_path)
                    stats = []
                    for e in entries:
                        stat = abstract_fs.fs_stat(directory=e.directory, 
                                                   path=e.path,
                                                   name=e.name, 
                                                   size=e.size,
                                                   checksum=e.checksum,
                                                   create_time=e.create_time,
                                                   modify_time=e.modify_time)
                        stats.append(stat)
                    self.dataset_tracker.updateDirectory(path=old_parent_path, entries=stats)

                new_path = msg.get("new-path")
                if new_path:
                    new_parent_path = os.path.dirname(new_path)
                    if new_parent_path != old_parent_path:
                        entries = self.irods.listStats(new_parent_path)
                        stats = []
                        for e in entries:
                            stat = abstract_fs.fs_stat(directory=e.directory, 
                                                       path=e.path,
                                                       name=e.name, 
                                                       size=e.size,
                                                       checksum=e.checksum,
                                                       create_time=e.create_time,
                                                       modify_time=e.modify_time)
                            stats.append(stat)
                        self.dataset_tracker.updateDirectory(path=new_parent_path, entries=stats)

    def _on_dataset_update(self, updated_entries, added_entries, removed_entries):
        if self.notification_cb:
            self.notification_cb(updated_entries, added_entries, removed_entries)

    def _on_request_update(self, entry):
        entries = self.irods.listStats(entry.path)
        stats = []
        for e in entries:
            stat = abstract_fs.fs_stat(directory=e.directory, 
                                       path=e.path,
                                       name=e.name, 
                                       size=e.size,
                                       checksum=e.checksum,
                                       create_time=e.create_time,
                                       modify_time=e.modify_time)
            stats.append(stat)
        self.dataset_tracker.updateDirectory(path=entry.path, entries=stats)

    def connect(self):
        self.irods.connect()
        self.bms.connect()

        # add initial dataset
        dataset_root = self.dataset_tracker.getRootPath()
        entries = self.irods.listStats(dataset_root)
        stats = []
        for entry in entries:
            stat = abstract_fs.fs_stat(directory=entry.directory, 
                                       path=entry.path,
                                       name=entry.name, 
                                       size=entry.size,
                                       checksum=entry.checksum,
                                       create_time=entry.create_time,
                                       modify_time=entry.modify_time)
            stats.append(stat)
        self.dataset_tracker.updateDirectory(path=dataset_root, entries=stats)

    def close(self):
        self.bms.close()
        self.irods.close()

    def exists(self, path):
        ascii_path = path.encode('ascii','ignore')
        return self.irods.exists(ascii_path)

    def list_dir(self, dirpath):
        ascii_path = dirpath.encode('ascii','ignore')
        return self.irods.list(ascii_path)

    def is_dir(self, dirpath):
        ascii_path = dirpath.encode('ascii','ignore')
        return self.irods.isDir(ascii_path)

    def read(self, filepath, offset, size):
        ascii_path = filepath.encode('ascii','ignore')
        return self.irods.read(ascii_path, offset, size)

    def backend(self):
        return self.__class__

    def notification_supported(self):
        return True

    def set_notification_cb(self, notification_cb):
        self.notification_cb = notification_cb


