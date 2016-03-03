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
import imp
import logging

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

"""
Find backends and create an instance
"""

class fs_backend_loader(object):
    def __init__(self, backend_name=None, backend_config=None):
        self.backend_name = backend_name
        self.backend_config = backend_config

    def getModulePath(self):
        module_path = os.path.abspath(__file__)
        module_dir = os.path.dirname(module_path)
        return module_dir + "/fs_backends/" + self.backend_name + "/" + self.backend_name + ".py"

    def load(self):
        if self.backend_name:
            module_path = self.getModulePath()
            log.info("load a backend module from %s", module_path)
            backend = imp.load_source(self.backend_name, 
                                      module_path)
            if backend:
                return backend.backend_impl(self.backend_config)
            else:
                log.error("unable to find a backend module for %s", self.backend_name)
                return None
        else:
            log.error("a backend module name is not given")
            return None


