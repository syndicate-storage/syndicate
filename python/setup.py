#!/usr/bin/python

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

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

import os
import sys
import shutil

source_root = "../"
build_dir = ""
distro = "UNKNOWN"

# is build_root in the args?
i = 0
while i < len(sys.argv):
   if sys.argv[i].startswith('--source-root='):
      source_root = sys.argv[i].split("=", 1)[1]
      sys.argv.remove( sys.argv[i] )
      continue

   if sys.argv[i].startswith("--build-dir="):
      build_dir = sys.argv[i].split("=", 1)[1]
      sys.argv.remove( sys.argv[i] )
      continue

   if sys.argv[i].startswith("--distro="):
      distro = sys.argv[i].split("=", 1)[1]
      sys.argv.remove( sys.argv[i] )
      continue 

   i += 1

distro_switch = "-D_DISTRO_%s" % distro

ext_source_root = source_root

ext_modules=[
    Extension("syndicate",
              sources=["syndicate.pyx"],
              libraries=["syndicate"],
              library_dirs=[os.path.join(source_root, build_dir, "../lib")],             # libsyndicate local build
              include_dirs=[os.path.join(source_root, build_dir, "../include")],
              extra_compile_args=["-D__STDC_FORMAT_MACROS", "-D_FORTIFY_SOUCRE", "-D_BUILD_PYTHON", "-fstack-protector", "-fstack-protector-all", distro_switch],
              language="c++"),
    
    Extension("volume",
              sources=["volume.pyx"],
              libraries=["syndicate", "syndicate-ug"],
              library_dirs=[os.path.join(source_root, build_dir, "../lib")],
              include_dirs=[os.path.join(source_root, build_dir, "../include")],
              extra_compile_args=["-D__STDC_FORMAT_MACROS", "-D_FORTIFY_SOUCRE", "-D_BUILD_PYTHON", "-fstack-protector", "-fstack-protector-all", distro_switch],
              language="c++"),
]

setup(name='syndicate',
      version='0.1',
      description='Syndicate Python library',
      url='https://github.com/jcnelson/syndicate',
      author='Jude Nelson',
      author_email='syndicate@lists.cs.princeton.edu',
      license='Apache 2.0',
      ext_package='syndicate',
      ext_modules = ext_modules,
      packages = ['syndicate',
                  'syndicate.ms',
                  'syndicate.protobufs',
                  'syndicate.util',
                  'syndicate.observer',
                  'syndicate.observer.storage',
                  'syndicate.ag',
                  'syndicate.ag.curation',
                  'syndicate.ag.datasets',
                  'syndicate.ag.fs_driver_common',
                  'syndicate.ag.fs_driver_common.fs_backends',
                  'syndicate.ag.fs_driver_common.fs_backends.iplant_datastore',
                  'syndicate.rg',
                  'syndicate.rg.drivers',
                  'syndicate.rg.drivers.s3',
                  'syndicate.rg.drivers.disk'],
      package_dir = {
                  'syndicate.ms': os.path.join(ext_source_root, build_dir, 'syndicate/ms'),
                  'syndicate.protobufs': os.path.join(ext_source_root, build_dir, '../protobufs/python'),
                  'syndicate.util': os.path.join(ext_source_root, build_dir, 'syndicate/util'),
                  'syndicate.observer': os.path.join(ext_source_root, build_dir, 'syndicate/observer'),
                  'syndicate.observer.storage': os.path.join(ext_source_root, build_dir, 'syndicate/observer/storage'),
                  'syndicate.rg': os.path.join(ext_source_root, build_dir, 'syndicate/rg'),
                  'syndicate.rg.drivers': os.path.join(ext_source_root, build_dir, 'syndicate/rg/drivers'),
                  'syndicate.rg.drivers.s3': os.path.join(ext_source_root, build_dir, 'syndicate/rg/drivers/s3'),
                  'syndicate.rg.drivers.disk': os.path.join(ext_source_root, build_dir, 'syndicate/rg/drivers/disk'),
                  'syndicate.ag': os.path.join(ext_source_root, build_dir, 'syndicate/ag'),
                  'syndicate.ag.datasets': os.path.join(ext_source_root, build_dir, 'syndicate/ag/datasets'),
                  'syndicate.ag.curation': os.path.join(ext_source_root, build_dir, 'syndicate/ag/curation'),
                  'syndicate.ag.fs_driver_common': os.path.join(ext_source_root, build_dir, 'syndicate/ag/fs_driver_common'),
                  'syndicate.ag.fs_driver_common.fs_backends': os.path.join(ext_source_root, build_dir, 'syndicate/ag/fs_driver_common/fs_backends'),
                  'syndicate.ag.fs_driver_common.fs_backends.iplant_datastore': os.path.join(ext_source_root, build_dir, 'syndicate/ag/fs_driver_common/fs_backends/iplant_datastore')
      },
      cmdclass = {"build_ext": build_ext},
      zip_safe=False)
