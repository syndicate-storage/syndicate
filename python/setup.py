#!/usr/bin/python

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

import os
import sys
import shutil

source_root = "../"
build_dir = ""

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

   i += 1

ext_source_root = source_root
ext_library = "libsyndicate"

ext_modules=[
    Extension("syndicate",
              sources=["syndicate.pyx"],
              libraries=["syndicate"],
              library_dirs=[os.path.join(source_root, build_dir, "lib/libsyndicate")],             # libsyndicate local build
              include_dirs=[os.path.join(source_root, build_dir, "lib/libsyndicate"), os.path.join(source_root, build_dir, "protobufs"), "/usr/include/syndicate"],
              extra_compile_args=["-D__STDC_FORMAT_MACROS", "-D_FORTIFY_SOUCRE"],
              language="c++") 
]

# get the list of drivers
driver_path = os.path.abspath( os.path.join(ext_source_root, "RG") )
sys.path.append( driver_path )

drivers = __import__("drivers")
driver_package_names = ["syndicate.rg.drivers." + name for name in drivers.__all__]

driver_package_paths = dict( [(package_name, os.path.join(ext_source_root, os.path.join("RG/drivers", package_name))) for package_name in drivers.__all__] )

setup(name='syndicate',
      version='0.1',
      description='Syndicate Python library',
      url='https://github.com/jcnelson/syndicate',
      author='Jude Nelson',
      author_email='syndicate@lists.cs.princeton.edu',
      license='Apache 2.0',
      ext_package='syndicate',
      ext_modules = ext_modules,
      packages = ['syndicate', 'syndicate.client', 'syndicate.client.common', 'syndicate.client.bin', 'syndicate.protobufs', 'syndicate.rg', 'syndicate.rg.drivers'] + driver_package_names,
      package_dir = dict({
                         'syndicate.client': os.path.join(ext_source_root, 'ms/clients/python'),
                         'syndicate.client.common': os.path.join(ext_source_root, 'ms/common'),
                         'syndicate.client.bin': os.path.join(ext_source_root, 'ms/clients/python/bin'),
                         'syndicate.protobufs': os.path.join(ext_source_root, build_dir, 'protobufs'),
                         'syndicate.rg': os.path.join(ext_source_root, 'RG'),
                         'syndicate.rg.drivers': os.path.join(ext_source_root, 'RG/drivers'),
                        }.items() + driver_package_paths.items()
                     ),
      cmdclass = {"build_ext": build_ext},
      zip_safe=False)
