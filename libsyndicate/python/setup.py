#!/usr/bin/env python

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

import os
import shutil

ext_name="syndicate"

# clean previous build
for root, dirs, files in os.walk(".", topdown=False):
    for name in files:
        if (name.startswith(ext_name) and not(name.endswith(".pyx") or name.endswith(".pxd"))):
            os.remove(os.path.join(root, name))
    for name in dirs:
        if (name == "build"):
            shutil.rmtree(name)

ext_modules=[
    Extension("syndicate",
              sources=["syndicate.pyx"],
              libraries=["syndicate"],
              library_dirs=["../"],             # libsyndicate local build
              include_dirs=["../", "../../protobufs", "/usr/include/syndicate"],
              extra_compile_args=["-D__STDC_FORMAT_MACROS"],
              language="c++") 
]

setup(
  name = ext_name,
  cmdclass = {"build_ext": build_ext},
  ext_modules = ext_modules
)
