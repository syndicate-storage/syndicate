#!/usr/bin/python

import sys
import os
from collections import namedtuple

class PackageInfo( object ):
   def __init__(self, **kw):
      for (k, v) in kw.items():
         setattr(self, k, v)

BASE_DIR = "/root/syndicate"
BUILD_ROOT = "/root/syndicate/syndicate"
PACKAGE_ROOT = "/root/syndicate"
BUILD_FLAGS = "devel=true"

PACKAGE_INFO = [
   PackageInfo( name            = "libsyndicate",
                build_target    = "libsyndicate",
		install_target  = "libsyndicate-install",
		install_dir     = "%s/libsyndicate-root/usr" % BASE_DIR,
                package_root    = "%s/libsyndicate-root" % BASE_DIR,
		package_script  = "%s/libsyndicate-deb.sh" % BASE_DIR ),

   PackageInfo( name            = "libsyndicate-ug",
                build_target    = "libsyndicateUG",
                install_target  = "libsyndicateUG-install",
                install_dir     = "%s/libsyndicateUG-root/usr" % BASE_DIR,
                package_root    = "%s/libsyndicateUG-root" % BASE_DIR,
                package_script  = "%s/libsyndicateUG-deb.sh" % BASE_DIR ),

   PackageInfo( name            = "syndicate-ug",
                build_target    = "UG",
                install_target  = "UG-install",
                install_dir     = "%s/syndicate-UG-root/usr" % BASE_DIR,
                package_root    = "%s/syndicate-UG-root" % BASE_DIR,
                package_script  = "%s/syndicate-UG-deb.sh" % BASE_DIR ),

   PackageInfo( name            = "syndicate-ag",
                build_target    = "AG",
                install_target  = "AG-install",
                install_dir     = "%s/syndicate-AG-root/usr" % BASE_DIR,
                package_root    = "%s/syndicate-AG-root" % BASE_DIR,
                package_script  = "%s/syndicate-AG-deb.sh" % BASE_DIR ),

   PackageInfo( name            = "syndicate-rg",
                build_target    = "RG",
                install_target  = "RG-install",
                install_dir     = "%s/syndicate-RG-root/usr" % BASE_DIR,
                package_root    = "%s/syndicate-RG-root" % BASE_DIR,
                package_script  = "%s/syndicate-RG-deb.sh" % BASE_DIR ),

   PackageInfo( name            = "ms-clients",
                build_target    = "MS-clients",
                install_target  = "MS-clients-install",
                install_dir     = "%s/syndicate-MS-clients-root/usr" % BASE_DIR,
                package_root    = "%s/syndicate-MS-clients-root" % BASE_DIR,
                package_script  = "%s/syndicate-MS-clients-deb.sh" % BASE_DIR ),

   PackageInfo( name            = "syndicated-opencloud",
                build_target    = "syndicated",
                install_target  = "syndicated-install",
                install_dir     = "%s/syndicated-opencloud-root/usr" % BASE_DIR,
                config_target   = "syndicated-opencloud-install-etc",
                config_install_dir = "%s/syndicated-opencloud-root" % BASE_DIR,
                package_root    = "%s/syndicated-opencloud-root" % BASE_DIR,
                package_script  = "%s/syndicated-opencloud-deb.sh" % BASE_DIR,
                package_scripts_target = "syndicated-opencloud-install-pkg",
                package_scripts_root = "%s/syndicated-opencloud-pkg/" % BASE_DIR ),

   PackageInfo( name            = "python-syndicate",
                build_target    = "python-syndicate",
                install_target  = "python-syndicate-install",
                install_dir     = "%s/python-syndicate-root/usr" % BASE_DIR,
                package_root    = "%s/python-syndicate-root/" % BASE_DIR,
                package_script  = "%s/python-syndicate-deb.sh" % BASE_DIR ),

   PackageInfo( name            = "syndicate-opencloud", 
                build_target    = None, 
                install_target  = None,
                install_dir     = None,
                package_root    = "%s/syndicate-opencloud-root" % BASE_DIR,
                package_script  = "%s/syndicate-opencloud-deb.sh" % BASE_DIR )
]
