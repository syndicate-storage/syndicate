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

# SCons build script for Syndicate

import os 
import sys
import types

import SCons

# add common tools
sys.path.append( os.path.join( os.getcwd(), "build/tools/" ) )
import common

# installation prefix
install_prefix = "/usr/local"

# default CPPPATH 
CPPPATH = [
   "#",
   "/usr/include/",
   "."
]

# for protobufs...
LIBPATH = [
   "/usr/lib/x86_64-linux-gnu/",
   "/usr/lib/i386-linux-gnu/",
   "/usr/local/lib",
   "/usr/lib"
]

# default CPPFLAGS
CPPFLAGS = "-g -Wall -D__STDC_FORMAT_MACROS -fstack-protector -fstack-protector-all -pthread -rdynamic"
LIBS = ""
LINK_FLAGS = ""

# parse options
devel = False
valgrind_fixes = True
old_boost = False
firewall = False
extra_args = {}

for key, value in ARGLIST:
   if key == "DESTDIR":
      install_prefix = value

   elif key == "CPPFLAGS":
      CPPFLAGS = value
   
   # development flag
   elif key == "devel":
      if value == "true":
         CPPFLAGS += " -D_DEVELOPMENT"
         devel = True

   # disable valgrind fix-ups flag 
   elif key == "no-valgrind-fixes":
      if value == "true":
         valgrind_fixes = False

   # firewall flag - set true if this system works behind firewall
   elif key == "firewall":
      if value == "true":
         CPPFLAGS += " -D_FIREWALL"
         firewall = True

   # PlanetLab (Fedora 12) flag
   elif key == "old_boost":
      if value == "true":
         old_boost = True

   else:
      extra_args[key] = value

# modify CPPFLAGS to disable features 
if not valgrind_fixes:
   CPPFLAGS += " -D_NO_VALGRIND_FIXES"

# deduce the host linux distro
def deduce_distro():
   distro = "UNKNOWN"

   try:
      fd = os.popen("lsb_release -i")
      distro_id_text = fd.read()
      fd.close()
   except:
      print "WARN: failed to run 'lsb_release -i'.  Cannot deduce distribution (assuming defaults)"
      return distro

   _, distro = distro_id_text.split("\t")
   distro = distro.strip().upper()
   
   return distro

if not extra_args.has_key("DISTRO"):
   extra_args["DISTRO"] = deduce_distro()
   CPPFLAGS += " -D_DISTRO_%s" % extra_args["DISTRO"]

# TODO: possibly isolate this somewhere else?
# select the appropriate JSON library
if extra_args["DISTRO"] == "DEBIAN":
   extra_args['LIBJSON'] = "json-c"

# setup environment for Google Native Client
def make_nacl_env( env, NACL_TOOLCHAIN, PEPPER_ROOT, arch ):
   # Native Client library
   import os
   env_nacl = env.Clone()

   cc = arch + "-nacl-gcc"
   cxx = arch + "-nacl-g++"
   env_nacl.Replace(CC = os.path.join(NACL_TOOLCHAIN, "bin/" + cc))
   env_nacl.Replace(CXX = os.path.join(NACL_TOOLCHAIN, "bin/" + cxx))

   nacl_inc = [
      os.path.join(PEPPER_ROOT, "include"),
      os.path.join(NACL_TOOLCHAIN, "%s-nacl/include" % arch),
      os.path.join(NACL_TOOLCHAIN, "%s-nacl/usr/include" % arch),
   ]

   pepper_release = "Debug"
   if not devel:
      pepper_release = "Release"

   archbits = "64"
   if arch != "x86_64":
      archbits = "32"

   libdir = "lib%s" % archbits 
   libcdir = "glibc_x86_%s" % archbits

   nacl_libpath = [
      os.path.join(PEPPER_ROOT, "lib/%s/%s" % (libcdir, pepper_release)),
      os.path.join(NACL_TOOLCHAIN, "%s-nacl/usr/lib" % arch),
      os.path.join(NACL_TOOLCHAIN, "%s-nacl/%s" % (arch, libdir))
   ]

   env_nacl.Replace(CPPPATH = Split("# .") + nacl_inc)
   env_nacl.Replace(LIBPATH = nacl_libpath)
   env_nacl.Append(CPPFLAGS = "-D_SYNDICATE_NACL_")

   # preserve PYTHONPATH and PATH
   env_nacl.Replace( ENV = {'PATH': os.getenv("PATH"), "PYTHONPATH": os.getenv("PYTHONPATH")} )
   return env_nacl 

Export("make_nacl_env")

# install directories
bin_install_dir = os.path.join( install_prefix, "bin" )
lib_install_dir = os.path.join( install_prefix, "lib" )
inc_install_dir = os.path.join( install_prefix, "include/libsyndicate" )
etc_install_dir = os.path.join( install_prefix, "etc" )
pkg_install_dir = os.path.join( install_prefix, "pkg" )

nacl_lib_install_dir = None
nacl_env = None

env = Environment( 
   ENV = {'PATH': os.getenv('PATH'), 'PYTHONPATH': os.getenv('PYTHONPATH')},
   CPPFLAGS = Split(CPPFLAGS),
   CPPPATH = CPPPATH,
   LIBPATH = LIBPATH,
   LIBS = LIBS,
   LINK_FLAGS = LINK_FLAGS,
   toolpath = ['build/tools'],
   tools = ['default', 'protoc'],
   devel = devel,
   firewall = firewall,
   old_boost = old_boost,   # for the UG
   install_prefix = install_prefix,
)

common.setup_env( env )

nacl_env = None

if extra_args.has_key("NACL_TOOLCHAIN") and extra_args.has_key("arch") and extra_args.has_key("PEPPER_ROOT"):
   NACL_TOOLCHAIN = extra_args['NACL_TOOLCHAIN']
   PEPPER_ROOT = extra_args['PEPPER_ROOT']
   arch = extra_args['arch']

   nacl_lib_install_dir = os.path.join(NACL_TOOLCHAIN, "%s-nacl/usr/lib" % arch)

   nacl_env = make_nacl_env( env, NACL_TOOLCHAIN, PEPPER_ROOT, arch )

Export("nacl_env")

# begin build

Export("env")
Export("extra_args")

# ----------------------------------------
# protobuf build
protobuf_out = "build/out/protobufs"
protobufs, protobuf_header_paths = SConscript( "protobufs/SConscript", variant_dir=protobuf_out )
protobuf_cc_files = filter( lambda x: x.path.endswith(".cc"), protobufs )
protobuf_py_files = filter( lambda x: x.path.endswith(".py"), protobufs )

Export("protobuf_out")           # needed by libsyndicate
Export("protobuf_cc_files")      # needed by libsyndicate
Export("protobuf_py_files")      # needed by MS and RG


# ----------------------------------------
# libsyndicate build
libsyndicate_out = "build/out/lib/libsyndicate"
libsyndicate_driver_out = "build/out/lib/syndicate-drivers/volume"
libsyndicate_driver_install = os.path.join( lib_install_dir, "syndicate-drivers/volume" )

libsyndicate, libsyndicate_nacl, libsyndicate_header_paths, libsyndicate_scrypt_header_paths, libsyndicate_ms_client_header_paths, libsyndicate_source_paths = SConscript( "libsyndicate/SConscript", variant_dir=libsyndicate_out )
libsyndicate_drivers = SConscript( "libsyndicate/drivers/SConscript", variant_dir=libsyndicate_driver_out )

# all libsyndicate targets depends on protobufs
libsyndicate_protobuf_deps = [protobufs, protobuf_cc_files]
for libsyndicate_target in [libsyndicate, libsyndicate_nacl, libsyndicate_header_paths, libsyndicate_source_paths]:
   if libsyndicate_target is not None:
      env.Depends( libsyndicate_target, libsyndicate_protobuf_deps )

# alias installation targets for libsyndicate
libsyndicate_install_headers = env.Install( inc_install_dir, libsyndicate_header_paths + protobuf_header_paths )
libsyndicate_install_scrypt_headers = env.Install( os.path.join( inc_install_dir, "scrypt" ), libsyndicate_scrypt_header_paths )
libsyndicate_install_ms_client_headers = env.Install( os.path.join( inc_install_dir, "ms" ), libsyndicate_ms_client_header_paths )

libsyndicate_install_library = env.Install( lib_install_dir, [libsyndicate] ) 
libsyndicate_install_drivers = env.Install( libsyndicate_driver_install, [libsyndicate_drivers] )

libsyndicate_install_nacl = None
if libsyndicate_nacl is not None:
   libsyndicate_install_nacl = env.Install( nacl_lib_install_dir, [libsyndicate_nacl] )

   env.Alias( 'libsyndicate-nacl', [libsyndicate_nacl] )
   env.Alias( 'libsyndicate-nacl-install', [libsyndicate_install_nacl] )

# main targets...
env.Alias( 'libsyndicate', [libsyndicate] )
env.Alias( 'libsyndicate-install', [libsyndicate_install_library, libsyndicate_install_headers, libsyndicate_install_scrypt_headers, libsyndicate_install_ms_client_headers] )

env.Alias( 'libsyndicate-drivers', [libsyndicate_drivers] )
env.Alias( 'libsyndicate-drivers-install', [libsyndicate_install_drivers] )

# ----------------------------------------
# UG build
libsyndicateUG_inc_install_dir = os.path.join( install_prefix, "include/libsyndicateUG" )
libsyndicateUG_inc_fs_install_dir = os.path.join( install_prefix, "include/libsyndicateUG/fs" )

ug_out = "build/out/bin/UG"
ug_driver_out = "build/out/lib/UG/drivers"
syndicatefs, syndicate_httpd, syndicate_ipc, libsyndicateUGclient, libsyndicateUG, libsyndicateUG_headers, libsyndicateUG_fs_headers, syndicate_watchdog, UG_nacl = SConscript( "UG/SConscript", variant_dir=ug_out )
ug_drivers = SConscript( "UG/drivers/SConscript", variant_dir=ug_driver_out )

ugs_bin = [syndicatefs, syndicate_httpd, syndicate_ipc, syndicate_watchdog]
ugs_lib = [libsyndicateUGclient, libsyndicateUG]
ug_aliases = [syndicatefs, syndicate_httpd, syndicate_ipc, libsyndicateUGclient, libsyndicateUG, syndicate_watchdog]

env.Depends( syndicatefs, libsyndicate )
env.Depends( syndicate_ipc, libsyndicate )
env.Depends( syndicate_httpd, libsyndicate )
env.Depends( libsyndicateUG, libsyndicate )
env.Depends( libsyndicateUGclient, libsyndicate )
env.Depends( UG_nacl, libsyndicate_nacl )

env.Alias("syndicatefs", syndicatefs)
env.Alias("UG-httpd", syndicate_httpd)
env.Alias("libsyndicateUG", libsyndicateUG)
env.Alias("libsyndicateUGclient", libsyndicateUGclient)
env.Alias("UG-ipc", syndicate_ipc)
env.Alias("UG-nacl", UG_nacl)
env.Alias("UG-drivers", ug_drivers )

# UG installation 
common.install_targets( env, 'UG-install', bin_install_dir, ugs_bin )
common.install_targets( env, 'UG-drivers-install', lib_install_dir, ug_drivers )

libsyndicateUG_install_library = env.Install( lib_install_dir, [libsyndicateUG, libsyndicateUGclient] )
libsyndicateUG_install_headers = env.Install( libsyndicateUG_inc_install_dir, libsyndicateUG_headers )
libsyndicateUG_install_fs_headers = env.Install( libsyndicateUG_inc_fs_install_dir, libsyndicateUG_fs_headers )

env.Alias( "libsyndicateUG-install", [libsyndicateUG_install_library, libsyndicateUG_install_headers, libsyndicateUG_install_fs_headers] )
env.Alias("UG", ug_aliases )

# ----------------------------------------
# AG build
ag_out = "build/out/bin/AG"
ags = SConscript( "AG/SConscript", variant_dir=ag_out )
env.Depends( ags, libsyndicate )

# AG disk driver
libAGdiskdriver_out = "build/out/lib/AG/drivers/disk"
libAGdiskdriver = SConscript( "AG/drivers/disk/SConscript", variant_dir=libAGdiskdriver_out )
ag_driver_disk_install = env.Install( lib_install_dir, libAGdiskdriver )
env.Alias( 'AG-disk-driver', libAGdiskdriver )
env.Alias( 'AG-disk-driver-install', [ag_driver_disk_install] )

# AG disk-polling driver
libAGdiskpollingdriver_out = "build/out/lib/AG/drivers/disk_polling"
libAGdiskpollingdriver = SConscript( "AG/drivers/disk_polling/SConscript", variant_dir=libAGdiskpollingdriver_out )
ag_driver_disk_polling_install = env.Install( lib_install_dir, libAGdiskpollingdriver )
env.Alias( 'AG-disk-polling-driver', libAGdiskpollingdriver )
env.Alias( 'AG-disk-polling-driver-install', [ag_driver_disk_polling_install] )

# AG Shell driver
libAGshelldriver_out = "build/out/lib/AG/drivers/shell"
libAGshelldriver = SConscript( "AG/drivers/shell/SConscript", variant_dir=libAGshelldriver_out )
ag_driver_shell_install = env.Install( lib_install_dir, libAGshelldriver )
env.Alias( 'AG-shell-driver', libAGshelldriver )
env.Alias( 'AG-shell-driver-install', [ag_driver_shell_install] )

# AG curl driver 
libAGcurldriver_out = "build/out/lib/AG/drivers/curl"
libAGcurldriver = SConscript( "AG/drivers/curl/SConscript", variant_dir=libAGcurldriver_out )
ag_driver_curl_install = env.Install( lib_install_dir, libAGcurldriver )
env.Alias( 'AG-curl-driver', libAGcurldriver )
env.Alias( 'AG-curl-driver-install', [ag_driver_curl_install] )

# All drivers
#ag_drivers = [libAGcurldriver, libAGshelldriver, libAGdiskdriver, libAGdiskpollingdriver]
ag_drivers = [libAGcurldriver, libAGshelldriver, libAGdiskdriver]

# installation
common.install_targets( env, 'AG-bin-install', bin_install_dir, ags )
common.install_targets( env, 'AG-drivers-install', os.path.join(lib_install_dir, "syndicate/AG"), ag_drivers )

# main targets....
env.Alias('AG', [ags, ag_drivers])
env.Alias( 'AG-drivers', ag_drivers )
env.Alias('AG-install', ['AG-bin-install', 'AG-drivers-install'] )

# ----------------------------------------
# MS build
# Only parse the SConscript if we need to, since it performs argument validation.

ms_aliases = []

if "MS" in COMMAND_LINE_TARGETS:
   ms_server_out = "build/out/ms"
   ms_server = SConscript( "ms/SConscript.server", variant_dir=ms_server_out )
   env.Depends( ms_server, protobuf_py_files )  # ms requires Python protobufs to be built first

   env.Alias( "MS-server", ms_server )
   ms_aliases.append( ms_server )

# MS clients build
ms_clients_bin_out = "build/out/bin/ms"
ms_client_bin, ms_client_bin_install = SConscript( "ms/SConscript.client", variant_dir=ms_clients_bin_out )

env.Alias( "MS-clients", [ms_client_bin] )
env.Alias( "MS", ms_aliases + [ms_client_bin] )

common.install_targets( env, 'MS-clients-install', bin_install_dir, ms_client_bin_install )

# ----------------------------------------
# RG build

# replica gateway server build
rg_out = "build/out/bin/RG"
rg_server = SConscript( "RG/SConscript", variant_dir=rg_out )

env.Alias("RG", [rg_server])

common.install_targets( env, "RG-install", bin_install_dir, rg_server )

# ----------------------------------------
# Automount daemon build

automount_out = "build/out/bin/automount"
automount_daemon = SConscript( "automount/SConscript", variant_dir=automount_out )

env.Alias("syndicated", [automount_daemon])

common.install_targets( env, "syndicated-install", bin_install_dir, automount_daemon )

# ----------------------------------------
# OpenCloud-specific automount daemon config 

opencloud_out = "build/out/opencloud"
opencloud_automount_etc_files = SConscript( "automount/opencloud/SConscript", variant_dir=opencloud_out )

env.Alias("syndicated-opencloud-etc", [automount_daemon, opencloud_automount_etc_files] )

common.install_tree( env, "syndicated-opencloud-install-etc", etc_install_dir, opencloud_automount_etc_files, opencloud_out + "/etc" )

# ----------------------------------------
# OpenCloud-specific installation scripts 

opencloud_pkg_out = "build/out/pkg/opencloud/syndicated"
opencloud_automount_pkgscripts = SConscript( "automount/opencloud/pkg/SConscript", variant_dir=opencloud_pkg_out )

env.Alias("syndicated-opencloud-pkg", opencloud_automount_pkgscripts )

common.install_tree( env, "syndicated-opencloud-install-pkg", pkg_install_dir, opencloud_automount_pkgscripts, opencloud_pkg_out + "/pkg" )

# ----------------------------------------
# Python build 

syndicate_python_out = "build/out/python"
python_target, python_install, python_files = SConscript("python/SConscript", variant_dir=syndicate_python_out)
env.Depends(python_target, [python_files, protobuf_py_files, libsyndicate, libsyndicateUG])

env.Alias("syndicate-python", python_target)
env.Alias("python-syndicate", "syndicate-python")

env.Alias("syndicate-python-install", python_install)
env.Alias("python-syndicate-install", "syndicate-python-install")

# ----------------------------------------
# Top-level build

env.Alias("syndicate", ["RG", "AG", "UG"])
env.Alias("syndicate-install", ["RG-install", "AG-install", "UG-install"])


# ----------------------------------------
# initialization

# set umask correctly
try:
   umask = os.umask(022)
except OSError:
   pass

Default(None)

