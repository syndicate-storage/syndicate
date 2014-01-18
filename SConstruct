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
   "/usr/local/lib",
   "/usr/lib"
]

# default CPPFLAGS
CPPFLAGS = "-g -Wall -D__STDC_FORMAT_MACROS -fstack-protector -fstack-protector-all"
LIBS = ""
LINK_FLAGS = ""

# parse options
devel = False
extra_args = {}
for key, value in ARGLIST:
   if key == "DESTDIR":
      install_prefix = value
   elif key == "CPPFLAGS":
      CPPFLAGS = value
   elif key == "devel":
      if value == "true":
         CPPFLAGS += " -D_DEVELOPMENT"
         devel = True
   else:
      extra_args[key] = value

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

   return env_nacl 

Export("make_nacl_env")

# install directories
bin_install_dir = os.path.join( install_prefix, "bin" )
lib_install_dir = os.path.join( install_prefix, "lib" )
inc_install_dir = os.path.join( install_prefix, "include/libsyndicate" )

nacl_lib_install_dir = None
nacl_env = None

env = Environment( 
   ENV = {'PATH': os.environ['PATH']},
   CPPFLAGS = Split(CPPFLAGS),
   CPPPATH = CPPPATH,
   LIBPATH = LIBPATH,
   LIBS = LIBS,
   LINK_FLAGS = LINK_FLAGS,
   toolpath = ['build/tools'],
   tools = ['default', 'protoc'],
   devel = devel
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
libsyndicate, libsyndicate_nacl, libsyndicate_header_paths, libsyndicate_source_paths = SConscript( "libsyndicate/SConscript", variant_dir=libsyndicate_out )

libsyndicate_protobuf_deps = [protobufs, protobuf_cc_files]
for libsyndicate_target in [libsyndicate, libsyndicate_nacl, libsyndicate_header_paths, libsyndicate_source_paths]:
   if libsyndicate_target is not None:
      env.Depends( libsyndicate_target, libsyndicate_protobuf_deps )

# alias installation targets for libsyndicate
libsyndicate_install_headers = env.Install( inc_install_dir, libsyndicate_header_paths + protobuf_header_paths )
libsyndicate_install_library = env.Install( lib_install_dir, [libsyndicate] ) 
libsyndicate_install_c_targets = [libsyndicate_install_headers, libsyndicate_install_library]

libsyndicate_install_nacl = None
if libsyndicate_nacl is not None:
   libsyndicate_install_nacl = env.Install( nacl_lib_install_dir, [libsyndicate_nacl] )

   env.Alias( 'libsyndicate-nacl', [libsyndicate_nacl] )
   env.Alias( 'libsyndicate-nacl-install', [libsyndicate_install_nacl] )

# main targets...
env.Alias( 'libsyndicate', [libsyndicate, libsyndicate] )
env.Alias( 'libsyndicate-install', [libsyndicate_install_library, libsyndicate_install_headers] )


# ----------------------------------------
# UG build
ug_out = "build/out/bin/UG"
syndicatefs, syndicate_httpd, syndicate_ipc, libUG, UG_nacl = SConscript( "UG/SConscript", variant_dir=ug_out )

ugs_bin = [syndicatefs, syndicate_httpd, syndicate_ipc]
ugs_lib = [libUG]
ug_aliases = [syndicatefs, syndicate_httpd, syndicate_ipc, libUG]

env.Depends( syndicatefs, libsyndicate )
env.Depends( syndicate_ipc, libsyndicate )
env.Depends( syndicate_httpd, libsyndicate )
env.Depends( libUG, libsyndicate )
env.Depends( UG_nacl, libsyndicate_nacl )

env.Alias("syndicatefs", syndicatefs)
env.Alias("UG-httpd", syndicate_httpd)
env.Alias("libUG", libUG)
env.Alias("UG-ipc", syndicate_ipc)
env.Alias("UG-nacl", UG_nacl)

# UG installation 
common.install_targets( env, 'UG-install', bin_install_dir, ugs_bin )
common.install_targets( env, 'libUG-install', lib_install_dir, ugs_lib )
env.Alias("UG", ug_aliases )

# ----------------------------------------
# AG build
ag_out = "build/out/bin/AG"
ags = SConscript( "AG/SConscript", variant_dir=ag_out )
env.Depends( ags, libsyndicate )

# AG driver build
libAGcommon_out = "build/out/lib/AG/"
libAGcommon = SConscript( "AG/drivers/common/SConscript", variant_dir=libAGcommon_out )
env.Depends( libAGcommon, libsyndicate )
env.Alias( 'AG-common', libAGcommon )

# AG disk driver
libAGdiskdriver_out = "build/out/lib/AG/drivers/disk"
libAGdiskdriver = SConscript( "AG/drivers/disk/SConscript", variant_dir=libAGdiskdriver_out )
ag_driver_disk_install = env.Install( lib_install_dir, libAGdiskdriver )
env.Alias( 'AG-disk-driver', libAGdiskdriver )
env.Alias( 'AG-disk-driver-install', [ag_driver_disk_install] )
env.Depends( libAGdiskdriver, libAGcommon  )

# AG SQL driver
libAGSQLdriver_out = "build/out/lib/AG/drivers/sql"
libAGSQLdriver = SConscript( "AG/drivers/sql/SConscript", variant_dir=libAGSQLdriver_out )
ag_driver_sql_install = env.Install( lib_install_dir, libAGSQLdriver )
env.Alias( 'AG-sql-driver', libAGSQLdriver )
env.Alias( 'AG-sql-driver-install', [ag_driver_sql_install] )
env.Depends( libAGSQLdriver, libAGcommon )

# AG Shell driver
libAGshelldriver_out = "build/out/lib/AG/drivers/shell"
libAGshelldriver = SConscript( "AG/drivers/shell/SConscript", variant_dir=libAGshelldriver_out )
ag_driver_shell_install = env.Install( lib_install_dir, libAGshelldriver )
env.Alias( 'AG-shell-driver', libAGshelldriver )
env.Alias( 'AG-shell-driver-install', [ag_driver_shell_install] )
env.Depends( libAGshelldriver, libAGcommon )

# All drivers
ag_drivers = [libAGSQLdriver, libAGshelldriver, libAGdiskdriver]
env.Alias( 'AG-drivers', ag_drivers )
#env.Alias( 'AG-drivers-install', [ag_driver_shell_install, ag_driver_sql_install, ag_driver_disk_install] )

# AG Watchdog daemon
watchdog_daemon_out = "build/out/bin/AG/watchdog"
watchdog_daemon = SConscript( "AG/watchdog-daemon/SConscript", variant_dir=watchdog_daemon_out )
env.Alias( "AG-watchdog", watchdog_daemon )

# installation
common.install_targets( env, 'AG-common-install', lib_install_dir, libAGcommon )
common.install_targets( env, 'AG-install', bin_install_dir, ags )
common.install_targets( env, 'AG-install', lib_install_dir, ag_drivers )
common.install_targets( env, 'AG-drivers-install', lib_install_dir, ag_drivers )

# main targets....
env.Alias( 'AG', [libAGcommon, libAGdiskdriver, libAGSQLdriver, libAGshelldriver, watchdog_daemon, ags] )


# ----------------------------------------
# MS build
# Only parse the SConscript if we need to, since it performs argument validation.

if "MS" in COMMAND_LINE_TARGETS:
   ms_server_out = "build/out/ms"
   ms_server = SConscript( "ms/SConscript.server", variant_dir=ms_server_out )
   env.Depends( ms_server, protobuf_py_files )  # ms requires Python protobufs to be built first

   env.Alias( "MS-server", ms_server )

   # MS clients build
   ms_clients_bin_out = "build/out/bin/ms"
   ms_client_bin = SConscript( "ms/SConscript.client", variant_dir=ms_clients_bin_out )

   env.Alias( "MS-clients", [ms_client_bin] )

   # main targets....
   env.Alias( "MS", [ms_server, ms_client_bin] )
   env.Alias( "ms", [ms_server, ms_client_bin] )

# ----------------------------------------
# RG build

# replica gateway server build
rg_out = "build/out/bin/RG"
rg_server = SConscript( "RG/SConscript", variant_dir=rg_out )

env.Alias("RG", [rg_server])

common.install_targets( env, "RG-install", bin_install_dir, rg_server )

# ----------------------------------------
# Python build 

syndicate_python_out = "build/out/python"
python_target, python_install, python_files = SConscript("python/SConscript", variant_dir=syndicate_python_out)
env.Depends(python_target, [python_files, protobuf_py_files, libsyndicate, libUG])

env.Alias("syndicate-python", python_target)
env.Alias("syndicate-python-install", python_install)

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

