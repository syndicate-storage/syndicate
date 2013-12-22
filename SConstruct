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
   "/usr/local/include/",
   "."
]

# default CPPFLAGS
CPPFLAGS = "-g -Wall -D__STDC_FORMAT_MACROS -D_FORTIFY_SOURCE"

# parse options
devel = False
for key, value in ARGLIST:
   if key == "DESTDIR":
      install_prefix = value
   if key == "CPPFLAGS":
      CPPFLAGS = value
   if key == "devel":
      if value == "true":
         CPPFLAGS += " -D_DEVELOPMENT"
         devel = True

# install directories
bin_install_dir = os.path.join( install_prefix, "bin" )
lib_install_dir = os.path.join( install_prefix, "lib" )
inc_install_dir = os.path.join( install_prefix, "include/syndicate" )

# begin build
env = Environment( 
   ENV = {'PATH': os.environ['PATH']},
   CPPFLAGS = Split(CPPFLAGS),
   CPPPATH = CPPPATH,
   toolpath = ['build/tools'],
   tools = ['default', 'protoc'],
   devel = devel
)

common.setup_env( env )

Export("env")

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
libsyndicate, libsyndicate_header_paths, libsyndicate_source_paths = SConscript( "libsyndicate/SConscript", variant_dir=libsyndicate_out )
env.Depends( libsyndicate_source_paths, protobuf_cc_files )  # libsyndicate requires protobufs to be built first

# alias installation targets for libsyndicate
libsyndicate_install_headers = env.Install( inc_install_dir, libsyndicate_header_paths + protobuf_header_paths )
libsyndicate_install_library = env.Install( lib_install_dir, libsyndicate ) 
libsyndicate_install_c_targets = [libsyndicate_install_headers, libsyndicate_install_library]

# main targets...
env.Alias( 'libsyndicate', [libsyndicate] )
env.Alias( 'libsyndicate-install', [libsyndicate_install_library, libsyndicate_install_headers] )

# ----------------------------------------
# UG build
ug_out = "build/out/bin/UG"
syndicatefs, syndicate_httpd, syndicate_ipc = SConscript( "UG/SConscript", variant_dir=ug_out )
ugs = [syndicatefs, syndicate_httpd, syndicate_ipc]
env.Depends( syndicatefs, libsyndicate )
env.Depends( syndicate_ipc, libsyndicate )
env.Depends( syndicate_httpd, libsyndicate )

env.Alias("syndicatefs", syndicatefs)
env.Alias("UG-ipc", syndicate_ipc)

# UG installation 
common.install_targets( env, 'UG-install', bin_install_dir, ugs )
env.Alias("UG", [syndicatefs, syndicate_httpd] )

# ----------------------------------------
# AG build
ag_out = "build/out/bin/AG"
ags = SConscript( "AG/SConscript", variant_dir=ag_out )
env.Depends( ags, libsyndicate )

# AG driver build
libAGcommon_out = "build/out/lib/AG/"
libAGcommon = SConscript( "AG/drivers/common/SConscript", variant_dir=libAGcommon_out )
env.Depends( libAGcommon, libsyndicate )
ag_common_install = env.Install( lib_install_dir, libAGcommon )
env.Alias( 'AG-common', libAGcommon )
env.Alias( 'AG-common-install', [ag_common_install] )

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
env.Alias( 'AG-drivers', [libAGSQLdriver, libAGshelldriver, libAGdiskdriver] )
env.Alias( 'AG-drivers-install', [ag_driver_shell_install, ag_driver_sql_install, ag_driver_disk_install] )

# AG Watchdog daemon
watchdog_daemon_out = "build/out/bin/AG/watchdog"
watchdog_daemon = SConscript( "AG/watchdog-daemon/SConscript", variant_dir=watchdog_daemon_out )
env.Alias( "AG-watchdog", watchdog_daemon )

# installation
common.install_targets( env, 'AG-install', bin_install_dir, ags )

# main targets....
env.Alias( 'AG', [libAGcommon, libAGdiskdriver, libAGSQLdriver, libAGshelldriver, watchdog_daemon, ags] )


# ----------------------------------------
# MS build
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
env.Depends(python_target, [python_files, protobuf_py_files, libsyndicate])

env.Alias("python", python_target)
env.Alias("python-install", python_install)

# ----------------------------------------
# Top-level build

env.Alias("syndicate", ["MS", "RG", "AG", "UG"])
env.Alias("syndicate-install", ["RG-install", "AG-install", "UG-install"])

# ----------------------------------------
# initialization

# set umask correctly
try:
   umask = os.umask(022)
except OSError:
   pass

Default(None)

