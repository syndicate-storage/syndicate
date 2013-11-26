# SCons build script for Syndicate

import os 
import sys
import types

import SCons

# add common tools
sys.path.append( os.path.join( os.getcwd(), "build/tools/" ) )
import common

# installation prefix
install_prefix = "/usr/"

# default CPPPATH 
CPPPATH = [
   "#",
   "/usr/include/",
   "/usr/local/include/",
   "."
]

# default CPPFLAGS
CPPFLAGS = "-g -Wall"

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
conf_install_dir = os.path.join( install_prefix, "etc/syndicate" )

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

# protobuf build
protobuf_out = "build/out/protobufs"
protobufs, protobuf_header_paths = SConscript( "protobufs/SConscript", variant_dir=protobuf_out )
protobuf_cc_files = filter( lambda x: x.path.endswith(".cc"), protobufs )
protobuf_py_files = filter( lambda x: x.path.endswith(".py"), protobufs )

Export("protobuf_out")           # needed by libsyndicate
Export("protobuf_cc_files")      # needed by libsyndicate
Export("protobuf_py_files")      # needed by ms and rm

# libsyndicate build
libsyndicate_out = "build/out/libsyndicate"
libsyndicate, libsyndicate_python, libsyndicate_header_paths, libsyndicate_source_paths = SConscript( "libsyndicate/SConscript", variant_dir=libsyndicate_out )
env.Depends( libsyndicate_source_paths, protobuf_cc_files )  # libsyndicate requires protobufs to be built first
env.Depends( libsyndicate_python, protobuf_py_files )

# UG for shared library build
if "UG-shared" in COMMAND_LINE_TARGETS:
   ugshared_out = "build/out/UG-shared"
   ugshareds = SConscript( "UG-shared/SConscript", variant_dir=ugshared_out )
   env.Depends( ugshareds, libsyndicate )

# UG build
ug_out = "build/out/UG"
ugs = SConscript( "UG/SConscript", variant_dir=ug_out )
env.Depends( ugs, libsyndicate )

# AG build
ag_out = "build/out/AG"
ags = SConscript( "AG/SConscript", variant_dir=ag_out )
env.Depends( ags, libsyndicate )

# AG driver build
#if "AG/drivers/common" in COMMAND_LINE_TARGETS:
libAGcommon_out = "build/out/AG/drivers/common"
libAGcommon = SConscript( "AG/drivers/common/SConscript", variant_dir=libAGcommon_out )
env.Depends( libAGcommon, libsyndicate )

# disk driver
if "AG/drivers/disk" in COMMAND_LINE_TARGETS:
    libAGdiskdriver_out = "build/out/AG/drivers/disk"
    libAGdiskdriver = SConscript( "AG/drivers/disk/SConscript", variant_dir=libAGdiskdriver_out )
    env.Depends( libAGdiskdriver, libsyndicate )

# SQL driver
if "AG/drivers/sql" in COMMAND_LINE_TARGETS:
    libAGSQLdriver_out = "build/out/AG/drivers/sql"
    libAGSQLdriver = SConscript( "AG/drivers/sql/SConscript", variant_dir=libAGSQLdriver_out )
    env.Depends( libAGSQLdriver, libsyndicate)
    env.Depends( libAGSQLdriver, libAGcommon )

# Shell driver
if "AG/drivers/shell" in COMMAND_LINE_TARGETS:
    libAGshelldriver_out = "build/out/AG/drivers/shell"
    libAGshelldriver = SConscript( "AG/drivers/shell/SConscript", variant_dir=libAGshelldriver_out )
    env.Depends( libAGshelldriver, libsyndicate  )
    env.Depends( libAGshelldriver, libAGcommon  )

#Watchdog daemon
if "AG/watchdog-daemon" in COMMAND_LINE_TARGETS:
    watchdog_daemon_out = "build/out/AG/watchdog-daemon"
    watchdog_daemon = SConscript( "AG/watchdog-daemon/SConscript", variant_dir=watchdog_daemon_out )

# ms build
ms_out = "build/out/ms"
ms, client_libs, client_bin = SConscript( "ms/SConscript", variant_dir=ms_out )
env.Depends( ms, protobuf_py_files )  # ms requires Python protobufs to be built first
env.Depends( client_libs, ms )
env.Depends( client_bin, ms )

env.Alias( "ms-tools", [client_libs, client_bin] )

# replica_manager build
rm_out = "build/out/replica_manager"
rm = SConscript( "replica_manager/SConscript", variant_dir=rm_out )
env.Depends( rm, [libsyndicate_python, protobufs] )  # replica_manager requires Python protobofs to be built first

# UG installation 
common.install_targets( env, 'UG-install', bin_install_dir, ugs )
env.Install( conf_install_dir, "conf/syndicate-UG.conf" )
env.Alias("UG-install", conf_install_dir )

# AG installation
common.install_targets( env, 'AG-install', bin_install_dir, ags )

# alias installation targets for libsyndicate
libsyndicate_install_headers = env.InstallHeader( inc_install_dir, libsyndicate_header_paths + protobuf_header_paths )
libsyndicate_install_library = env.InstallLibrary( lib_install_dir, libsyndicate ) 
libsyndicate_install_python = env.Command( "syndicate.so", [], "cd %s/python && ./setup.py install" % libsyndicate_out )

env.Alias( 'libsyndicate-install', [libsyndicate_install_library, libsyndicate_install_headers] )
env.Alias( 'libsyndicate-python-install', [libsyndicate_install_python] )
env.Depends( libsyndicate_install_python, [libsyndicate_install_library, libsyndicate_install_headers] )

# alias installation targets for AG disk driver
if "AG-disk-driver-install" in COMMAND_LINE_TARGETS:
    libAGdiskdriver_out = "build/out/AG/drivers/disk"
    libAGdiskdriver = SConscript( "AG/drivers/disk/SConscript", variant_dir=libAGdiskdriver_out )
    ag_driver_disk_install = env.InstallLibrary( lib_install_dir, libAGdiskdriver )
    env.Alias( 'AG-disk-driver-install', [ag_driver_disk_install] )

# alias installation targets for AG disk driver
if "AG-SQL-driver-install" in COMMAND_LINE_TARGETS:
    libAGSQLdriver_out = "build/out/AG/drivers/sql"
    libAGSQLdriver = SConscript( "AG/drivers/sql/SConscript", variant_dir=libAGSQLdriver_out )
    ag_driver_sql_install = env.InstallLibrary( lib_install_dir, libAGSQLdriver )
    env.Alias( 'AG-SQL-driver-install', [ag_driver_sql_install] )

# alias installation targets for AG disk driver
if "AG-shell-driver-install" in COMMAND_LINE_TARGETS:
    libAGshelldriver_out = "build/out/AG/drivers/shell"
    libAGshelldriver = SConscript( "AG/drivers/shell/SConscript", variant_dir=libAGshelldriver_out )
    ag_driver_shell_install = env.InstallLibrary( lib_install_dir, libAGshelldriver )
    env.Alias( 'AG-shell-driver-install', [ag_driver_shell_install] )

# alias installation targets for AG watchdog daemon
if "watchdog-daemon-install" in COMMAND_LINE_TARGETS:
    watchdog_daemon_out = "build/out/AG/watchdog-daemon"
    watchdog_daemon = SConscript( "AG/watchdog-daemon/SConscript", variant_dir=watchdog_daemon_out )

common.install_targets( env, 'watchdog-daemon-install', bin_install_dir, ags )

#if "AG-common-install" in COMMAND_LINE_TARGETS:
ag_common_install = env.InstallLibrary( lib_install_dir, libAGcommon )
env.Alias( 'AG-common-install', [ag_common_install] )
# initialization

# set umask correctly
try:
   umask = os.umask(022)
except OSError:
   pass

Default(None)

