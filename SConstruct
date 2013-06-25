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
for key, value in ARGLIST:
   if key == "DESTDIR":
      install_prefix = value
   if key == "CPPFLAGS":
      CPPFLAGS = value

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
   tools = ['default', 'protoc']
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
Export("protobuf_py_files")      # needed by ms

# libsyndicate build
libsyndicate_out = "build/out/libsyndicate"
libsyndicate, libsyndicate_header_paths, libsyndicate_source_paths = SConscript( "libsyndicate/SConscript", variant_dir=libsyndicate_out )
env.Depends( libsyndicate_source_paths, protobufs )  # libsyndicate requires protobufs to be built first

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
# disk driver
if "AG/drivers/disk" in COMMAND_LINE_TARGETS:
    libAGdiskdriver_out = "build/out/AG/drivers/disk"
    libAGdiskdriver = SConscript( "AG/drivers/disk/SConscript", variant_dir=libAGdiskdriver_out )
    env.Depends( libAGdiskdriver, libsyndicate )

# ms build
ms_out = "build/out/ms"
ms = SConscript( "ms/SConscript", variant_dir=ms_out )
env.Depends( ms, protobuf_py_files )  # ms requires Python protobufs to be built first

# UG installation 
common.install_targets( env, 'UG-install', bin_install_dir, ugs )
env.Install( conf_install_dir, "conf/syndicate-UG.conf" )
env.Alias("UG-install", conf_install_dir )

# AG installation
common.install_targets( env, 'AG-install', bin_install_dir, ags )

# alias installation targets for libsyndicate
libsyndicate_install_headers = env.InstallHeader( inc_install_dir, libsyndicate_header_paths + protobuf_header_paths )
libsyndicate_install_library = env.InstallLibrary( lib_install_dir, libsyndicate )
env.Alias( 'libsyndicate-install', [libsyndicate_install_library, libsyndicate_install_headers] )

# alias installation targets for AG disk driver
if "AG-disk-driver-install" in COMMAND_LINE_TARGETS:
    ag_driver_disk_install = env.InstallLibrary( lib_install_dir, libAGdiskdriver )
    env.Alias( 'AG-disk-driver-install', [ag_driver_disk_install] )

# initialization

# set umask correctly
try:
   umask = os.umask(022)
except OSError:
   pass

Default(None)

