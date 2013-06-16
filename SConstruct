# SCons build script for Syndicate

import os 
import sys
import types

import SCons

# add common tools
sys.path.append( os.path.join( os.getcwd(), "build/tools/" ) )
import common

# default library install directory
lib_install_dir = "/usr/local/lib"

# default UG install directory
bin_install_dir = "/usr/local/bin"

# default header install directory
inc_install_dir = "/usr/local/include/syndicate"

# default CPPPATH 
CPPPATH = """
   #
   /usr/include/
   /usr/local/include/
   /usr/local/include/syndicate
   .
"""

# default CPPFLAGS
CPPFLAGS = "-g -Wall"



# begin build
env = Environment( 
   ENV = {'PATH': os.environ['PATH']},
   CPPFLAGS = Split(CPPFLAGS),
   CPPPATH = Split(CPPPATH),
   toolpath = ['build/tools'],
   tools = ['default', 'protoc']
)

common.setup_env( env )

Export("env")

# libsyndicate target
libsyndicate = SConscript( "libsyndicate/SConscript", variant_dir="build/out/libsyndicate" )
env.InstallLibrary( lib_install_dir, libsyndicate )
env.InstallHeader( inc_install_dir, ["build/out/libsyndicate/%s" % x for x in ['libsyndicate.h', 'libgateway.h', 'util.h', 'ms-client.h', "ms.pb.h", "serialization.pb.h"]] )

# UG target
ugs = SConscript( "UG/SConscript", variant_dir="build/out/UG" )

# AG target
ags = SConscript( "AG/SConscript", variant_dir="build/out/AG" )

# installation and aliases
common.install_targets( env, 'UG-install', bin_install_dir, ugs )
common.install_targets( env, 'AG-install', bin_install_dir, ags )

# alias installation targets for libsyndicate
env.Alias( 'libsyndicate-install', [lib_install_dir, inc_install_dir] )


# initialization

# usage function
def usage():
   print "Please specify a valid target:"
   for k in ['libsyndicate', 'libsyndicate-install', 'UG', 'UG-install', 'AG', 'AG-install']:
      print "   %s" % k

   return None


# vhet the list of targets
if len(COMMAND_LINE_TARGETS) == 0:
   usage()
   sys.exit(1)

for target in COMMAND_LINE_TARGETS:
   if target not in map(str, BUILD_TARGETS):
      usage( None, None, env )
      sys.exit(1)
      

# set umask correctly
try:
   umask = os.umask(022)
except OSError:
   pass

Default(None)

