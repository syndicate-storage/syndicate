# SCons build script for Syndicate

import os 
import sys
import types

import SCons

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




env = Environment( 
   ENV = {'PATH': os.environ['PATH']},
   CPPFLAGS = Split(CPPFLAGS),
   CPPPATH = Split(CPPPATH),
   toolpath = ['build/tools'],
   tools = ['default', 'protoc']
)


# install a set of files with the given permissions
from SCons.Script.SConscript import SConsEnvironment
SConsEnvironment.Chmod = SCons.Action.ActionFactory(os.chmod, lambda dest, mode: 'Chmod("%s", 0%o)' % (dest, mode))

def InstallPerm(env, dest, files, perm):
    obj = env.Install(dest, files)
    for i in obj:
        env.AddPostAction(i, env.Chmod(str(i), perm))
    return dest

SConsEnvironment.InstallPerm = InstallPerm

# installers for binaries, headers, and libraries
SConsEnvironment.InstallProgram = lambda env, dest, files: InstallPerm(env, dest, files, 0755)
SConsEnvironment.InstallHeader = lambda env, dest, files: InstallPerm(env, dest, files, 0644)
SConsEnvironment.InstallLibrary = lambda env, dest, files: InstallPerm(env, dest, files, 0644)

# install a list of targets and set up aliases
def install_targets( alias, dir, targets ):
   flatten = lambda lst: reduce(lambda l, i: l + flatten(i) if isinstance(i, (list, tuple, SCons.Node.NodeList)) else l + [i], lst, [])
   targets = flatten( targets )

   for target in targets:
      bn = os.path.basename( target.path )
      bn_install_path = os.path.join( dir, bn )
      
      env.InstallProgram( dir, target )
      env.Alias( alias, bn_install_path )


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
install_targets( 'UG-install', bin_install_dir, ugs )
install_targets( 'AG-install', bin_install_dir, ags )

# alias installation targets for libsyndicate
env.Alias( 'libsyndicate-install', [lib_install_dir, inc_install_dir] )

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

