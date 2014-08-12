#!/usr/bin/python

import os
import sys
import traceback
import subprocess
import shlex

sys.path.append('.')
import package_info     # in the same directory

BUILD_FLAGS = package_info.BUILD_FLAGS
PACKAGE_INFO = package_info.PACKAGE_INFO
BUILD_ROOT = package_info.BUILD_ROOT
PACKAGE_ROOT = package_info.PACKAGE_ROOT

ORDER = getattr( package_info, "ORDER", None )

def do_cmd( command ):
   print command 
   cmd_parts = shlex.split( command )
   rc = subprocess.call( cmd_parts )
   return rc

def build_target( target_name ):
   command = "scons %s %s" % (BUILD_FLAGS, target_name)

   rc = do_cmd( command )
   if rc != 0:
      raise Exception( "Command failed: %s" % command )

   return rc


def install_target( target_name, dest_dir ):
   command = "rm -rf %s/*" % dest_dir

   rc = do_cmd( command )
   if rc != 0:
      raise Exception( "Command failed: %s" % command )

   command = "scons %s DESTDIR=%s %s" % (BUILD_FLAGS, dest_dir, target_name)

   rc = do_cmd( command ) 
   if rc != 0:
      raise Exception( "Command failed: %s" % command )
   
   return rc

def package_target( package_script, package_root, package_scripts_root ):
   command = "%s %s %s" % (package_script, package_root, package_scripts_root)

   rc = do_cmd( command )
   if rc != 0:
      raise Exception( "Command failed: %s" % command )
   
   return rc


def do_build_step( package_name, step_name, build_func ):
   try:
      rc = build_func()
      assert rc == 0, "Building package '%s' at step '%s' failed, rc = %s" % (package_name, step_name, rc)
   except Exception, e:
      print "------------------------------------------------"
      traceback.print_exc()
      print "------------------------------------------------"
      print "Build failed"
      sys.exit(1)


if __name__ == "__main__":

   if ORDER is None:
      # build all 
      ORDER = [p.name for p in PACKAGE_INFO]
   
   for package_name in ORDER:
      # find the package
      package = None
      for p in PACKAGE_INFO:
         if p.name == package_name:
            package = p
            break

      if package is None:
         print "---------------------------------------------"
         raise Exception("No such package %s" % package_name)

      name = package.name
      build = package.build_target
      install = package.install_target
      installdir = package.install_dir
      package_script = package.package_script
      package_root = package.package_root

      # optional
      package_scripts_target = getattr( package, "package_scripts_target", None)
      package_scripts_root = getattr( package, "package_scripts_root", None)

      config_target = getattr( package, "config_target", None )
      config_install_dir = getattr( package, "config_install_dir", None )

      msg = "Build package: %s" % name
      
      print "-" * len(msg)
      print msg
      print "-" * len(msg)

      os.chdir( BUILD_ROOT )
      do_build_step( name, "build", lambda: build_target( build ) )
      do_build_step( name, "install", lambda: install_target( install, installdir ) )

      if config_target is not None and config_install_dir is not None:
         do_build_step( name, "config-install", lambda: install_target( config_target, config_install_dir ) )

      if package_scripts_target is not None:
         do_build_step( name, "package-scripts", lambda: install_target( package_scripts_target, package_scripts_root ) )

      os.chdir( PACKAGE_ROOT )
      do_build_step( name, "package", lambda: package_target( package_script, package_root, package_scripts_root ) )

   print "Build complete"
