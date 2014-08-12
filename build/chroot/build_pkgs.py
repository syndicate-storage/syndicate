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
   
   for package in PACKAGE_INFO:

      name = package.name
      build = package.build_target
      install = package.install_target
      installdir = package.install_dir
      package_script = package.package_script
      package_root = package.package_root
      package_scripts_root = package.package_scripts_root

      msg = "Build package: %s" % name
      
      print "-" * len(msg)
      print msg
      print "-" * len(msg)

      os.chdir( BUILD_ROOT )
      do_build_step( name, "build", lambda: build_target( build ) )
      do_build_step( name, "install", lambda: install_target( install, installdir ) )

      os.chdir( PACKAGE_ROOT )
      do_build_step( name, "package", lambda: package_target( package_script, package_root, package_scripts_root ) )

   print "Build complete"
