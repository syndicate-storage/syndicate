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

def do_cmd( command, shell=False ):
   print command 

   if not shell:
      cmd_parts = shlex.split( command )
      rc = subprocess.call( cmd_parts, shell=shell )

   else:
      rc = subprocess.call( command, shell=shell )
   
   return rc

def build_target( target_name ):
   command = "scons %s %s" % (BUILD_FLAGS, target_name)

   rc = do_cmd( command )
   if rc != 0:
      raise Exception( "Command failed: %s" % command )

   return rc


def install_target( target_name, dest_dir, remove_old=True ):

   if remove_old:
      command = "rm -rf %s/*" % dest_dir

      rc = do_cmd( command, shell=True )
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
  
   if len(sys.argv) > 1:
      ORDER = sys.argv[1:]
    
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
      package_script = package.package_script
      package_root = package.package_root

      # optional
      build = getattr( package, "build_target", None)
      install = getattr( package, "install_target", None)
      installdir = getattr( package, "install_dir", None)
      package_scripts_target = getattr( package, "package_scripts_target", None)
      package_scripts_root = getattr( package, "package_scripts_root", None)
      config_target = getattr( package, "config_target", None )
      config_install_dir = getattr( package, "config_install_dir", None )

      msg = "Build package: %s" % name
      
      print "-" * len(msg)
      print msg
      print "-" * len(msg)

      remove_old = True

      os.chdir( BUILD_ROOT )
      
      if build is not None:
         do_build_step( name, "build", lambda: build_target( build ) )

      if install is not None:
         do_build_step( name, "install", lambda: install_target( install, installdir, remove_old=remove_old ) )
         remove_old = False

      if config_target is not None and config_install_dir is not None:
         do_build_step( name, "config-install", lambda: install_target( config_target, config_install_dir, remove_old=remove_old ) )
         remove_old = False

      if package_scripts_target is not None:
         do_build_step( name, "package-scripts", lambda: install_target( package_scripts_target, package_scripts_root, remove_old=remove_old ) )
         remove_old = False 

      os.chdir( PACKAGE_ROOT )

      if package_script is not None:
         do_build_step( name, "package", lambda: package_target( package_script, package_root, package_scripts_root ) )

   print "Build complete"
