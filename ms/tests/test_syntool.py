#!/usr/bin/python

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

import os 
import sys
import syndicate.log
import syndicate.conf as conf
import syndicate.common.api as api
import shutil
import json 

log = syndicate.log.log


sys.path.append("../clients/syntool")
import syntool

TEST_ADMIN_ID = "admin@gmail.com"

TEST_CONFIG = ("""
[syndicate]
MSAPI=http://localhost:8080/api
user_id=%s
gateway_keys=gateway_keys/
volume_keys=volume_keys/
user_keys=user_keys/
""" % TEST_ADMIN_ID).strip()

TEST_ADMIN_PKEY_PATH = "./user_test_key.pem"

USER_ATTRS = ["email", "openid_url", "max_UGs", "max_RGs", "max_AGs", "max_volumes", "signing_public_key", "verifying_public_key"]

CONFIG_PATH = None

#-------------------------------
class Suite:
   def __init__(self, name, description):
      self.name = name 
      self.description = description
   
   def __call__(self, func):
      def inner(*args, **kw):
         log.info("--- Suite: %s ---" % self.name)
         return func(*args, **kw)
      
      inner.__name__ = func.__name__
      inner.name = self.name
      inner.description = self.description
      inner.test_suite = True
      return inner


#-------------------------------
def result( fmt ):
   print "[RESULT] %s" % fmt 
   
#-------------------------------
def write_config( base_dir, config_text ):
   config_path = os.path.join( base_dir, "syndicate.conf" )
   fd = open( config_path, "w" )
   fd.write( config_text )
   fd.close()
   
def build_argv( *args, **kw ):
   return list( [str(a) for a in args] ) + list( [str(k) + "=" + str(v) for (k, v) in kw.items()] )


def test( method_name, *args, **kw ):
   syntool_args = ['-c', CONFIG_PATH]
   if kw.has_key("syntool_args"):
      syntool_args.append( kw['syntool_args'] )
      
   argv = ['syntool.py'] + syntool_args +  [method_name] + build_argv( *args, **kw )
   log.info( "Test '%s"  % " ".join(argv) )
   
   return syntool.main( argv )


def load_config( conf_file ):
   argv = ['syntool.py', '-c', conf_file, "test"]
   
   config = syntool.load_options( argv )

   return config
   
   
def setup( test_config_text, test_user_id, test_user_pkey_path, testdir=None ):
   global CONFIG_PATH
   # set up a test directory 
   if testdir == None:
      testdir = os.tempnam( "/tmp", "test-syntool-" )
      os.mkdir( testdir )
      
   write_config( testdir, test_config_text )
   
   log.info("Test config in %s" % testdir )
   
   config_path = os.path.join( testdir, "syndicate.conf" )
   config = load_config( config_path )
   
   # copy over our test admin key
   shutil.copyfile( test_user_pkey_path, conf.object_signing_key_filename( config, "user", test_user_id ) )
   
   CONFIG_PATH = config_path
   return config


def abort( message, exception=None, cleanup=False ):
   log.error(message)
   
   if exception != None:
      log.exception(exception)
      
   if cleanup:
      pass
   
   sys.exit(1)


def check_result_present( result, required_keys ):
   for key in required_keys:
      assert result.has_key(key), "Missing key: %s" % key
      
      result_str = str(result[key])
      if len(result_str) > 50:
         result_str = result_str[:50]
         
      result("  %s == %s" % (key, result_str) )


def check_result_absent( result, absent_keys ):
   for key in absent_keys:
      assert not result.has_key( key ), "Present key: %s" % key
      

#-------------------------------
@Suite("create_user_bad_args", "Test creating users with missing or invalid arguments")
def test_create_user_bad_args():
   # test no arguments
   try:
      log.info("Create user: Missing arguments")
      ret = test( "create_user", "user@gmail.com" )
      abort("Created user with missing arguments")
   except Exception, e:
      result( e.message )
      pass
   
   print ""
   
   # test invalid arguments
   try:
      log.info("Create user: Invalid arguments")
      ret = test( "create_user", "not an email", "http://www.vicci.org/id/foo", "MAKE_SIGNING_KEY" )
      abort("Created user with invalid email address")
   except Exception, e:
      result( e.message )
      pass
   
   print ""
   
   return True


#-------------------------------
@Suite("create_users", "Try creating admins and regular users, ensuring that admins can create users; users cannot create admins; duplicate users are forbidden.")
def test_create_users():
   # make a normal user
   try:
      log.info("Create normal user")
      ret = test( "create_user", "user@gmail.com", "http://www.vicci.org/id/user@gmail.com", "MAKE_SIGNING_KEY" )
      
      check_result_present( ret, USER_ATTRS )
   except Exception, e:
      abort("Failed to create user@gmail.com", exception=e )
   
   print ""
   
   # make another normal user
   try:
      log.info("Create another normal user")
      ret = test( "create_user", "anotheruser@gmail.com", "http://www.vicci.org/id/anotheruser@gmail.com", "MAKE_SIGNING_KEY" )
      
      check_result_present( ret, USER_ATTRS )
   except Exception, e:
      abort("Failed to create anotheruser@gmail.com", exception=e )
   
   print ""
   
   # verify that we can't overwrite a user
   try:
      log.info("Create a user that already exists")
      ret = test( "create_user", "anotheruser@gmail.com", "http://www.vicci.org/id/anotheruser@gmail.com", "MAKE_SIGNING_KEY" )
      
      abort("Created a user over itself")
   except Exception, e:
      result(e.message)
      pass
   
   print ""
   
   # try to create an admin
   try:
      log.info("Create admin by admin")
      ret = test( "create_user", "anotheradmin@gmail.com", "http://www.vicci.org/id/anotheradmin@gmail.com", "MAKE_SIGNING_KEY" )
      
      check_result_present( ret, USER_ATTRS )
   except Exception, e:
      abort("Failed to create admin anotheradmin@gmail.com", exception=e )
   
   print ""
   
   # try to create an admin from a normal user (should fail)
   try:
      log.info("Unprivileged create admin")
      ret = test("create_user", "mal@gmail.com", "http://www.vicci.org/id/mal@gmail.com", "MAKE_SIGNING_KEY", syntool_args=['-u', 'user@gmail.com'], is_admin=True)
      abort("Unprivileged user created an admin account")
   except Exception, e:
      result(e.message)
      pass

   print ""
   

#-------------------------------
@Suite("read_users", "Test reading users.  Ensure the admin can read anyone, but a user can only read its account.")
def test_read_users():
   # admin can read all users
   try:
      log.info("Read user as admin")
      ret = test( "read_user", "user@gmail.com" )
      
      check_result_present( ret, USER_ATTRS )
   except Exception, e:
      abort("Admin failed to read user user@gmail.com", exception=e )
   
   print ""
   
   # user can read himself
   try:
      log.info("Read user's own account")
      ret = test( "read_user", "user@gmail.com", syntool_args=['-u', 'user@gmail.com'] )
      
      check_result_present( ret, USER_ATTRS )
   except Exception, e:
      abort("User failed to read its account", exception=e )
   
   print ""
   
   # user cannot read another account
   try:
      log.info("Read admin as user")
      ret = test( "read_user", "anotheradmin@gmail.com" )
      
      abort("Unprivileged user read another account")
   except Exception, e:
      result(e.message)
      pass
   
   print ""
   
   # cannot read users that don't exist
   try:
      log.info("Read nonexistent user")
      ret = test( "read_user", "none@gmail.com" )
      
      abort("Got data for a nonexistent account")
   except Exception, e:
      result(e.message)
      pass 
   
   print ""
   
   
#-------------------------------
@Suite("update_users", "Test updating users.  Ensure the admin can update anyone, but a user can only update itself.")
def test_update_users():
   pass

#-------------------------------
@Suite("delete_users", "Test deleting users.  Ensure the admin can delete anyone, but a user can only delete itself.  Also, verify that keys are revoked.")
def test_delete_users():
   # user can't delete an admin
   try:
      log.info("Delete admin with user account")
      ret = test( "delete_user", "anotheradmin@gmail.com", syntool_args=['-u', 'user@gmail.com'] )
      
      abort("Unprivileged user deleted an admin")
   except Exception, e:
      log.info(e.message)
      pass 
   
   # user can delete itself
   try:
      log.info("User deletes itself")
      ret = test("delete_user", "user@gmail.com", syntool_args=['-u', 'user@gmail.com'] )
      
      assert ret, "delete_user failed"
   except Exception, e:
      abort("User failed to delete itself", exception=e)
      
   # admin can delete any user
   try:
      log.info("Admin deletes anyone")
      ret = test("delete_user", "anotheradmin@gmail.com" )
      
      assert ret, "delete_user failed"
   except Exception, e:
      abort("Admin failed to delete another user", exception=e)
      
   

#-------------------------------
def get_test_suites( suite_list=None ):
   # get all test suites 
   if suite_list == None:
      suite_list = globals().keys()
      
   suites = []
   
   for suite_name in globals().keys():
      obj = globals().get(suite_name, None)
      if obj == None:
         continue
      
      if hasattr(obj, "test_suite"):
         if obj.name in suite_list:
            suites.append( obj )
      
   
   return suites 


#-------------------------------
def main( suites, test_config_dir=None ):
   config = setup( TEST_CONFIG, TEST_ADMIN_ID, TEST_ADMIN_PKEY_PATH, testdir=test_config_dir )
   
   if len(suites) == 0:
      # run all suites
      suite_funcs = get_test_suites()
      suites = [sf.name for sf in suite_funcs]
   
   if suites[0] == "list_suites":
      suite_funcs = get_test_suites()
      print "All test suites:"
      print "\n".join( ["  " + s.name + "\n      " + s.description for s in suite_funcs] )
      sys.exit(0)
      
   else:
      # run the suites
      suite_funcs = get_test_suites( suites )
      
      log.info("Run tests %s" % [sf.name for sf in suite_funcs])
      for sf in suite_funcs:
         sf()
         

#-------------------------------      
if __name__ == "__main__":
   test_dir = None
   if len(sys.argv) == 1:
      print "Usage: %s [-d testdir] suite [suite...]"
      sys.exit(1)
      
   suites = sys.argv[1:]
   
   # get suites and test config directory
   if len(sys.argv) > 2:
      if sys.argv[1] == '-d':
         test_dir = sys.argv[2]
         suites = sys.argv[3:]
      
   main( suites, test_config_dir=test_dir )
   
