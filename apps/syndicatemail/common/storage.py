#!/usr/bin/env python

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

import syndicate.client.common.log as Log
import syndicate.syndicate as c_syndicate
from syndicate.volume import Volume

import singleton

import os 
import errno
import collections
import json
import random
import binascii
import hashlib
import shutil
import pickle

log = Log.get_logger()

VOLUME_ROOT_DIR = None   # root directory on the Volume; overwritten at runtime
LOCAL_ROOT_DIR = None    # root directory on local disk
CACHE_DIR = ".syndicatemail.cache"        # root directory for cached data; overwritten at runtime

PATH_SALT = None  # loaded at runtime
PATH_SALT_FILENAME = "/config/salt"

GET_FROM_SESSION=True

VOLUME_STORAGE_DIRS = [
   "/config"
]

# -------------------------------------
def path_join( a, *b ):
   b_stripped = []
   for c in b:
      b_stripped.append( c.strip("/") )
   
   return os.path.join( a, *b_stripped )

# -------------------------------------
def volume_path( a, *b ):
   if VOLUME_ROOT_DIR is None:
      raise Exception("VOLUME_ROOT_DIR not set")
   
   parts = [a] + list(b)
   return path_join( VOLUME_ROOT_DIR, *parts )

# -------------------------------------
def local_path( a, *b ):
   if LOCAL_ROOT_DIR is None:
      raise Exception("LOCAL_ROOT_DIR not set")
   
   parts = [a] + list(b)
   return path_join( LOCAL_ROOT_DIR, *parts )

# -------------------------------------
def tuple_to_dict( tuple_inst ):
   fields_dict = {}
   for f in tuple_inst._fields:
      fields_dict[f] = getattr(tuple_inst, f)
   
   tuple_dict = {
      "type": tuple_inst.__class__.__name__,
      "fields": fields_dict
   }
   
   return tuple_dict

# -------------------------------------
def tuple_to_json( tuple_inst ):
   json_dict = tuple_to_dict( tuple_inst )
   
   try:
      json_str = json.dumps( json_dict )
   except Exception, e:
      log.error("Failed to serialize")
      raise e
   
   return json_str   
   
# -------------------------------------
def json_to_tuple( tuple_class, json_str ):
   json_dict = {}
   try:
      json_dict = json.loads( json_str )
   except Exception, e:
      log.error("Failed to unserialize")
      raise e
   
   for field_name in ["type", "fields"]:
      assert field_name in json_dict, "Missing field: %s" % field_name
   
   _type = str(json_dict["type"])
   _fields = json_dict["fields"]
   
   if _type != tuple_class.__name__:
      raise Exception("JSON encodes '%s'; expected '%s'" % (_type, tuple_class.__name__))
   
   assert isinstance( _fields, dict ), "Expected dictionary for 'fields'"
   
   # check fields
   for field_name in _fields.keys():
      assert field_name in tuple_class._fields, "Unexpected field '%s'" % field_name
   
   return tuple_class( **_fields )
      

# -------------------------------------
def volume_makedirs( volume, dir_abspath, mode=0700 ):
   pieces = dir_abspath.split("/")
   prefix = "/"
   for piece in pieces:
      prefix = os.path.join( prefix, piece )
      if not path_exists( prefix, volume=volume ):
         rc = volume.mkdir( prefix, mode )
         if rc != 0 and rc != -errno.EEXIST:
            raise Exception("Volume mkdir rc = %s"  % rc )
   
   return True
   

# -------------------------------------
def setup_dirs( dir_names, prefix="/", volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   if prefix is None:
      raise Exception("Invalid argument: paseed None as prefix")
   
   for dirname in dir_names:
      if dirname is None:
         raise Exception("Invalid argument: passed None as a directory")
      try:
         dir_path = None
         
         if volume is None:
            dir_path = local_path(prefix, dirname)
            os.makedirs( dir_path, mode=0700 )
            
         else:
            dir_path = volume_path(prefix, dirname)
            volume_makedirs( volume, dir_path, mode=0700 )
            
      except OSError, oe:
         if oe.errno != errno.EEXIST:
            log.error("Failed to mkdir '%s'" % dir_path )
            log.exception(oe)
            return False
         else:
            pass
         
      except Exception, e:
         log.error("Failed to mkdir '%s'" % dirname )
         log.exception(e)
         return False
   
   return True


# -------------------------------------
def volume_listdir( volume, dir_path ):
   dfd = volume.opendir( dir_path )
   dents = volume.readdir( dfd )
   volume.closedir( dfd )
   
   return [x.name for x in dents]


# -------------------------------------
def listdir( path, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   if volume is None:
      return os.listdir(path)
   else:
      return volume_listdir( volume, path)

# -------------------------------------
def volume_rmtree( volume, dir_abspath ):
   
   dfd = volume.opendir( dir_abspath )
   if dfd is None or dfd < 0:
      if isinstance(dfd, int) and dfd == -errno.ENOENT:
         return True
      
      raise Exception("Could not open %s" % dir_abspath)
   
   dents = volume.readdir( dfd )
   if dents is None or dents < 0:
      raise Exception("Could not read %s, rc = %s" % (dir_abspath, dents))
   
   volume.closedir( dfd )
   
   for dent in dents:
      if dent.name in [".", ".."]:
         continue
      
      if dent.type == volume.ENT_TYPE_DIR:
         child_dir_path = os.path.join( dir_abspath, dent.name )
         volume_rmtree( volume, child_dir_path )
         
      elif dent.type == volume.ENT_TYPE_FILE:
         child_ent_path = os.path.join( dir_abspath, dent.name )
         volume.unlink( child_ent_path )
         
   return True
            
   
# -------------------------------------
def delete_dirs( dirs, prefix="/", remove_contents=True, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   for dirname in dirs:
      if volume is None:
         dir_path = local_path( prefix, dirname )
         if remove_contents:
            try:
               shutil.rmtree( dir_path )
            except:
               return False
         
         else:
            try:
               os.rmdir( dir_path )
            except:
               return False
            
      else:
         dir_path = volume_path( prefix, dirname )
         if remove_contents:
            try:
               volume_rmtree( volume, dir_path )
            except Exception, e:
               log.exception(e)
               return False
         
         else:
            try:
               volume.rmdir( dir_path )
            except Exception, e:
               log.exception(e)
               return False
    
   return True


# -------------------------------------
def setup_volume_storage( volume_root_dir, modules, volume ):
   global VOLUME_ROOT_DIR
   global PATH_SALT
   global PATH_SALT_FILENAME
   
   all_volume_dirs = []
   VOLUME_ROOT_DIR = volume_root_dir 
   
   for mod in modules:
      volume_dirs = getattr(mod, "VOLUME_STORAGE_DIRS", None)
      if volume_dirs is not None:
         all_volume_dirs += volume_dirs

   
   # set up volume directories
   if volume_root_dir is not None:
      rc = setup_dirs( ["/"] + all_volume_dirs, volume=volume )
      if not rc:
         log.error("Volume setup_dirs failed")
         return rc
   
   
   salt_path = volume_path( PATH_SALT_FILENAME )
      
   if not path_exists( salt_path, volume=volume ):
      salt_dirname = os.path.dirname(PATH_SALT_FILENAME)
      rc = setup_dirs( [salt_dirname], volume=volume )
      if not rc:
         log.error("Failed to create '%s'" % dirname)
         return False
      
      # make a 512-bit (64-byte) salt
      salt = binascii.b2a_hex( os.urandom(64) )
      rc = write_file( salt_path, salt, volume=volume )
      if not rc:
         log.error("Failed to write '%s'" % salt_path )
         return False
      
      PATH_SALT = salt
   
      log.info("wrote salt to %s" % salt_path )      
      
   else:
      salt = read_file( salt_path, volume=volume )
      if salt is None:
         log.error("Failed to read '%s'" % salt_path )
         return False
   
      PATH_SALT = salt
      
   return True


# -------------------------------------
def setup_local_storage( local_dir, modules ):
   global LOCAL_ROOT_DIR
   global CACHE_DIR
   
   if local_dir is not None:
      LOCAL_ROOT_DIR = local_dir
   
   all_local_dirs = []
   for mod in modules:
      local_dirs = getattr(mod, "LOCAL_STORAGE_DIRS", None)
      if local_dirs is not None:
         all_local_dirs += local_dirs

   # set up local directories
   rc = setup_dirs( ["/"] + all_local_dirs, volume=None )
   if not rc:
      log.error("Local setup_dirs failed")
      return rc
   
   # set up cache
   rc = setup_dirs( ["/"] + [".syndicatemail.cache"], volume=None )
   if not rc:
      log.error("Local cache setup failed")
      return rc

   return True
   
# -------------------------------------
def setup_storage( volume_root_dir, local_dir, modules, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   rc = setup_local_storage( local_dir, modules )
   if not rc:
      log.error("Failed to set up local storage")
      return False
   
   rc = setup_volume_storage( volume_root_dir, modules, volume )
   if not rc:
      log.error("Failed to set up volume storage")
      return False
      
   return True


# -------------------------------------
def salt_string( name, salt=None, iterations=10000 ):
   global PATH_SALT
   
   if salt is None:
      salt = PATH_SALT
   
   if not PATH_SALT:
      raise Exception("call setup_storage() first")
   
   m = hashlib.sha256()
   for i in xrange(0,iterations):
      m.update( PATH_SALT )
      m.update( name )
   
   return m.hexdigest()


# -------------------------------------
def read_file( file_path, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   if volume is None:
      try:
         fd = open( file_path, "r" )
      except:
         log.error("Failed to open '%s' for reading" % file_path)
         return None
      
      try:
         buf = fd.read()
      except:
         log.error("Failed to read '%s'" % file_path)
         try:
            fd.close()
         except:
            pass 
         
         return None
      
      try:
         fd.close()
         return buf
      except:
         log.error("Failed to close '%s'" % file_path)
         return None
      
   else:
      try:
         statbuf = volume.stat( file_path )
         if isinstance(statbuf, int) and statbuf < 0:
            raise Exception("volume.stat rc = %d", statbuf)
         
         size = statbuf.st_size

         fd = volume.open( file_path, os.O_RDONLY )
         if fd is None:
            raise Exception("Failed to open Volume file %s" % file_path)
         
         buf = volume.read( fd, size )
         volume.close( fd )
         return buf 
      
      except Exception, e:
         log.exception(e)
         log.error("Failed to read Volume file %s" % file_path)
         return None

   
   
# -------------------------------------
def write_file( file_path, data, volume=GET_FROM_SESSION, create_mode=0600 ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   if volume is None:
      try:
         fd = open( file_path, "w+" )
      except Exception, e:
         log.exception(e)
         log.error("Failed to open '%s' for writing" % file_path)
         return False
      
      try:
         fd.write(data)
         fd.flush()
      except Exception, e:
         log.exception(e)
         log.error("Failed to write '%s'" % file_path)
         try:
            os.unlink(file_path)
         except:
            pass 
         
         try:
            fd.close()
         except:
            pass
         
         return False
      
      try:
         fd.close()
      except:
         log.error("Failed to close '%s'" % file_path)
         try:
            os.unlink(file_path)
         except:
            pass
         return False
      
      return True
   
   else:
      try:
         fd = volume.create( file_path, create_mode )
         if fd < 0:
            # try to open?
            fd = volume.open( file_path, os.O_WRONLY | os.O_TRUNC )
            
         if fd < 0:
            raise Exception("Failed to open Volume file %s for writing: rc = %d" % (file_path, fd) )
         
         rc = volume.write( fd, data ) 
         if rc != len(data):
            raise Exception("Volume write rc = %s (expected %s)" % (rc, len(data)))
         
         rc = volume.close( fd )
         if rc != 0:
            raise Exception("Volume close rc = %s" % rc)
         
         return True
      except Exception, e:
         log.exception(e)
         log.error("Failed to write Volume file %s" % file_path )
         return False
      
   
# -------------------------------------
def encrypt_data( pubkey_pem, data ):
   rc, enc_data = c_syndicate.encrypt_data( pubkey_pem, data )
   if rc != 0:
      log.error("encrypt_data rc = %s" % rc)
      return None
   
   return enc_data
   
# -------------------------------------
def decrypt_data( privkey_pem, enc_data ):
   rc, data = c_syndicate.decrypt_data( privkey_pem, enc_data )
   if rc != 0:
      log.error("decrypt_data rc = %s" % rc)
      return None
   
   return data
      
# -------------------------------------
def read_encrypted_file( privkey_pem, file_path, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   # get file data
   try:
      enc_data = read_file( file_path, volume=volume )
      if enc_data == None:
         log.error( "No data for %s" % file_path )
         return None
      
   except Exception, e:
      log.error("read_file(%s) failed" % file_path)
      log.exception(e)
      return None
   
   return decrypt_data( privkey_pem, enc_data )

# -------------------------------------
def write_encrypted_file( pubkey_pem, file_path, data, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   enc_data = encrypt_data( pubkey_pem, data )
   if enc_data is None:
      log.error("encrypt_data returned None")
      return False
   
   try:
      rc = write_file( file_path, enc_data, volume=volume )
      if not rc:
         log.error("write_file failed")
         return False
      
      return True
   
   except Exception, e:
      log.error("write_file(%s) failed" % file_path)
      log.exception(e)
      return False
      

# -------------------------------------
def delete_file( file_path, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   if volume is None:
      try:
         os.unlink( file_path )
      except OSError, oe:
         if oe.errno != errno.EEXIST:
            return False
         else:
            log.exception(oe)
            return True
      
      except Exception, e:
         log.exception(e)
         return False
      
      return True
   
   else:
      try:
         volume.unlink( file_path )
         return True
      except Exception, e:
         log.exception(e)
         return False


# -------------------------------------
def erase_file( file_path, volume=GET_FROM_SESSION ):
   # unlike delete_file, overwrite the file data with random bits a few times if local 
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
   
   if volume is None:
      try:
         size = os.stat(file_path).st_size
         fd = open(file_path, "w")
         
         for i in xrange(0,10):
            fd.seek(0)
            
            # overwrite with junk
            buf = ''.join(chr(random.randint(0,255)) for i in xrange(0,size))
            fd.write( buf )
            fd.flush()

         fd.close()
         
         # now safe to unlink
         os.unlink( file_path )
         return True 
      except Exception, e:
         log.exception(e)
         return False
      
   else:
      return delete_file( file_path, volume=volume )

# -------------------------------------
def path_exists( file_path, volume=GET_FROM_SESSION ):
   if volume == GET_FROM_SESSION:
      volume = singleton.get_volume()
      
   if volume is None:
      return os.path.exists( file_path )
   
   else:
      try:
         statbuf = volume.stat( file_path )
         if statbuf is None:
            return False
         elif isinstance(statbuf, int) and statbuf < 0:
            return False
         else:
            return True
      except Exception, e:
         return False
      
# -------------------------------------
def cache_path( cache_name ):
   ret = local_path( CACHE_DIR, cache_name )
   return ret

# -------------------------------------
def purge_cache( cache_name ):
   # purge cache
   try:
      cpath = cache_path( cache_name )
      os.unlink( cpath )
         
   except Exception, e:
      log.info("No cache to purge")
      pass
   
   return True

# -------------------------------------
def cache_data( pubkey_str, cache_name, data ):
   global CACHE_DIR
   
   if "/" in cache_name:
      setup_dirs( [os.path.dirname(cache_name)], prefix=CACHE_DIR, volume=None )
      
   try:
      data_serialized = pickle.dumps( data )
   except Exception, e:
      log.error("Failed to serialize data for caching")
      return False 
   
   return write_encrypted_file( pubkey_str, cache_path( cache_name ), data_serialized, volume=None )
   

# -------------------------------------
def get_cached_data( privkey_str, cache_name ):
   
   cp = cache_path( cache_name )
   if path_exists( cp, volume=None ):
      data_serialized = read_encrypted_file( privkey_str, cp, volume=None )
      if data_serialized != None:
         # cache hit
         try:
            data = pickle.loads(data_serialized)
         except Exception, e:
            log.warning("Failed to deserialize cache")
            purge_cache( cache_name )
            return None
         
         else:
            return data

   else:
      return None


# -------------------------------------
if __name__ == "__main__":
   
   fake_module = collections.namedtuple( "FakeModule", ["VOLUME_STORAGE_DIRS", "LOCAL_STORAGE_DIRS"] )
   fake_vol = singleton.do_test_volume( "/tmp/storage-test/volume" )
   singleton.set_volume( fake_vol )
   
   print "------- setup --------"
   fake_mod = fake_module( LOCAL_STORAGE_DIRS=['/testroot-local'], VOLUME_STORAGE_DIRS=['/testroot-volume'] )
   assert setup_storage( "/apps/syndicatemail/data", "/tmp/storage-test/local", [fake_mod] ), "setup_storage failed"
   
   foo_class = collections.namedtuple("Foo", ["bar", "baz"])
   goo_class = collections.namedtuple("Xyzzy", ["foo", "baz"])
   
   foo = foo_class( bar="a", baz="b" )
   goo = goo_class( foo="c", baz="d" )
   
   print "------- serialization --------"
   print "foo == %s" % str(foo)
   print "goo == %s" % str(goo)
   
   foo_json = tuple_to_json( foo )
   print "foo_json == %s" % foo_json
   
   goo_json = tuple_to_json( goo )
   print "goo_json == %s" % goo_json
   
   foo2 = json_to_tuple( foo_class, foo_json )
   goo2 = json_to_tuple( goo_class, goo_json )
   
   print "foo2 == %s" % str(foo2)
   print "goo2 == %s" % str(goo2)
   
   print "------ file I/O -------"
   
   pubkey_str = """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxwhi2mh+f/Uxcx6RuO42
EuVpxDHuciTMguJygvAHEuGTM/0hEW04Im1LfXldfpKv772XrCq+M6oKfUiee3tl
sVhTf+8SZfbTdR7Zz132kdP1grNafGrp57mkOwxjFRE3FA23T1bHXpIaEcdhBo0R
rXyEnxpJmnLyNYHaLN8rTOig5WFbnmhIZD+xCNtG7hFy39hKt+vNTWK98kMCOMsY
QPywYw8nJaax/kY5SEiUup32BeZWV9HRljjJYlB5kMdzeAXcjQKvn5y47qmluVmx
L1LRX5T2v11KLSpArSDO4At5qPPnrXhbsH3C2Z5L4jqStdLYB5ZYZdaAsaRKcc8V
WpsmzZaFExJ9Nj05sDS1YMFMvoINqaPEftS6Be+wgF8/klZoHFkuslUNLK9k2f65
A7d9Fn/B42n+dCDYx0SR6obABd89cR8/AASkZl3QKeCzW/wl9zrt5dL1iydOq2kw
JtgiKSCt6m7Hwx2kwHBGI8zUfNMBlfIlFu5CP+4xLTOlRdnXqYPylT56JQcjA2CB
hGBRJQFWVutrVtTXlbvT2OmUkRQT9+P5wr0c7fl+iOVXh2TwfaFeug9Fm8QWoGyP
GuKX1KO5JLQjcNTnZ3h3y9LIWHsCTCf2ltycUBguq8Mwzb5df2EkOVgFeLTfWyR2
lPCia/UWfs9eeGgdGe+Wr4sCAwEAAQ==
-----END PUBLIC KEY-----
""".strip()
   
   privkey_str = """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
""".strip()

   
   data = "abcde"
   path = volume_path( "/test" )
   
   rc = write_encrypted_file( pubkey_str, path, data )
   
   if not rc:
      raise Exception("write_encrypted_file failed")
   
   buf = read_encrypted_file( privkey_str, path )
   
   if not buf:
      raise Exception("read_encrypted_file failed")
   
   if buf != data:
      raise Exception("data not equal: got '%s', expected '%s'" % (buf, data))
   
   delete_file( path )
   
   print "------------- cache --------------"
   
   cache_data( pubkey_str, "/cache/test", "poop")
   dat = get_cached_data( privkey_str, "/cache/test" )
   
   assert dat == "poop", "get_cached_data( cache_data( %s ) ) == %s" % ("poop", dat)
   
   purge_cache( "/cache/test" )
   