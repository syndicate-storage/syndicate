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

from syndicate cimport *
from volume cimport *

cimport libc.stdlib as stdlib

cimport cython

import types
import errno
import os
import collections

# ------------------------------------------
cdef class SyndicateFileHandle:
   """
      Python wrapper around syndicate_handle_t
   """

   cpdef uintptr_t handle_ptr

   cdef Init( self, syndicate_handle_t* handle ):
      self.handle_ptr = <uintptr_t>handle

   cpdef Get( self ):
      return self.handle_ptr

# ------------------------------------------
SyndicateEntry = collections.namedtuple( "SyndicateEntry", ["type", "name", "file_id", "ctime", "mtime", "manifest_mtime", "write_nonce", "xattr_nonce", "version",
                                                             "max_read_freshness", "max_write_freshness", "owner", "coordinator", "volume", "mode", "size"] )

SyndicateStat = collections.namedtuple( "SyndicateStat", ["st_mode", "st_ino", "st_dev", "st_nlink", "st_uid", "st_gid", "st_size", "st_atime", "st_mtime", "st_ctime"] )

# ------------------------------------------
cdef class Volume:
   """
      Python interface to a Volume.
   """

   ENT_TYPE_FILE = FTYPE_FILE
   ENT_TYPE_DIR = FTYPE_DIR
   
   cdef syndicate_state state_inst
   cdef int wait_replicas
   
   # ------------------------------------------
   def __cinit__(self):
      pass

   # ------------------------------------------
   def __dealloc__(self):
      wr = -1
      if hasattr(self, "wait_replicas"):
         wr = self.wait_replicas
      
      syndicate_client_shutdown( &self.state_inst, self.wait_replicas )

   # ------------------------------------------
   def __init__( self, gateway_name=None,
                       config_file=None,
                       ms_url=None,
                       username=None,
                       password=None,
                       volume_name=None,
                       volume_key_pem=None,
                       gateway_pkey_pem=None,
                       user_pkey_pem=None,
                       gateway_pkey_decryption_password=None,
                       syndicate_pubkey_pem=None,
                       storage_root=None,
                       wait_replicas=-1 ):

      '''
         Initialize Volume client.
      '''

      cdef:
         char* c_gateway_name = NULL
         char* c_config_file = NULL
         char* c_ms_url = NULL
         char* c_username = NULL
         char* c_password = NULL
         char* c_volume_name = NULL
         char* c_volume_key_pem = NULL
         char* c_gateway_pkey_pem = NULL
         char* c_gateway_pkey_decryption_password = NULL
         char* c_storage_root = NULL
         char* c_syndicate_pubkey_pem = NULL
         syndicate_opts opts
         
      if gateway_name != None:
         c_gateway_name = gateway_name
      
      if ms_url != None:
         c_ms_url = ms_url

      if username != None:
         c_username = username 
         
      if password != None:
         c_password = password
         
      if volume_name != None:
         c_volume_name = volume_name 
         
      if config_file != None:
         c_config_file = config_file 

      if gateway_pkey_pem != None:
         c_gateway_pkey_pem = gateway_pkey_pem
       
      if gateway_pkey_decryption_password != None:
         c_gateway_pkey_decryption_password = gateway_pkey_decryption_password

      if storage_root != None:
         c_storage_root = storage_root
      
      if syndicate_pubkey_pem != None:
         c_syndicate_pubkey_pem = syndicate_pubkey_pem

      self.wait_replicas = wait_replicas

      memset( &opts, 0, sizeof(opts) )
      
      opts.gateway_name = gateway_name
      opts.ms_url = ms_url
      opts.username = username
      opts.volume_name = volume_name
      opts.config_file = config_file
      opts.storage_root = storage_root
      opts.syndicate_pubkey_pem = syndicate_pubkey_pem

      # NOTE: not mlock'ed!
      opts.gateway_pkey_pem.ptr = c_gateway_pkey_pem
      opts.gateway_pkey_pem.len = len(gateway_pkey_pem)
      
      # NOTE: not mlock'ed!
      opts.gateway_pkey_decryption_password.ptr = c_gateway_pkey_decryption_password
      opts.gateway_pkey_decryption_password.len = len(gateway_pkey_decryption_password)

      # NOTE: not mlock'ed!
      opts.password.ptr = c_password
      opts.password.len = len(password)
      
      if wait_replicas < 0:
         opts.flush_replicas = 1
      
      rc = syndicate_client_init( &self.state_inst, &opts )

      if rc != 0:
         raise Exception("syndicate_client_init rc = %s" % rc )


   # ------------------------------------------
   cpdef create( self, path, mode ):
      cdef char* c_path = path
      cdef mode_t c_mode = mode 
      cdef int rc = 0

      cdef syndicate_handle_t* ret = syndicate_create( &self.state_inst, c_path, c_mode, &rc )
      if ret == NULL:
         raise Exception("syndicate_create rc = %d" % rc)

      py_ret = SyndicateFileHandle()
      py_ret.Init( ret )
      return py_ret

   # ------------------------------------------
   cpdef open( self, path, flags ):
      cdef char* c_path = path 
      cdef int c_flags = flags 
      cdef int rc = 0

      cdef syndicate_handle_t* ret = syndicate_open( &self.state_inst, c_path, c_flags, &rc )
      if ret == NULL or rc != 0:
         raise Exception("syndicate_open rc = %d" % rc)
   
      py_ret = SyndicateFileHandle()
      py_ret.Init( ret )
      return py_ret

   # ------------------------------------------
   cpdef read( self, handle, size ):
      cpdef uintptr_t tmp = handle.Get()
      cdef syndicate_handle_t* c_handle = <syndicate_handle_t*>tmp

      cdef size_t c_size = size

      cdef char* c_buf = <char*>stdlib.malloc( size * cython.sizeof(char) )
      if c_buf == NULL:
         raise MemoryError()

      cdef ssize_t c_read = syndicate_read( &self.state_inst, c_buf, c_size, c_handle )
      
      if c_read < 0:
         stdlib.free( c_buf )
         raise Exception("syndicate_read rc = %d" % c_read)
      
      # NOTE: this can cause a MemoryError if the buffer is really big
      try:
         py_buf = c_buf[:c_read]
      finally:
         stdlib.free( c_buf )

      return py_buf

   # ------------------------------------------
   cpdef write( self, handle, buf ):
      cpdef uintptr_t tmp = handle.Get()
      cdef syndicate_handle_t* c_handle = <syndicate_handle_t*>tmp

      cdef size_t c_size = len(buf)

      cdef char* c_buf = buf

      cdef ssize_t c_write = syndicate_write( &self.state_inst, c_buf, c_size, c_handle )

      if c_write < 0:
         raise Exception("syndicate_write rc = %d" % c_write)

      return c_write

   # ------------------------------------------
   cpdef seek( self, handle, offset, whence ):
      cpdef uintptr_t tmp = handle.Get()
      cdef syndicate_handle_t* c_handle = <syndicate_handle_t*>tmp

      cdef off_t c_offset = offset
      cdef int c_whence = whence

      cdef int rc = syndicate_seek( c_handle, c_offset, c_whence )

      return rc

   # ------------------------------------------
   cpdef close( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef syndicate_handle_t* c_handle = <syndicate_handle_t*>tmp
      
      cdef int close_rc = 0
      cdef int rc = syndicate_flush( &self.state_inst, c_handle )
      
      if rc != 0:
         close_rc = syndicate_close( &self.state_inst, c_handle )
         if close_rc != 0:
            return close_rc

      else:
         rc = syndicate_close( &self.state_inst, c_handle )

      return rc

   # ------------------------------------------
   cpdef fsync( self, handle, datasync=1 ):
      cpdef uintptr_t tmp = handle.Get()
      cdef syndicate_handle_t* c_handle = <syndicate_handle_t*>tmp

      cdef int rc = syndicate_fsync( &self.state_inst, datasync, c_handle )
      
      return rc

   # ------------------------------------------
   cpdef stat( self, path ):
      cdef char* c_path = path
      cdef stat statbuf
      
      cdef int rc = syndicate_getattr( &self.state_inst, path, &statbuf )

      if rc == 0:
         py_stat = SyndicateStat(st_mode = statbuf.st_mode,
                                 st_ino = statbuf.st_ino,
                                 st_dev = statbuf.st_dev,
                                 st_nlink = statbuf.st_nlink,
                                 st_uid = statbuf.st_uid,
                                 st_gid = statbuf.st_gid,
                                 st_size = statbuf.st_size,
                                 st_atime = statbuf.st_atime,
                                 st_mtime = statbuf.st_mtime,
                                 st_ctime = statbuf.st_ctime )


         return py_stat

      else:
         return rc

   # ------------------------------------------
   cpdef mkdir( self, path, mode ):
      cdef char* c_path = path
      cdef mode_t c_mode = mode

      cdef int rc = syndicate_mkdir( &self.state_inst, c_path, c_mode )
      return rc
   
   # ------------------------------------------
   cpdef unlink( self, path ):
      cdef char* c_path = path

      cdef int rc = syndicate_unlink( &self.state_inst, c_path )
      
      return rc

   # ------------------------------------------
   cpdef rmdir( self, path ):
      cdef char* c_path = path

      cdef int rc = syndicate_rmdir( &self.state_inst, c_path )
      
      return rc

   # ------------------------------------------
   cpdef opendir( self, path ):
      cdef char* c_path = path 
      cdef int rc = 0

      cdef syndicate_handle_t* ret = syndicate_opendir( &self.state_inst, c_path, &rc )
      if ret == NULL or rc != 0:
         raise Exception("syndicate_opendir rc = %d" % rc)
   
      py_ret = SyndicateFileHandle()
      py_ret.Init( ret )
      return py_ret

   # ------------------------------------------
   cpdef readdir( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef syndicate_handle_t* c_handle = <syndicate_handle_t*>tmp

      cdef syndicate_dir_listing_t c_dirs = NULL

      cdef int rc = syndicate_readdir( &self.state_inst, &c_dirs, c_handle )
      if rc != 0:
         raise Exception("syndicate_readdir rc = %s" % rc)

      # get number of entries
      cdef char* name = NULL
      py_dirs = []
      i = 0
      while c_dirs[i] != NULL:
         name = fs_dir_entry_name( c_dirs[i] )
         py_name = name[:]
         dir_ent = SyndicateEntry(  type = fs_dir_entry_type( c_dirs[i] ),
                                    name = py_name,
                                    file_id = fs_dir_entry_file_id( c_dirs[i] ),
                                    ctime = (fs_dir_entry_ctime_sec( c_dirs[i] ), fs_dir_entry_ctime_nsec( c_dirs[i] )),
                                    mtime = (fs_dir_entry_mtime_sec( c_dirs[i] ), fs_dir_entry_mtime_nsec( c_dirs[i] )),
                                    manifest_mtime = (fs_dir_entry_manifest_mtime_sec( c_dirs[i] ), fs_dir_entry_manifest_mtime_nsec( c_dirs[i] )),
                                    write_nonce = fs_dir_entry_write_nonce( c_dirs[i] ),
                                    xattr_nonce = fs_dir_entry_xattr_nonce( c_dirs[i] ),
                                    version = fs_dir_entry_version( c_dirs[i] ),
                                    max_read_freshness = fs_dir_entry_max_read_freshness( c_dirs[i] ),
                                    max_write_freshness = fs_dir_entry_max_write_freshness( c_dirs[i] ),
                                    owner = fs_dir_entry_owner( c_dirs[i] ),
                                    coordinator = fs_dir_entry_coordinator( c_dirs[i] ),
                                    volume = fs_dir_entry_volume( c_dirs[i] ),
                                    mode = fs_dir_entry_mode( c_dirs[i] ),
                                    size = fs_dir_entry_size( c_dirs[i] ) )

         py_dirs.append( dir_ent )
         i += 1

      syndicate_free_dir_listing( c_dirs )
      return py_dirs

   # ------------------------------------------
   cpdef closedir( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef syndicate_handle_t* c_handle = <syndicate_handle_t*>tmp
      
      cdef int rc = syndicate_closedir( &self.state_inst, c_handle )
      return rc
      