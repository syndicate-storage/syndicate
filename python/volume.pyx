"""
   Copyright 2015 The Trustees of Princeton University

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
import threading

class VolumeException(Exception):
   pass

# ------------------------------------------
cdef class VolumeHandle:
   """
      Python wrapper around UG_handle_t
   """

   cpdef uintptr_t handle_ptr

   cdef Init( self, UG_handle_t* handle ):
      self.handle_ptr = <uintptr_t>handle

   cpdef Get( self ):
      return self.handle_ptr

# ------------------------------------------
VolumeEntry = collections.namedtuple( "VolumeEntry", ["type", "name", "file_id", "ctime", "mtime", "manifest_mtime", "write_nonce", "xattr_nonce", "version",
                                                      "max_read_freshness", "max_write_freshness", "owner", "coordinator", "volume", "mode", "size", "generation", "capacity", "num_children"] )

VolumeStat = collections.namedtuple( "VolumeStat", ["st_mode", "st_ino", "st_dev", "st_nlink", "st_uid", "st_gid", "st_size", "st_atime", "st_mtime", "st_ctime"] )

# ------------------------------------------
cdef class Volume:
   """
      Python wrapper around a Volume
   """

   VOLUME_ENTRY_TYPE_FILE = FTYPE_FILE
   VOLUME_ENTRY_TYPE_DIR = FTYPE_DIR
   
   cdef UG_state* state_inst
   
   # ------------------------------------------
   def __cinit__(self):
      pass

   # ------------------------------------------
   def __dealloc__(self):
      UG_shutdown( self.state_inst )
      self.state_inst = NULL

   # ------------------------------------------
   def __init__( self, args, is_client ):

      '''
         Initialize UG, starting it in its own thread
      '''
      
      cdef char** argv = <char**>stdlib.malloc( (len(args) + 1) * sizeof(char*) )
      if argv == NULL:
         raise MemoryError()
      
      self.state_inst = UG_state_init( len(args), argv, is_client )

      if self.state_inst == NULL:      
         raise VolumeException("Failed to initialize UG")

      rc = UG_start( self.state_inst )
      if rc != 0:
         raise VolumeException("Failed to start UG")

   # ------------------------------------------
   cpdef create( self, path, mode ):
      cdef char* c_path = path
      cdef mode_t c_mode = mode 
      cdef int rc = 0

      cdef UG_handle_t* ret = UG_create( self.state_inst, c_path, c_mode, &rc )
      if ret == NULL:
         raise Exception("syndicate_create rc = %d" % rc)

      py_ret = VolumeHandle()
      py_ret.Init( ret )
      return py_ret

   # ------------------------------------------
   cpdef open( self, path, flags ):
      cdef char* c_path = path 
      cdef int c_flags = flags 
      cdef int rc = 0

      cdef UG_handle_t* ret = UG_open( self.state_inst, c_path, c_flags, &rc )
      if ret == NULL or rc != 0:
         raise Exception("syndicate_open rc = %d" % rc)
   
      py_ret = VolumeHandle()
      py_ret.Init( ret )
      return py_ret

   # ------------------------------------------
   cpdef read( self, handle, size ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp

      cdef size_t c_size = size

      cdef char* c_buf = <char*>stdlib.malloc( size * cython.sizeof(char) )
      if c_buf == NULL:
         raise MemoryError()

      cdef ssize_t c_read = UG_read( self.state_inst, c_buf, c_size, c_handle )
      
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
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp

      cdef size_t c_size = len(buf)

      cdef char* c_buf = buf

      cdef ssize_t c_write = UG_write( self.state_inst, c_buf, c_size, c_handle )

      if c_write < 0:
         raise Exception("syndicate_write rc = %d" % c_write)

      return c_write

   # ------------------------------------------
   cpdef seek( self, handle, offset, whence ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp

      cdef off_t c_offset = offset
      cdef int c_whence = whence

      cdef int rc = UG_seek( c_handle, c_offset, c_whence )

      return rc

   # ------------------------------------------
   cpdef close( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp
      
      cdef int close_rc = 0
      cdef int rc = UG_flush( self.state_inst, c_handle )
      
      if rc != 0:
         close_rc = UG_close( self.state_inst, c_handle )
         if close_rc != 0:
            return close_rc

      else:
         rc = UG_close( self.state_inst, c_handle )

      return rc

   # ------------------------------------------
   cpdef fsync( self, handle, datasync=1 ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp

      cdef int rc = UG_fsync( self.state_inst, datasync, c_handle )
      
      return rc

   # ------------------------------------------
   cpdef stat( self, path ):
      cdef char* c_path = path
      cdef stat statbuf
      
      cdef int rc = UG_getattr( self.state_inst, path, &statbuf )

      if rc == 0:
         py_stat = VolumeStat(st_mode = statbuf.st_mode,
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

      cdef int rc = UG_mkdir( self.state_inst, c_path, c_mode )
      return rc
   
   # ------------------------------------------
   cpdef unlink( self, path ):
      cdef char* c_path = path

      cdef int rc = UG_unlink( self.state_inst, c_path )
      
      return rc

   # ------------------------------------------
   cpdef rmdir( self, path ):
      cdef char* c_path = path

      cdef int rc = UG_rmdir( self.state_inst, c_path )
      
      return rc

   # ------------------------------------------
   cpdef opendir( self, path ):
      cdef char* c_path = path 
      cdef int rc = 0

      cdef UG_handle_t* ret = UG_opendir( self.state_inst, c_path, &rc )
      if ret == NULL or rc != 0:
         raise Exception("syndicate_opendir rc = %d" % rc)
   
      py_ret = VolumeHandle()
      py_ret.Init( ret )
      return py_ret

   # ------------------------------------------
   cpdef readdir( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp

      cdef UG_dir_listing_t c_dirs = NULL

      cdef int rc = UG_readdir( self.state_inst, &c_dirs, c_handle )
      if rc != 0:
         raise Exception("syndicate_readdir rc = %s" % rc)

      # get number of entries
      cdef char* name = NULL
      py_dirs = []
      i = 0
      while c_dirs[i] != NULL:
         name = fs_dir_entry_name( c_dirs[i] )
         py_name = name[:]
         dir_ent = VolumeEntry( type = fs_dir_entry_type( c_dirs[i] ),
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

      UG_free_dir_listing( c_dirs )
      return py_dirs

   # ------------------------------------------
   cpdef closedir( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp
      
      cdef int rc = UG_closedir( self.state_inst, c_handle )
      return rc
      