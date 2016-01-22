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

   # cpdef uintptr_t handle_ptr

   cdef Init( self, UG_handle_t* handle ):
      self.handle_ptr = <uintptr_t>handle

   cpdef Get( self ):
      return self.handle_ptr

# ------------------------------------------
VolumeEntry = collections.namedtuple( "VolumeEntry", ["type", "name", "file_id", "ctime", "mtime", "manifest_mtime", "write_nonce", "xattr_nonce", "version",
                                                      "max_read_freshness", "max_write_freshness", "owner", "coordinator", "volume", "mode", "size", "generation", "capacity", "num_children",
                                                      "xattr_hash", "ent_sig"] )

cdef md_entry_to_VolumeEntry( md_entry* ent ):
   py_name = None 
   if ent.name != NULL:
      py_name = ent.name[:]

   py_xattr_hash = None 
   if ent.xattr_hash != NULL:
      py_sig = ent.xattr_hash[:32]      # SHA256

   py_ent_sig = None
   if ent.ent_sig != NULL:
      py_ent_sig = ent.ent_sig[:ent.ent_sig_len]

   ve = VolumeEntry( type = ent.type,
                     name = py_name,
                     ctime = (ent.ctime_sec, ent.ctime_nsec),
                     mtime = (ent.mtime_sec, ent.mtime_nsec),
                     manifest_mtime = (ent.manifest_mtime_sec, ent.manifest_mtime_nsec),
                     write_nonce = ent.write_nonce,
                     xattr_nonce = ent.xattr_nonce,
                     version = ent.version,
                     max_read_freshness = ent.max_read_freshness,
                     max_write_freshness = ent.max_write_freshness,
                     owner = ent.owner,
                     coordinator = ent.coordinator,
                     volume = ent.volume,
                     mode = ent.mode,
                     generation = ent.generation,
                     capacity = ent.capacity,
                     num_children = ent.num_children,
                     xattr_hash = py_xattr_hash,
                     ent_sig = py_ent_sig )

   return ve

# ------------------------------------------
cdef class Volume:
   """
      Python wrapper around a Volume
   """

   VOLUME_ENTRY_TYPE_FILE = MD_ENTRY_FILE
   VOLUME_ENTRY_TYPE_DIR = MD_ENTRY_DIR
   
   # cdef UG_state* state_inst
   
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
      
      self.state_inst = UG_init( len(args), argv, is_client )

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
         raise VolumeException("UG_create rc = %d" % rc)

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
         raise VolumeException("UG_open rc = %d" % rc)
   
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
         raise VolumeException("UG_read rc = %d" % c_read)
      
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
         raise VolumeException("UG_write rc = %d" % c_write)

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
      
      rc = UG_close( self.state_inst, c_handle )

      return rc

   # ------------------------------------------
   cpdef fsync( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp

      cdef int rc = UG_fsync( self.state_inst, c_handle )
      
      return rc

   # ------------------------------------------
   cpdef stat( self, path ):
      cdef char* c_path = path
      cdef stat statbuf
      
      cdef int rc = UG_stat( self.state_inst, path, &statbuf )

      if rc == 0:
         py_stat = os.stat_result( statbuf.st_mode,
                                   statbuf.st_ino,
                                   statbuf.st_dev,
                                   statbuf.st_nlink,
                                   statbuf.st_uid,
                                   statbuf.st_gid,
                                   statbuf.st_size,
                                   statbuf.st_atime,
                                   statbuf.st_mtime,
                                   statbuf.st_ctime )
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
         raise VolumeException("UG_opendir rc = %d" % rc)
   
      py_ret = VolumeHandle()
      py_ret.Init( ret )
      return py_ret

   # ------------------------------------------
   cpdef readdir( self, handle, count ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp

      cdef md_entry** c_dirs = NULL

      cdef int rc = UG_readdir( self.state_inst, &c_dirs, count, c_handle )
      if rc != 0:
         raise VolumeException("UG_readdir rc = %s" % rc)

      # get number of entries
      py_dirs = []
      i = 0
      while c_dirs[i] != NULL:

         ve = md_entry_to_VolumeEntry( c_dirs[i] )
         py_dirs.append( ve )
         
         i += 1

      UG_free_dir_listing( c_dirs )
      return py_dirs

   # ------------------------------------------
   cpdef closedir( self, handle ):
      cpdef uintptr_t tmp = handle.Get()
      cdef UG_handle_t* c_handle = <UG_handle_t*>tmp
      
      cdef int rc = UG_closedir( self.state_inst, c_handle )
      return rc
      
   # ------------------------------------------
   cpdef setxattr( self, path, name, value, flags ):
      cdef rc = 0
      cdef char* c_path = path 
      cdef char* c_name = name
      cdef char* c_value = value
      cdef size_t c_value_len = len(value)
      cdef int c_flags = flags
      
      rc = UG_setxattr( self.state_inst, c_path, c_name, c_value, c_value_len, c_flags )
      if rc < 0:
         raise VolumeException( "UG_setxattr rc = %s" % rc )

      return rc

   # ------------------------------------------
   cpdef getxattr( self, path, name, size ):
      cdef rc = 0
      cdef char* c_path = path 
      cdef char* c_name = name
      cdef size_t c_size = size
      cdef char* c_value = NULL

      if size > 0:
         c_value = <char*>stdlib.malloc( sizeof(char) * size )
         if c_value == NULL:
            raise MemoryError()

      rc = UG_getxattr( self.state_inst, c_path, c_name, c_value, c_size )
      if rc < 0:
         if c_value != NULL:
            stdlib.free( c_value )

         raise VolumeException( "UG_getxattr rc = %s" % rc )

      ret_value = c_value[:size]
      stdlib.free( c_value )
      return ret_value

   # ------------------------------------------
   cpdef listxattr( self, path, size ):
      cdef rc = 0
      cdef char* c_path = path 
      cdef char* c_list = NULL
      cdef size_t c_size = 0

      if size > 0:
         c_list = <char*>stdlib.malloc( sizeof(char) * size )
         if c_list == NULL:
            raise MemoryError()
      
      rc = UG_listxattr( self.state_inst, c_path, c_list, c_size )
      if rc < 0:
         if c_list != NULL:
            stdlib.free( c_list )
         
         raise VolumeException( "UG_listxattr rc = %s" % rc )
      
      ret_list = c_list[:size]
      stdlib.free( c_list )
      return ret_list
   
   # ------------------------------------------
   cpdef removexattr( self, path, name ):
      cdef rc = 0
      cdef char* c_path = path 
      cdef char* c_name = name

      rc = UG_removexattr( self.state_inst, c_path, c_name )
      if rc < 0:
         raise VolumeException( "UG_removexattr rc = %s" % rc )

      return rc
      
      
   # ------------------------------------------
   cpdef refresh( self, path ):
      cdef rc = 0 
      cdef char* c_path = path 
      rc = UG_refresh( self.state_inst, c_path )
      if rc < 0:
          raise VolumeException("UG_refresh rc = %s" % rc)

      return rc


   # ------------------------------------------
   cpdef invalidate( self, path ):
      cdef rc = 0
      cdef char* c_path = path 
      rc = UG_invalidate( self.state_inst, c_path )
      if rc < 0:
          raise VolumeException("UG_refresh rc = %s" % rc)

      return rc
  
   # ------------------------------------------
   cpdef chcoord( self, path ):
      cdef rc = 0 
      cdef char* c_path = path
      cdef uint64_t new_coordinator = 0
      rc = UG_chcoord( self.state_inst, c_path, &new_coordinator )
      if rc < 0:
          raise VolumeException("UG_chcoord rc = %s" % rc)

      return new_coordinator

   
