
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

from libc.stdint cimport int32_t, uint32_t, int64_t, uint64_t, uintptr_t
   
# ------------------------------------------
cdef extern from "sys/types.h":
   ctypedef int bool
   ctypedef int mode_t
   ctypedef unsigned int size_t
   ctypedef int off_t
   ctypedef unsigned int dev_t
   ctypedef unsigned long long ino_t
   ctypedef unsigned int nlink_t
   ctypedef unsigned int uid_t
   ctypedef unsigned int gid_t
   ctypedef unsigned int blksize_t
   ctypedef unsigned long long blkcnt_t
   ctypedef long int time_t


# ------------------------------------------
cdef extern from "string.h":
   void* memset( void*, int, size_t )


# ------------------------------------------
cdef extern from "sys/stat.h":
   cdef struct stat:
      dev_t     st_dev     # /* ID of device containing file */
      ino_t     st_ino     # /* inode number */
      mode_t    st_mode    # /* protection */
      nlink_t   st_nlink   # /* number of hard links */
      uid_t     st_uid     # /* user ID of owner */
      gid_t     st_gid     # /* group ID of owner */
      dev_t     st_rdev    # /* device ID (if special file) */
      off_t     st_size    # /* total size, in bytes */
      blksize_t st_blksize # /* blocksize for file system I/O */
      blkcnt_t  st_blocks  # /* number of 512B blocks allocated */
      time_t    st_atime   # /* time of last access */
      time_t    st_mtime   # /* time of last modification */
      time_t    st_ctime   # /* time of last status change */


# ------------------------------------------
cdef extern from "libsyndicate/libsyndicate.h":

   cdef struct md_entry:
      int type            # // file or directory?
      char* name          # // name of this entry
      uint64_t file_id    # // id of this file 
      int64_t ctime_sec   # // creation time (seconds)
      int32_t ctime_nsec  # // creation time (nanoseconds)
      int64_t mtime_sec   # // last-modified time (seconds)
      int32_t mtime_nsec  # // last-modified time (nanoseconds)
      int64_t manifest_mtime_sec    # // manifest last-mod time (actual last-write time, regardless of utime) (seconds)
      int32_t manifest_mtime_nsec   # // manifest last-mod time (actual last-write time, regardless of utime) (nanoseconds)
      int64_t write_nonce # // last-write nonce 
      int64_t xattr_nonce # // xattr write nonce
      int64_t version     # // file version
      int32_t max_read_freshness      # // how long is this entry fresh until it needs revalidation?
      int32_t max_write_freshness     # // how long can we delay publishing this entry?
      uint64_t owner         # // ID of the User that owns this File
      uint64_t coordinator  # // ID of the Gateway that coordinatates writes on this File
      uint64_t volume        # // ID of the Volume
      mode_t mode         # // file permission bits
      off_t size          # // size of the file
      int32_t error       # // error information with this md_entry
      int64_t generation  # // n, as in, the nth item to ever been created in the parent directory 
      int64_t num_children  # // number of children this entry has, if it's a directory 
      int64_t capacity     # // maximum index number a child can have 
      uint64_t parent_id  # // id of this file's parent directory
      char* parent_name   # // name of this file's parent directory
      unsigned char* xattr_hash   # // hash over sorted (xattr name, xattr value) pairs
      unsigned char* ent_sig      # // signature over this entry from the coordinator
      size_t ent_sig_len

   cdef int MD_ENTRY_FILE 
   cdef int MD_ENTRY_DIR

# ------------------------------------------
cdef extern from "libsyndicate/util.h":

   cdef struct mlock_buf:
      void* ptr
      size_t len

# ------------------------------------------   
cdef extern from "libsyndicate-ug/core.h":
   
   cdef struct UG_state:
      pass

   UG_state* UG_init( int argc, char** argv, bool client )
   int UG_main( UG_state* state )
   int UG_start( UG_state* state )
   int UG_shutdown( UG_state* state )
   

# ------------------------------------------   
cdef extern from "libsyndicate-ug/inode.h":

   cdef struct UG_inode:
      pass
      
# ------------------------------------------
cdef extern from "libsyndicate-ug/client.h":
   cdef struct _UG_handle:
      pass

   ctypedef _UG_handle UG_handle_t
   
   int UG_stat( UG_state* state, const char* path, stat *statbuf )
   int UG_mkdir( UG_state* state, const char* path, mode_t mode )
   int UG_unlink( UG_state* state, const char* path )
   int UG_rmdir( UG_state* state, const char* path )
   int UG_rename( UG_state* state, const char* path, const char* newpath )
   int UG_chmod( UG_state* state, const char* path, mode_t mode )
   int UG_chown( UG_state* state, const char* path, uint64_t new_owner )
   int UG_chcoord( UG_state* state, const char* path, uint64_t* new_coordinator_response )
   int UG_truncate( UG_state* state, const char* path, off_t newsize )
   int UG_access( UG_state* state, const char* path, int mask )
   int UG_invalidate( UG_state* state, const char* path )
   int UG_refresh( UG_state* state, const char* path )

   UG_handle_t* UG_create( UG_state* state, const char* path, mode_t mode, int* rc )
   UG_handle_t* UG_open( UG_state* state, const char* path, int flags, int* rc )
   int UG_read( UG_state* state, char *buf, size_t size, UG_handle_t* fi )
   int UG_write( UG_state* state, const char* buf, size_t size, UG_handle_t *fi )
   off_t UG_seek( UG_handle_t* fi, off_t pos, int whence )
   int UG_close( UG_state* state, UG_handle_t *fi )
   int UG_fsync( UG_state* state, UG_handle_t *fi )
   int UG_ftruncate( UG_state* state, off_t offset, UG_handle_t *fi )
   int UG_fstat( UG_state* state, stat *statbuf, UG_handle_t *fi )

   UG_handle_t* UG_opendir( UG_state* state, const char* path, int* rc )
   int UG_readdir( UG_state* state, md_entry*** listing, size_t num_children, UG_handle_t *fi )
   int UG_rewinddir( UG_handle_t* fi )
   off_t UG_telldir( UG_handle_t* fi )
   int UG_seekdir( UG_handle_t* fi, off_t loc )
   int UG_closedir( UG_state* state, UG_handle_t *fi )
   void UG_free_dir_listing( md_entry** listing )

   int UG_setxattr( UG_state* state, const char* path, const char* name, const char* value, size_t size, int flags )
   int UG_getxattr( UG_state* state, const char* path, const char* name, char *value, size_t size )
   int UG_listxattr( UG_state* state, const char* path, char *list, size_t size )
   int UG_removexattr( UG_state* state, const char* path, const char* name )

# ------------------------------------------
cdef class VolumeHandle:
   cpdef uintptr_t handle_ptr

   cdef Init( self, UG_handle_t* handle )
   cpdef Get( self )

# ------------------------------------------
cdef class Volume:

   cdef UG_state* state_inst

   cpdef create( self, path, mode )
   cpdef open( self, path, flags )
   cpdef read( self, handle, size )
   cpdef write( self, handle, buf )
   cpdef seek( self, handle, offset, whence )
   cpdef close( self, handle )
   cpdef fsync( self, handle )
   cpdef stat( self, path )
   cpdef mkdir( self, path, mode )
   cpdef unlink( self, path )
   cpdef rmdir( self, path )
   cpdef opendir( self, path )
   cpdef readdir( self, handle, count )
   cpdef closedir( self, handle )
   cpdef setxattr( self, path, name, value, flags )
   cpdef getxattr( self, path, name, size )
   cpdef listxattr( self, path, size )
   cpdef removexattr( self, path, name )
   cpdef refresh( self, path )
   cpdef invalidate( self, path )
   cpdef chcoord( self, path )
