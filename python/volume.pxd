
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
cdef extern from "libsyndicate.h":

   cdef struct md_entry:
      int type            # // file or directory?
      char* name          # // name of this entry
      uint64_t file_id    # // id of this file 
      int64_t ctime_sec   # // creation time (seconds)
      int32_t ctime_nsec  # // creation time (nanoseconds)
      int64_t mtime_sec   # // last-modified time (seconds)
      int32_t mtime_nsec  # // last-modified time (nanoseconds)
      int64_t write_nonce # // last-write nonce 
      int64_t version     # // file version
      int32_t max_read_freshness      # // how long is this entry fresh until it needs revalidation?
      int32_t max_write_freshness     # // how long can we delay publishing this entry?
      uint64_t owner         # // ID of the User that owns this File
      uint64_t coordinator  # // ID of the Gateway that coordinatates writes on this File
      uint64_t volume        # // ID of the Volume
      mode_t mode         # // file permission bits
      off_t size          # // size of the file
      int32_t error       # // error information with this md_entry
      uint64_t parent_id  # // id of this file's parent directory
      char* parent_name   # // name of this file's parent directory


# ------------------------------------------
cdef extern from "fs/fs_entry.h":
   cdef struct fs_dir_entry:
      int type
      md_entry ent
   
   cdef int FTYPE_DIR
   cdef int FTYPE_FILE
   cdef int FTYPE_NONE

   uint64_t fs_dir_entry_type( fs_dir_entry* dirent )
   char* fs_dir_entry_name( fs_dir_entry* dirent )
   uint64_t fs_dir_entry_file_id( fs_dir_entry* dirent )
   int64_t fs_dir_entry_mtime_sec( fs_dir_entry* dirent )
   int32_t fs_dir_entry_mtime_nsec( fs_dir_entry* dirent )
   int64_t fs_dir_entry_ctime_sec( fs_dir_entry* dirent )
   int32_t fs_dir_entry_ctime_nsec( fs_dir_entry* dirent )
   int64_t fs_dir_entry_write_nonce( fs_dir_entry* dirent )
   int64_t fs_dir_entry_version( fs_dir_entry* dirent )
   int32_t fs_dir_entry_max_read_freshness( fs_dir_entry* dirent )
   int32_t fs_dir_entry_max_write_freshness( fs_dir_entry* dirent )
   uint64_t fs_dir_entry_owner( fs_dir_entry* dirent )
   uint64_t fs_dir_entry_coordinator( fs_dir_entry* dirent )
   uint64_t fs_dir_entry_volume( fs_dir_entry* dirent )
   int32_t fs_dir_entry_mode( fs_dir_entry* dirent )
   uint64_t fs_dir_entry_size( fs_dir_entry* dirent )

# ------------------------------------------
cdef extern from "state.h":
   cdef struct syndicate_state:
      pass


# ------------------------------------------   
cdef extern from "client.h":
   cdef struct syndicate_handle_t_TAG:
      pass

   ctypedef syndicate_handle_t_TAG syndicate_handle_t
   ctypedef fs_dir_entry** syndicate_dir_listing_t

   int syndicate_client_init( syndicate_state* state,
                              char * config_file,
                              char * ms_url,
                              char * volume_name,
                              char * gateway_name,
                              int gateway_port,
                              char * md_username,
                              char * md_password,
                              char * volume_pubkey_file,
                              char * my_key_file,
                              char * storage_root
                             )

   int syndicate_client_shutdown( syndicate_state* state, int wait_replicas )

   syndicate_handle_t* syndicate_create( syndicate_state* state, char *path, mode_t mode, int* rc )
   syndicate_handle_t* syndicate_open( syndicate_state* state, char *path, int flags, int* rc)
   
   int syndicate_read( syndicate_state* state, char *buf, size_t size, syndicate_handle_t* fi)
   int syndicate_write( syndicate_state* state, char *buf, size_t size, syndicate_handle_t *fi)
   int syndicate_seek(syndicate_handle_t* fi, off_t pos, int whence)
   int syndicate_flush( syndicate_state* state, syndicate_handle_t *fi)
   int syndicate_close( syndicate_state* state, syndicate_handle_t *fi)
   int syndicate_fsync( syndicate_state* state, int datasync, syndicate_handle_t *fi)
   
   int syndicate_getattr( syndicate_state* state, char *path, stat *statbuf)
   int syndicate_mkdir( syndicate_state* state, char *path, mode_t mode)
   int syndicate_unlink( syndicate_state* state, char *path)
   int syndicate_rmdir( syndicate_state* state, char *path)

   syndicate_handle_t* syndicate_opendir( syndicate_state* state, char *path, int* rc)
   int syndicate_readdir( syndicate_state* state, syndicate_dir_listing_t* listing, syndicate_handle_t *fi)
   int syndicate_closedir( syndicate_state* state, syndicate_handle_t *fi)

   void syndicate_free_dir_listing( syndicate_dir_listing_t listing )
