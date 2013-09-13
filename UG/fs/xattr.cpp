/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "xattr.h"


// getxattr
ssize_t fs_entry_getxattr( struct fs_core* core, char const* path, char const *name, char *value, size_t size, uint64_t user, uint64_t volume ) {
   
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }
   
   ssize_t ret = 0;
   
   if( strcmp( name, SYNDICATE_XATTR_MTIME ) == 0 ) {
      // enough space?
      if( size < 50 ) {
         fs_entry_unlock( fent );
         return -ERANGE;
      }
      else {
         memset( value, 0, size );
         snprintf( value, size, "%ld.%d", fent->mtime_sec, fent->mtime_nsec );
         ret = strlen( value ) + 1
      }
   }

   else if( strcmp(name, SYNDICATEFS_XATTR_COORDINATOR) == 0 ) {
      // get the URL for this file
      // TODO:
      return -ENOATTR;
   }
   else {
      fs_entry_unlock( fent );
      return -ENOATTR;
   }

   fs_entry_unlock( fent );

   return ret;
}


int fs_entry_setxattr( struct fs_core* core, char const* path, char const *name, char const *value, size_t size, int flags, uint64_t user, uint64_t volume ) {
   // not supported
   return -ENOTSUP;
}

// listxattr
ssize_t fs_entry_listxattr( struct fs_core* core, char const* path, char *list, size_t size, uint64_t user, uint64_t volume ) {
   return -ENOSYS;
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   size_t attr_len = strlen(SYNDICATE_XATTR_MTIME) + 1
                     strlen(SYNDICATE_XATTR_COORDINATOR) + 1;
                     
   if( list != NULL && attr_len >= size ) {
      // not enough space
      fs_entry_unlock( fent );
      return -ERANGE;
   }
   else if( list != NULL ) {
      snprintf( list, size, "%s\0%s", SYNDICATE_XATTR_MTIME, SYNDICATE_XATTR_COORDINATOR );
   }

   fs_entry_unlock( fent );

   return attr_len;
}

// removexattr
int fs_entry_removexattr( struct fs_core* core, char const* path, char const *name, uint64_t user, uint64_t volume ) {
   // not supported
   return -ENOTSUP;
}
