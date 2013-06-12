/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "xattr.h"


// getxattr
ssize_t fs_entry_getxattr( struct fs_core* core, char const* path, char const *name, char *value, size_t size, uid_t user, gid_t volume ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   if( strcmp(name, SYNDICATEFS_XATTR_URL) != 0 ) {
      // unrecognized xattr
      fs_entry_unlock( fent );
      return -ENOATTR;
   }

   size_t url_len = strlen(fent->url);

   if( value != NULL && url_len >= size ) {
      // not enough space
      fs_entry_unlock( fent );
      return -ERANGE;
   }

   else if( value != NULL )
      strcpy( value, fent->url );

   fs_entry_unlock( fent );

   return url_len + 1;
}

// setxattr--change the URL of a file
int fs_entry_setxattr( struct fs_core* core, char const* path, char const *name, char const *value, size_t size, int flags, uid_t user, gid_t volume ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, true, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   // only support files
   if( fent->ftype != FTYPE_FILE ) {
      fs_entry_unlock( fent );
      return -EISDIR;
   }

   // only support XATTR_REPLACE or 0
   if( flags == XATTR_CREATE ) {
      fs_entry_unlock( fent );
      return -EEXIST;
   }

   // only support syndicatefs_url
   if( size < strlen(SYNDICATEFS_XATTR_URL) || strcmp(name, SYNDICATEFS_XATTR_URL) != 0 ) {
      fs_entry_unlock( fent );
      return -ENOTSUP;
   }

   // replace the URL
   if( fent->url ) {
      free( fent->url );
   }

   fent->url = (char*)calloc( size + 1, 1 );
   strncpy( fent->url, value, size );

   fs_entry_unlock( fent );
   return 0;
}

// listxattr
ssize_t fs_entry_listxattr( struct fs_core* core, char const* path, char *list, size_t size, uid_t user, gid_t volume ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   size_t attr_len = strlen(SYNDICATEFS_XATTR_URL);
   if( list != NULL && attr_len >= size ) {
      // not enough space
      fs_entry_unlock( fent );
      return -ERANGE;
   }
   else if( list != NULL )
      strcpy( list, SYNDICATEFS_XATTR_URL );

   fs_entry_unlock( fent );

   return attr_len + 1;
}

// removexattr
int fs_entry_removexattr( struct fs_core* core, char const* path, char const *name, uid_t user, gid_t volume ) {
   // not supported
   return -ENOTSUP;
}
