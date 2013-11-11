/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "open.h"
#include "link.h"
#include "manifest.h"
#include "network.h"
#include "storage.h"
#include "unlink.h"
#include "url.h"
#include "collator.h"

// create a file handle from an fs_entry
struct fs_file_handle* fs_file_handle_create( struct fs_core* core, struct fs_entry* ent, char const* opened_path, uint64_t parent_id, char const* parent_name ) {
   struct fs_file_handle* fh = CALLOC_LIST( struct fs_file_handle, 1 );
   fh->flags = 0;
   fh->open_count = 0;
   fh->fent = ent;
   fh->dirty = false;
   fh->volume = ent->volume;
   fh->file_id = ent->file_id;
   fh->path = strdup( opened_path );
   fh->parent_name = strdup( parent_name );
   fh->parent_id = parent_id;
   fh->transfer_timeout_ms = (core->conf->transfer_timeout) * 1000L;

   int gateway_type = ms_client_get_gateway_type( core->ms, ent->coordinator );
   if( gateway_type == SYNDICATE_AG ) {
      fh->is_AG = true;
      fh->AG_blocksize = ms_client_get_AG_blocksize( core->ms, ent->coordinator );
   }
   
   pthread_rwlock_init( &fh->lock, NULL );
   
   fh->rctxs = new vector<struct replica_context*>();
   
   return fh;
}


// open a file handle
// NOTE: make sure everything's locked first!
int fs_file_handle_open( struct fs_file_handle* fh, int flags, mode_t mode ) {
   // is this a local file?
   fh->flags = flags;
   fh->open_count++;
   return 0;
}

// create an entry (equivalent to open with O_CREAT|O_WRONLY|O_TRUNC
struct fs_file_handle* fs_entry_create( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, mode_t mode, int* err ) {
   dbprintf( "create %s\n", path );
   return fs_entry_open( core, path, user, vol, O_CREAT|O_WRONLY|O_TRUNC, mode, err );
}


// make a node (regular files only at this time)
int fs_entry_mknod( struct fs_core* core, char const* path, mode_t mode, dev_t dev, uint64_t user, uint64_t vol ) {
   // only regular files at this time...
   if( ! ( S_ISREG( mode ) || S_ISFIFO( mode ) ) ) {
      return -ENOTSUP;
   }

   // revalidate this path
   int rc = fs_entry_revalidate_path( core, vol, path );
   if( rc != 0 && rc != -ENOENT ) {
      // consistency cannot be guaranteed
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      return -EREMOTEIO;
   }
   
   int err = 0;

   // get the parent directory and lock it
   char* path_dirname = md_dirname( path, NULL );
   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, vol, true, &err );
   free( path_dirname );

   if( !IS_DIR_READABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // not searchable
      fs_entry_unlock( parent );
      return -EACCES;
   }

   if( !IS_WRITEABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // not writeable
      fs_entry_unlock( parent );
      return -EACCES;
   }

   uint64_t parent_id = parent->file_id;
   char* parent_name = strdup( parent->name );

   char* path_basename = md_basename( path, NULL );

   // make sure it doesn't exist already
   if( fs_entry_set_find_name( parent->children, path_basename ) != NULL ) {
      free( path_basename );
      fs_entry_unlock( parent );
      free( parent_name );
      return -EEXIST;
   }

   struct fs_entry* child = (struct fs_entry*)calloc( sizeof(struct fs_entry), 1 );

   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   int mmode = 0;
   if (S_ISFIFO(mode)) {
       mmode = ( mode & 0777 ) | S_IFIFO;
       err = fs_entry_init_fifo( core, child, path_basename, fs_entry_next_file_version(), user, core->gateway, vol, mmode, 0, ts.tv_sec, ts.tv_nsec, true );
   }
   if (S_ISREG(mode)) {
       mmode = ( mode & 0777 );
       err = fs_entry_init_file( core, child, path_basename, fs_entry_next_file_version(), user, core->gateway, vol, mmode, 0, ts.tv_sec, ts.tv_nsec );
   }

   if( err == 0 ) {
      // attach the file
      fs_entry_wlock( child );
      fs_entry_attach_lowlevel( core, parent, child );

      struct md_entry data;
      fs_entry_to_md_entry( core, &data, child, parent_id, parent_name );
      
      err = ms_client_create( core->ms, &child->file_id, &data );

      if( err != 0 ) {
         errorf( "ms_client_create(%s) rc = %d\n", path, err );
         err = -EREMOTEIO;

         child->open_count = 0;
         fs_entry_unlock( child );
         fs_entry_detach_lowlevel( core, parent, child, false );
      }
      else {
         rc = fs_entry_create_local_file( core, child->file_id, child->version, mode );
         if( rc != 0 ) {
            // revert
            errorf("fs_entry_create_local_file(%s /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ") rc = %d\n", path, child->volume, child->coordinator, child->file_id, rc );
            rc = ms_client_delete( core->ms, &data );
         }

         child->open_count = 0;
         fs_entry_unlock( child );
         fs_entry_detach_lowlevel( core, parent, child, false );
      }
      
      md_entry_free( &data );
   }
   
   fs_entry_unlock( parent );

   free( parent_name );
   free( path_basename );

   return err;
}


// mark an fs_entry as having been opened, and/or create a file
struct fs_file_handle* fs_entry_open( struct fs_core* core, char const* _path, uint64_t user, uint64_t vol, int flags, mode_t mode, int* err ) {

   char* path = strdup(_path);
   md_sanitize_path( path );
   
   // revalidate this path
   int rc = fs_entry_revalidate_path( core, vol, path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      
      if( rc == -ENOENT ) {
         if( !(flags & O_CREAT) ) {
            free( path );
            *err = rc;
            return NULL;
         }
         else {
            // otherwise we're good
            rc = 0;
         }
      }
      else {
         // consistency cannot be guaranteed
         errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
         free( path );
         *err = -EREMOTEIO;
         return NULL;
      }
   }
   else {
      if( flags & O_CREAT ) {
         errorf("%s already exists\n", path );
         free( path );
         *err = -EEXIST;
         return NULL;
      }
   }
         
   
   // resolve the parent of this child (and write-lock it)
   char* path_dirname = md_dirname( path, NULL );
   char* path_basename = md_basename( path, NULL );
   struct fs_file_handle* ret = NULL;

   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, vol, true, err );

   if( parent == NULL ) {

      free( path_basename );
      free( path_dirname );
      free( path );

      // err is set appropriately
      return NULL;
   }

   free( path_dirname );

   if( parent->ftype != FTYPE_DIR ) {
      // parent is not a directory
      fs_entry_unlock( parent );
      free( path_basename );
      free( path );
      *err = -ENOTDIR;
      return NULL;
   }

   *err = 0;

   // can parent be searched?
   if( !IS_DIR_READABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // nope
      fs_entry_unlock( parent );
      free( path_basename );
      free( path );
      *err = -EACCES;
      return NULL;
   }

   uint64_t parent_id = parent->file_id;
   char* parent_name = strdup( parent->name );

   // resolve the child
   struct fs_entry* child = fs_entry_set_find_name( parent->children, path_basename );
   bool created = false;

   if( flags & O_CREAT ) {
      if( child != NULL ) {
         // can't create
         fs_entry_unlock( parent );
         free( path_basename );
         free( path );
         free( parent_name );
         *err = -ENOENT;
         return NULL;
      }
      else if( !IS_WRITEABLE(parent->mode, parent->owner, parent->volume, user, vol) ) {
         // can't create
         fs_entry_unlock( parent );
         free( path_basename );
         free( path );
         free( parent_name );
         *err = -EACCES;
         return NULL;
      }
      else {
         struct timespec ts;
         clock_gettime( CLOCK_REALTIME, &ts );

         // can create--initialize the child
         child = CALLOC_LIST( struct fs_entry, 1 );

         int rc = fs_entry_init_file( core, child, path_basename, fs_entry_next_file_version(), user, core->gateway, vol, mode, 0, ts.tv_sec, ts.tv_nsec );

         if( rc != 0 ) {
            errorf("fs_entry_init_file(%s) rc = %d\n", path, rc );
            *err = rc;

            fs_entry_unlock( parent );
            free( path_basename );
            free( path );
            free( parent_name );
            fs_entry_destroy( child, false );
            free( child );
            
            return NULL;
         }
         else {
            // insert it into the filesystem
            
            fs_entry_wlock( child );
            child->open_count++;
            
            fs_entry_attach_lowlevel( core, parent, child );
            created = true;
            
            fs_entry_unlock( child );
            
         }
      }
   }
   else if( child == NULL ) {
      // not found
      fs_entry_unlock( parent );
      free( path_basename );
      free( path );
      free( parent_name );
      *err = -ENOENT;
      return NULL;
   }

   // now child exists.
   
   // safe to lock it so we can release the parent
   fs_entry_wlock( child );
   fs_entry_unlock( parent );

   if( child->link_count <= 0 ) {
      // only possible if we didn't just create
      // someone unlinked this child at the last minute
      // can't open
      fs_entry_unlock( child );
      free( path_basename );
      free( path );
      free( parent_name );
      *err = -ENOENT;
      return NULL;
   }

   if( child->ftype != FTYPE_FILE ) {
      // only possible if we didn't just create
      // not a file
      // can't open
      fs_entry_unlock( child );
      free( path_basename );
      free( path );
      free( parent_name );
      *err = -EISDIR;
      return NULL;
   }

   if( !created ) {

      // access control
      // check read/write status of flags, and bail on error
      if( (!(flags & O_RDWR) && !(flags & O_WRONLY))  && !IS_READABLE(child->mode, child->owner, child->volume, user, vol) ) {
         *err = -EACCES;  // not readable
      }
      else if( (flags & O_WRONLY) && !IS_WRITEABLE(child->mode, child->owner, child->volume, user, vol) ) {
         *err = -EACCES;  // not writable
      }
      else if( (flags & O_RDWR) && (!IS_READABLE(child->mode, child->owner, child->volume, user, vol) || !IS_WRITEABLE(child->mode, child->owner, child->volume, user, vol)) ) {
         *err = -EACCES;  // not readable or not writable
      }
      if( *err != 0 ) {
         // can't do I/O
         fs_entry_unlock( child );
         free( path_basename );
         free( path );
         free( parent_name );
         return NULL;
      }

      // refresh the manifest
      int rc = fs_entry_revalidate_manifest( core, path, child );
      if( rc != 0 ) {
         errorf("fs_entry_revalidate_manifest(%s) rc = %d\n", path, rc );
         fs_entry_unlock( child );
         free( path_basename );
         free( path );
         free( parent_name );
         *err = -EREMOTEIO;
         return NULL;
      }
   }

   // do we need to truncate?
   if( flags & O_TRUNC && !created ) {
      child->size = 0;
      
      if( FS_ENTRY_LOCAL( core, child ) ) {
         fs_entry_clear_local_file( core, child->file_id, child->version );
      }
      else {
         // send a truncate request to the owner
         Serialization::WriteMsg *truncate_msg = new Serialization::WriteMsg();
         fs_entry_init_write_message( truncate_msg, core, Serialization::WriteMsg::TRUNCATE );

         Serialization::TruncateRequest* truncate_req = truncate_msg->mutable_truncate();
         truncate_req->set_fs_path( path );
         truncate_req->set_file_version( child->version );
         truncate_req->set_size( 0 );

         Serialization::BlockList* blocks = truncate_msg->mutable_blocks();
         blocks->set_start_id( 0 );
         blocks->set_end_id( 1 );
         blocks->add_version( child->manifest->get_block_version( 0 ) );

         Serialization::WriteMsg *withdraw_ack = new Serialization::WriteMsg();

         *err = fs_entry_post_write( withdraw_ack, core, child->coordinator, truncate_msg );
         if( *err < 0 ) {
            errorf( "fs_entry_post_write(%s) rc = %d\n", path, *err );

            if( !created )
               fs_entry_unlock( child );

            free( path_basename );
            free( path );
            free( parent_name );
            
            *err = -EREMOTEIO;
            return NULL;
         }

         else if( withdraw_ack->type() != Serialization::WriteMsg::ACCEPTED ) {
            if( withdraw_ack->type() == Serialization::WriteMsg::ERROR ) {
               errorf( "remote truncate failed, error = %d (%s)\n", withdraw_ack->errorcode(), withdraw_ack->errortxt().c_str() );
               *err = withdraw_ack->errorcode();
            }
            else {
               errorf( "remote truncate invalid message %d\n", withdraw_ack->type() );
               *err = -EREMOTEIO;
            }
         }

         if( *err < 0 ) {
            
            if( !created )
               fs_entry_unlock( child );

            free( path_basename );
            free( path );
            free( parent_name );
            
            *err = -EREMOTEIO;
            return NULL;
         }
      }
   }


   if( !created )
      child->open_count++;

   if( created && *err == 0 ) {

      // create this file in the MS
      struct md_entry data;
      fs_entry_to_md_entry( core, &data, child, parent_id, parent_name );
      
      // create synchronously
      *err = ms_client_create( core->ms, &child->file_id, &data );

      if( *err != 0 ) {
         errorf("ms_client_create(%s) rc = %d\n", _path, *err );
         *err = -EREMOTEIO;

         // revert
         child->open_count = 0;
         fs_entry_unlock( child );

         // NOTE: parent will still exist--we can't remove a non-empty directory
         fs_entry_wlock( parent );
         fs_entry_detach_lowlevel( core, parent, child, true );
         fs_entry_unlock( parent );

         child = NULL;
      }
      else {
         // success on MS!  create locally
         rc = fs_entry_create_local_file( core, child->file_id, child->version, 0600 );
         if( rc != 0 ) {
            errorf("fs_entry_create_local_file(%s /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ") rc = %d\n", path, child->volume, child->coordinator, child->file_id, rc );
            *err = -EIO;

            // revert
            child->open_count = 0;

            // NOTE: parent will still exist--we can't remove a non-empty directory
            fs_entry_wlock( parent );
            fs_entry_detach_lowlevel( core, parent, child, true );
            fs_entry_unlock( parent );
         }
      }
      
      md_entry_free( &data );
   }

   if( *err == 0 ) {
      // still here--we can open the file now!
      child->atime = currentTimeSeconds();
      ret = fs_file_handle_create( core, child, path, parent_id, parent_name );
      fs_file_handle_open( ret, flags, mode );
   }
   
   if( child ) 
      fs_entry_unlock( child );
   
   free( path_basename );
   free( path );
   free( parent_name );
   
   return ret;
}

