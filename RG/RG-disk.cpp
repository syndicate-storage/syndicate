/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "RG-disk.h"

#define DISK_STORAGE_DIR "/tmp/RG-disk"

char* disk_storage_path( char const* username, char const* url_path ) {
   char* tmp = md_fullpath( DISK_STORAGE_DIR, username, NULL );
   char* fp = md_fullpath( tmp, url_path, NULL );
   free( tmp );
   return fp;
}

ssize_t put_file( struct gateway_context* dat, char const* buf, size_t len, void* user_cls ) {
   struct disk_context* disk_ctx = (struct disk_context*)user_cls;

   // write the data
   size_t ret = fwrite( buf, 1, len, disk_ctx->fh );

   return ret;
}


ssize_t get_file( struct gateway_context* dat, char* buf, size_t len, void* user_cls ) {
   struct disk_context* disk_ctx = (struct disk_context*)user_cls;
   
   // give back data
   size_t ret = fread( buf, 1, len, disk_ctx->fh );

   return ret;
}


int delete_file( struct gateway_context* dat, void* user_cls ) {
   struct disk_context* disk_ctx = (struct disk_context*)user_cls;

   int rc = unlink( disk_ctx->path );

   char* fp_dir = md_dirname( disk_ctx->path, NULL );
   md_rmdirs( fp_dir );
   
   free( fp_dir );
   
   if( rc != 0 ) {
      return rc;
   }

   return 0;
}

void* connect_file( struct gateway_context* ctx ) {
   // make the file!
   char* file_path = disk_storage_path( ctx->username, ctx->url_path );

   struct disk_context* disk_ctx = CALLOC_LIST( struct disk_context, 1 );
   
   if( strcmp(ctx->method, "GET") == 0 ) {
      disk_ctx->fh = fopen( file_path, "r" );
      if( disk_ctx->fh == NULL ) {
         int errsv = -errno;
         
         free( file_path );
         free( disk_ctx );

         if( errsv == -ENOENT )
            ctx->err = 404;
         else if( errsv == -EACCES )
            ctx->err = 403;
         else
            ctx->err = 500;
         return NULL;
      }
   }

   else if( strcmp(ctx->method, "POST") == 0 ) {
      
      char* fp_dir = md_dirname( file_path, NULL );
      int rc = md_mkdirs( fp_dir );
      free( fp_dir );

      if( rc != 0 ) {
         ctx->err = 500;
         errorf(" md_mkdirs rc = %d\n", rc );
         free( disk_ctx );
         return NULL;
      }
      
      disk_ctx->fh = fopen( file_path, "w" );
      if( disk_ctx->fh == NULL ) {
         rc = -errno;
         errorf(" fopen %s errno = %d\n", file_path, rc );
         free( file_path );
         free( disk_ctx );

         if( rc == -ENOENT )
            ctx->err = 404;
         else
            ctx->err = 500;
         
         return NULL;
      }
   }

   disk_ctx->path = file_path;
   return disk_ctx;
}

void cleanup_file( void* cls ) {
   struct disk_context* disk_ctx = (struct disk_context*)cls;

   if( disk_ctx ) {
      if( disk_ctx->fh ) {
         fclose( disk_ctx->fh );
      }

      if( disk_ctx->path ) {
         free( disk_ctx->path );
      }

      free( disk_ctx );
   }
}

int main( int argc, char** argv ) {
   
   gateway_put_func( put_file );
   gateway_get_func( get_file );
   gateway_delete_func( delete_file );
   gateway_connect_func( connect_file );
   gateway_cleanup_func( cleanup_file );

   int rc = RG_main( argc, argv );

   return rc;
}
