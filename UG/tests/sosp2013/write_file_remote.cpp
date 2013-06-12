#include "libsyndicate.h"
#include "syndicate.h"

#include <getopt.h>

#define WRITE_FILE "/testfile"


int serve = 1;

void sighandle( int value ) {
   serve = 0;
}

int main(int argc, char** argv) {
   md_debug(1);
   md_error(1);
   dbprintf("%s\n", "starting up debugging");
   errorf("%s\n", "starting up errors");

   int c;
   char* config_file = (char*)CLIENT_DEFAULT_CONFIG;
   int portnum = 0;

   struct md_HTTP syndicate_http;
   char* username = NULL;
   char* password = NULL;
   char* volume_name = NULL;
   char* volume_secret = NULL;
   char* ms_url = NULL;
   off_t offset = 0;
   size_t size = 0;

   static struct option syndicate_options[] = {
      {"config-file",     required_argument,   0, 'c'},
      {"volume-name",     required_argument,   0, 'v'},
      {"volume-secret",   required_argument,   0, 's'},
      {"username",        required_argument,   0, 'u'},
      {"password",        required_argument,   0, 'p'},
      {"port",            required_argument,   0, 'P'},
      {"MS",              required_argument,   0, 'm'},
      {"offset",          required_argument,   0, 'o'},
      {"size",            required_argument,   0, 'z'},
      {0, 0, 0, 0}
   };

   int opt_index = 0;

   while((c = getopt_long(argc, argv, "c:v:s:u:p:P:fm:o:z:", syndicate_options, &opt_index)) != -1) {
      switch( c ) {
         case 'o': {
            offset = strtol(optarg, NULL, 10);
            break;
         }
         case 'z': {
            size = strtol(optarg, NULL, 10);
            break;
         }
         case 'v': {
            volume_name = optarg;
            break;
         }
         case 'c': {
            config_file = optarg;
            break;
         }
         case 's': {
            volume_secret = optarg;
            break;
         }
         case 'u': {
            username = optarg;
            break;
         }
         case 'p': {
            password = optarg;
            break;
         }
         case 'P': {
            portnum = strtol(optarg, NULL, 10);
            break;
         }
         case 'm': {
            ms_url = optarg;
            break;
         }
         default: {
            break;
         }
      }
   }

   int rc = syndicate_init( config_file, &syndicate_http, portnum, ms_url, volume_name, volume_secret, username, password );
   if( rc != 0 )
      exit(1);

   struct md_syndicate_conf* conf = syndicate_get_conf();
   if( portnum == 0 )
      portnum = conf->httpd_portnum;

   struct syndicate_state* state = syndicate_get_state();

   // synchronous everything
   conf->default_write_freshness = 0;

   char file[PATH_MAX];
   memset(file, 0, PATH_MAX);
   strcpy( file, WRITE_FILE );

   signal( SIGINT, sighandle );
   signal( SIGQUIT, sighandle );
   signal( SIGTERM, sighandle );



   // write to the file
   ssize_t file_size = size;
   char* buf = CALLOC_LIST( char, file_size );
   char fill = rand() % 26 + 'A';
   memset( buf, fill, file_size );

   char* wbuf = CALLOC_LIST( char, conf->blocking_factor );
   memset( wbuf, fill, conf->blocking_factor );


   struct fs_file_handle* fh = NULL;
   ssize_t nw = 0;

   struct timespec ts, ts2;

   char const* cleanup_fmt = "/bin/rm -rf %s %s";
   char* cleanup_buf = CALLOC_LIST( char, strlen(cleanup_fmt) + strlen(conf->staging_root) + strlen(conf->data_root) + 1 );
   sprintf(cleanup_buf, cleanup_fmt, conf->data_root, conf->staging_root);

   system(cleanup_buf);

   DATA_BLOCK("open");

   BEGIN_TIMING_DATA( ts );
   
   // create the file
   fh = fs_entry_open( state->core, file, NULL, conf->owner, conf->volume, O_SYNC | O_RDWR, 0666, &rc );
   if( rc != 0 ) {
      errorf("fs_entry_open(%s) rc = %d\n", file, rc );
      exit(1);
   }

   END_TIMING_DATA( ts, ts2, "open + MS revalidate + manifest refresh" );

   DATA_BLOCK("write");

   // mark the file as stale
   fs_entry_wlock( fh->fent );
   fs_entry_mark_read_stale( fh->fent );
   fs_entry_unlock( fh->fent );


   dbprintf("write %zd bytes at %lld\n", file_size, (int64_t)offset );
   
   BEGIN_TIMING_DATA( ts );
   
   // write the file
   nw = fs_entry_write( state->core, fh, buf, file_size, offset );
   if( nw != file_size ) {
      errorf("fs_entry_write(%s) rc = %ld\n", file, nw );
      exit(1);
   }

   END_TIMING_DATA( ts, ts2, "write + MS revalidate" );

   DATA_BLOCK("close");

   BEGIN_TIMING_DATA( ts );
   
   // close
   rc = fs_entry_close( state->core, fh );
   if( rc != 0 ) {
      errorf("fs_entry_close(%s) rc = %d\n", file, rc );
      exit(1);
   }

   END_TIMING_DATA( ts, ts2, "close" );

   free( fh );

   // serve the file!
   DATA_BLOCK("serve");
   printf("OPT begin_serve\n");

   while( serve ) {
      sleep( 1 );
   }

   printf("OPT end_serve\n");
   DATA_BLOCK("");


   syndicate_destroy();

   system(cleanup_buf);
   free( buf );
   free( wbuf );
   return 0;
}
