#include "libsyndicate.h"
#include "ms-client.h"
#include <getopt.h>

int main( int argc, char** argv ) {
   gid_t volume_id;
   uid_t my_owner_id;
   uid_t volume_owner_id;
   char** replica_urls;
   int rc;

   atexit( google::protobuf::ShutdownProtobufLibrary );
   
   char* volume_name = NULL;
   char* volume_secret = NULL;
   char* ug_username = NULL;
   char* ug_password = NULL;
   char* command = NULL;
   bool queue = false;
   int delay = 0;             // number of millis between deadlines
   char const* config = "/etc/syndicate/syndicate-client.conf";
   
   static struct option long_options[] = {
      {"volume-name",     required_argument,   0, 'v'},
      {"command",         required_argument,   0, 'c'},
      {"queue",           no_argument,         0, 'q'},
      {"delay",           required_argument,   0, 'd'},
      {"volume-secret",   required_argument,   0, 's'},
      {"username",        required_argument,   0, 'u'},
      {"password",        required_argument,   0, 'p'},
      {0, 0, 0, 0}
   };

   int opt_index = 0;
   int c = 0;
   while((c = getopt_long(argc, argv, "v:c:qd:s:u:p:", long_options, &opt_index)) != -1) {
      switch( c ) {
         case 'v': {
            volume_name = optarg;
            break;
         }
         case 'c': {
            command = optarg;
            break;
         }
         case 'q': {
            queue = true;
            break;
         }
         case 'd': {
            delay = strtol(optarg, NULL, 10);
            break;
         }
         case 's': {
            volume_secret = optarg;
            break;
         }
         case 'u': {
            ug_username = optarg;
            break;
         }
         case 'p': {
            ug_password = optarg;
            break;
         }
            
         default: {
            errorf("Ignoring unrecognized option %c\n", c);
            break;
         }
      }
   }

   
   if( volume_name == NULL || command == NULL ) {
      errorf("%s", "No volume name or command given\n");
      exit(1);
   }

   struct md_syndicate_conf conf;
   rc = md_read_conf( config, &conf );
   if( rc != 0 ) {
      errorf("md_read_conf rc = %d\n", rc );
      exit(1);
   }

   rc = md_init( &conf, NULL );
   if( rc != 0 ) {
      errorf("md_init rc = %d\n", rc );
      exit(1);
   }

   struct ms_client* client = CALLOC_LIST( struct ms_client, 1 );

   if( ug_username != NULL ) {
      if( conf.metadata_username )
         free( conf.metadata_username );

      conf.metadata_username = ug_username;
   }

   if( ug_password != NULL ) {
      if( conf.metadata_password )
         free( conf.metadata_password );

      conf.metadata_password = ug_password;
   }
   
   rc = ms_client_init( client, &conf, volume_name, conf.metadata_username, conf.metadata_password );
   if( rc != 0 ) {
      errorf("ms_client_init rc = %d\n", rc );
      exit(1);
   }

   struct md_user_entry** users = NULL;
   uint64_t version = 0;
   rc = ms_client_get_volume_metadata( client, volume_name, volume_secret, &version, &conf.owner, &conf.volume_owner, &conf.volume, &conf.replica_urls, &conf.blocking_factor, &users );
   if( rc != 0 ) {
      errorf("ms_client_get_volume_metadata rc = %d\n", rc );
      exit(1);
   }

   printf("Volume:    %s\n"
          "version:   %d\n"
          "my UID:    %d\n"
          "owner UID: %d\n"
          "volume ID: %d\n"
          "blocksize: %ld\n",
          volume_name, version, conf.owner, conf.volume_owner, conf.volume, conf.blocking_factor );

   for( int i = 0; conf.replica_urls[i] != NULL; i++ ) {
      printf("replica:   %s\n", conf.replica_urls[i]);
   }

   for( int i = 0; users[i] != NULL; i++ ) {
      printf("UG:  UID:      %d\n"
             "     username: %s\n"
             "     passhash: %s\n",
             users[i]->uid, users[i]->username, users[i]->password_hash );
   }

   
   if( strcmp( command, "resolve" ) == 0 ) {
      int path_index = optind;
      char* path = argv[path_index];

      vector<struct md_entry> path_metadata;
      vector<struct md_entry> child_metadata;

      int ms_error = 0;
      rc = ms_client_resolve_path( client, path, &path_metadata, &child_metadata, &ms_error );
      if( rc != 0 ) {
         errorf("ms_client_resolve_path(%s) rc = %d\n", path, rc );
         exit(1);
      }

      for( unsigned int i = 0; i < path_metadata.size(); i++ ) {
         struct md_entry* ent = &path_metadata[i];

         printf("path:  %s\n"
                "  type:    %s\n"
                "  URL:     %s\n"
                "  ctime:   %ld.%d\n"
                "  mtime:   %ld.%d\n"
                "  version: %ld\n"
                "  owner:   %d\n"
                "  volume:  %d\n"
                "  mode:    %o\n"
                "  size:    %ld\n"
                "  max_read_freshness:  %d\n"
                "  max_write_freshness: %d\n"
                "\n",
                ent->path, (ent->type == MD_ENTRY_FILE ? "file" : "directory"), ent->url, ent->ctime_sec, ent->ctime_nsec, ent->mtime_sec, ent->mtime_nsec, ent->version, ent->owner, ent->volume, ent->mode, ent->size, ent->max_read_freshness, ent->max_write_freshness );

         md_entry_free( ent );
         
      }

      for( unsigned int i = 0; i < child_metadata.size(); i++ ) {
         struct md_entry* ent = &child_metadata[i];

         printf("child: %s\n"
                "  type:    %s\n"
                "  URL:     %s\n"
                "  ctime:   %ld.%d\n"
                "  mtime:   %ld.%d\n"
                "  version: %ld\n"
                "  owner:   %d\n"
                "  volume:  %d\n"
                "  mode:    %o\n"
                "  size:    %ld\n"
                "  max_read_freshness:  %d\n"
                "  max_write_freshness: %d\n"
                "\n",
                ent->path, (ent->type == MD_ENTRY_FILE ? "file" : "directory"), ent->url, ent->ctime_sec, ent->ctime_nsec, ent->mtime_sec, ent->mtime_nsec, ent->version, ent->owner, ent->volume, ent->mode, ent->size, ent->max_read_freshness, ent->max_write_freshness );

         md_entry_free( ent );
      }
      printf("ms_error = %d\n", ms_error);
   }

   else if( strcmp( command, "create" ) == 0 ||
            strcmp( command, "mkdir" ) == 0 ||
            strcmp( command, "update" ) == 0 ||
            strcmp( command, "delete" ) == 0 ) {

      int update_index = optind;
      

      struct timespec now;
      clock_gettime( CLOCK_REALTIME, &now );

      if( !queue ) {
         
         if( update_index + 4 >= argc ) {
            fprintf(stderr, "Usage: <command> PATH URL MODE MAX_READ_FRESHNESS MAX_WRITE_FRESHNESS\n");
            exit(1);
         }

         // arguments are given on the command line
         char* path = argv[update_index];
         char* url = argv[update_index+1];
         mode_t mode = strtol( argv[update_index+2], NULL, 8 );
         uint32_t max_read_freshness = strtol( argv[update_index+3], NULL, 10 );
         uint32_t max_write_freshness = strtol( argv[update_index+4], NULL, 10 );

         struct md_entry new_ent;
         memset( &new_ent, 0, sizeof(new_ent) );
         
         new_ent.path = path;
         new_ent.url = url;
         new_ent.ctime_sec = now.tv_sec;
         new_ent.ctime_nsec = now.tv_nsec;
         new_ent.mtime_sec = now.tv_sec;
         new_ent.mtime_nsec = now.tv_nsec;
         new_ent.version = 1;
         new_ent.owner = conf.owner;
         new_ent.volume = conf.volume;
         new_ent.mode = mode;
         new_ent.size = 0;
         new_ent.max_read_freshness = max_read_freshness;
         new_ent.max_write_freshness = max_write_freshness;

         if( strcmp(command, "create") == 0 )
            rc = ms_client_create( client, &new_ent );

         if( strcmp(command, "mkdir") == 0 )
            rc = ms_client_mkdir( client, &new_ent );

         if( strcmp(command, "delete") == 0 )
            rc = ms_client_delete( client, &new_ent );

         if( strcmp(command, "update") == 0 )
            rc = ms_client_update( client, &new_ent );
      }
      else if( strcmp( command, "update" ) == 0 ) {
         // queue up the command several times
         uint64_t now_ms = md_current_time_millis();
         uint64_t total_delay = md_current_time_millis() + delay;

         struct md_entry new_ent;
         memset( &new_ent, 0, sizeof(new_ent) );

         new_ent.path = CALLOC_LIST( char, PATH_MAX + 1 );
         new_ent.url = CALLOC_LIST( char, PATH_MAX + 1 );
         
         int num_ents = 0;
         while( true ) {

            memset( new_ent.path, 0, PATH_MAX + 1 );
            memset( new_ent.url, 0, PATH_MAX + 1 );
            
            int num_read = scanf( "%s %s %o %d %d\n", new_ent.path, new_ent.url, &new_ent.mode, &new_ent.max_read_freshness, &new_ent.max_write_freshness );

            if( num_read == EOF )
               break;
            
            new_ent.ctime_sec = now.tv_sec;
            new_ent.ctime_nsec = now.tv_nsec;
            new_ent.mtime_sec = now.tv_sec;
            new_ent.mtime_nsec = now.tv_nsec;
            new_ent.version = 1;
            new_ent.owner = conf.owner;
            new_ent.volume = conf.volume;
            new_ent.size = 0;

            printf("ms-client-test: update '%s' in %ld millis\n", new_ent.path, total_delay - now_ms  );
            rc = ms_client_queue_update( client, new_ent.path, &new_ent, total_delay, 0 );
            if( rc != 0 ) {
               fprintf(stderr, "ms_client_queue_update rc = %d\n", rc );
            }
            
            total_delay += delay;
            num_ents++;
         }

         free( new_ent.path );
         free( new_ent.url );
         
         // sleep for the total delay, plus a second
         printf("waiting for update thread...\n");
         usleep( (total_delay - md_current_time_millis() + 1500 * num_ents ) * 1000L );
      }
      else {
         fprintf(stderr, "-q and -d are only used with -c 'update'\n");
         exit(1);
      }
      
      if( rc != 0 ) {
         fprintf(stderr, "%s rc = %d\n", command, rc );
         exit(1);
      }
   }

   ms_client_destroy( client );
   free( client );
   
   return 0;
}

