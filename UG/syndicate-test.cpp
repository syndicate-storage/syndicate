/*
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
*/

#include "syndicate-test.h"

// test replication
int replication_test( struct md_syndicate_conf* conf, uint64_t blocking_factor ) {
   // create a file to upload
   char* rp_dirname = md_dirname( REPLICA_TESTFILE_PATH, NULL );
   int rc = md_mkdirs( rp_dirname );
   if( rc != 0 ) {
      SG_error(" md_mkdirs rc = %d\n", rc );
      exit(1);
   }
   free( rp_dirname );
   
   FILE* rpfile = fopen( REPLICA_TESTFILE_PATH, "w" );
   if( !rpfile ) {
      SG_error(" fopen errno = %d\n", -errno );
      exit(1);
   }

   // make some random data
   char* buf = SG_CALLOC( char, blocking_factor );
   for( uint64_t i = 0; i < blocking_factor; i++ ) {
      buf[i] = rand() % 256;
   }

   rc = (int)fwrite( buf, 1, blocking_factor, rpfile );
   if( (unsigned)rc != blocking_factor ) {
      SG_error(" fwrite rc = %d\n", rc );
      exit(1);
   }

   fclose( rpfile );

   free( buf );
   
   // replicate this file
   // int replicate_begin( char const* data_root, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, char* replica_url );
   rc = replicate_begin_all( conf, REPLICA_TESTFILE_FS_PATH, REPLICA_TESTFILE_FILE_VERSION, REPLICA_TESTFILE_BLOCK_ID, REPLICA_TESTFILE_BLOCK_VERSION, 1, 1 );
   if( rc != 0 ) {
      SG_error(" replicate_begin rc = %d\n", rc );
      exit(1);
   }

   // wait for this file to be replicated
   // int replicate_wait( char* local_path, bool block );
   rc = replicate_wait( REPLICA_TESTFILE_FS_FULLPATH, true );

   return rc;
}


// put a serialized md_query packet to disk
int make_md_query_file( struct md_syndicate_conf* conf, char const* fs_path, char const* output ) {
   FILE* f = fopen( output, "w" );
   if( !f ) {
      SG_error(" fopen errno = %d\n", -errno );
      exit(1);
   }

   md_query::md_packet pkt;
   md_query_request_packet( &pkt, fs_path );

   string data;
   if( !pkt.SerializeToString( &data ) ) {
      SG_error("%s", " serialization failed\n");
      exit(1);
   }

   uint32_t sz = htonl( data.size() );

   fwrite( &sz, 1, 4, f );
   fwrite( data.data(), 1, data.size(), f );
   fclose( f );

   return 0;
}



// usage
void usage( char* name, int exitrc ) {
   fprintf(stderr,
"\
Usage: %s [-c CONFIG] [-p HTTP_PORTNUM] [-u USER_SECRETS]\n\
Options:\n\
   -c CONFIG                 Use an alternate config file at CONFIG\n\
   -p HTTP_PORTNUM           Listen on port HTTP_PORTNUM\n\
   -u USER_SECRETS           Read user ID, username, and password information from the file at USER_SECRETS\n\
\n\
Secrets file format:\n\
   user_id:username:SHA1(password)\n\
   user_id:username:SHA1(password)\n\
   ...\n\
\n\
where user_id is the user's numeric ID in Syndicate; username is their Syndicate username, and SHA1(password) is the SHA-1 hash of \n\
the user's Syndicate password\n",
name );

   exit(exitrc);
}


int main( int argc, char** argv ) {

   curl_global_init(CURL_GLOBAL_ALL);
   GOOGLE_PROTOBUF_VERIFY_VERSION;

   int rc = 0;
   char* config_file = (char*)REPLICA_DEFAULT_CONFIG;

   // process command-line options
   int c;
   bool good_input = true;
   char* secrets_file = NULL;
   int portnum = 0;

   while((c = getopt(argc, argv, "fc:p:u:l:P:")) != -1) {
      switch( c ) {
         case '?': {
            usage( argv[0], 1 );
         }
         case 'c': {
            config_file = optarg;
            break;
         }
         case 'p': {
            portnum = strtol( optarg, NULL, 10 );
            if( portnum <= 0 )
               good_input = false;
            break;
         }
         case 'u': {
            secrets_file = optarg;
            break;
         }
         default: {
            fprintf(stderr, "Ignoring unrecognized option %c\n", c);
            good_input = false;
            break;
         }
      }
   }

   if( !good_input ) {
      usage( argv[0], 1 );
   }

   /*
   // read the config
   struct md_syndicate_conf conf;
   if( md_read_conf( config_file, &conf ) != 0 ) {
      SG_error("Could not read config at %s\n", config_file);
      usage( argv[0], 1 );
   }

   // user-given portnum?
   if( portnum > 0 ) {
      conf.portnum = portnum;
   }
   if( conf.portnum == 0 ) {
      SG_error("Invalid port number %d.  Specify PORTNUM in the config file or pass -p\n", conf.portnum);
      exit(1);
   }

   // secrets file supplied in args?
   if( secrets_file ) {
      if( conf.secrets_file ) {
         free( conf.secrets_file );
      }
      conf.secrets_file = secrets_file;
   }

   // set the config
   if( md_init( &conf, NULL ) != 0 )
      exit(1);

   md_connect_timeout( conf.query_timeout );
   md_signals( 0 );        // no signals

   // read the users file
   struct md_user_entry **users = NULL;
   if( conf.secrets_file ) {
      users = md_parse_secrets_file( conf.secrets_file );
      if( users == NULL ) {
         exit(1);
      }
   }
   else {
      SG_error("No secrets file given.  Pass -u or specify a value for %s in the config\n", SECRETS_FILE_KEY );
      usage( argv[0], 1 );
   }*/

   struct md_HTTP syndicate_http;
   struct md_user_entry** md_users;
   syndicate_init( config_file, &syndicate_http, -1, NULL, NULL, &md_users );

   /*
   rc = replication_init( &conf );
   if( rc != 0 ) {
      SG_error(" replication_init rc = %d\n", rc );
      exit(1);
   }

   rc = replication_test( &conf, conf.blocking_factor );

   SG_debug(" replication_test rc = %d\n", rc );
   
   replication_shutdown();
   */
   
   struct syndicate_state* state = syndicate_get_state();
   struct fs_entry* fent = SG_CALLOC( struct fs_entry, 1 );
   fs_entry_init_file( state->core, fent, "foo", "file:///tmp/syndicate-data/foo", NULL, 123, 12345, 0, 0666, 61440000, 1347783067, 123456789, NULL );

   printf("\n\n*** same url, different block\n\n");
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 1, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 2, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 3, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 4, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 5, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 6, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 7, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 8, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 9, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 10, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 11, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 12, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 13, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 14, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 15, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 16, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 17, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 18, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 19, 1 );

   char* mf_str;
   ssize_t mf_len;
   bool parsed;
   Serialization::ManifestMsg mmsg;
   
   mf_len = fs_entry_serialize_manifest( state->core, fent, &mf_str );
   printf("\nmanifest is %d bytes long\n", mf_len );
   free( mf_str );

   printf("\n\n*** same url, same block\n\n");
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 0, 1 );

   mf_len = fs_entry_serialize_manifest( state->core, fent, &mf_str );
   printf("\nmanifest is %d bytes long\n", mf_len );
   
   parsed = mmsg.ParseFromString( string(mf_str, mf_len) );
   if( !parsed ) {
      printf(" ERROR: failed to parse manifest!\n");
      exit(1);
   }

   free( mf_str );

   printf("\n\n*** remote writer, same block\n\n");
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 0, 1 );

   mf_len = fs_entry_serialize_manifest( state->core, fent, &mf_str );
   printf("\nmanifest is %d bytes long\n", mf_len );
   
   parsed = mmsg.ParseFromString( string(mf_str, mf_len) );
   if( !parsed ) {
      printf(" ERROR: failed to parse manifest!\n");
      exit(1);
   }

   free( mf_str );

   printf("\n\n*** remote writer (different URL), same block\n\n");
   fent->manifest->put_block_url( "http://www.sniffme.com:32780///foo", 123, 0, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780//foo", 123, 0, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 0, 1 );

   mf_len = fs_entry_serialize_manifest( state->core, fent, &mf_str );
   printf("\nmanifest is %d bytes long\n", mf_len );

   parsed = mmsg.ParseFromString( string(mf_str, mf_len) );
   if( !parsed ) {
      printf(" ERROR: failed to parse manifest!\n");
      exit(1);
   }

   free( mf_str );

   printf("\n\n*** remote writer, different block\n\n");
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 1, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 2, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 3, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 4, 1 );
   fent->manifest->put_block_url( "http://www.sniffme.com:32780/foo", 123, 5, 1 );

   mf_len = fs_entry_serialize_manifest( state->core, fent, &mf_str );
   printf("\nmanifest is %d bytes long\n", mf_len );

   parsed = mmsg.ParseFromString( string(mf_str, mf_len) );
   if( !parsed ) {
      printf(" ERROR: failed to parse manifest!\n");
      exit(1);
   }

   free( mf_str );

   printf("\n\n*** More writers, different block\n\n");
   fent->manifest->put_block_url( "http://www.poop.com:32780/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "http://www.poop.com:32780/foo", 123, 1, 1 );
   fent->manifest->put_block_url( "http://www.poop.com:32780/foo", 123, 2, 1 );
   
   fent->manifest->put_block_url( "http://www.crap.com:32780/foo", 123, 0, 1 );
   fent->manifest->put_block_url( "http://www.crap.com:32780/foo", 123, 1, 1 );
   
   fent->manifest->put_block_url( "http://www.dookie.com:32780/foo", 123, 0, 1 );

   mf_len = fs_entry_serialize_manifest( state->core, fent, &mf_str );
   printf("\nmanifest is %d bytes long\n", mf_len );


   parsed = mmsg.ParseFromString( string(mf_str, mf_len) );
   if( !parsed ) {
      printf(" ERROR: failed to parse manifest!\n");
      exit(1);
   }

   free( mf_str );

   printf("\n\n*** Consolidation on the Left\n\n");
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 0, 2 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 1, 2 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 2, 2 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 3, 2 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 4, 2 );
   fent->manifest->put_block_url( "file:///tmp/syndicate-data/foo", 123, 5, 2 );
   

   mf_len = fs_entry_serialize_manifest( state->core, fent, &mf_str );
   printf("\nmanifest is %d bytes long\n", mf_len );


   parsed = mmsg.ParseFromString( string(mf_str, mf_len ) );
   if( !parsed ) {
      printf(" ERROR: failed to parse manifest!\n");
      exit(1);
   }

   free( mf_str );

   fs_entry_destroy( fent, false );
   free( fent );

   //make_md_query_file( &conf, "/file100k", "/tmp/file100k.query");

   google::protobuf::ShutdownProtobufLibrary();

   return 0;
}
