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

#include "opts.h"

// fill opts with defaults
int syndicate_default_opts( struct syndicate_opts* opts ) {
   memset( opts, 0, sizeof(struct syndicate_opts) );
   
   opts->config_file = (char*)CLIENT_DEFAULT_CONFIG;
   opts->flush_replicas = true;
   
   opts->cache_soft_limit = CACHE_DEFAULT_SOFT_LIMIT;
   opts->cache_hard_limit = CACHE_DEFAULT_HARD_LIMIT;
   
   return 0;
}

// parse a long 
int syndicate_parse_long( int c, char* opt, long* result ) {
   char* tmp = NULL;
   int rc = 0;
   
   *result = strtol( opt, &tmp, 10 );
   
   if( tmp == opt ) {
      fprintf(stderr, "Invalid value for option -%c", c );
      rc = -1;
   }
   return rc;
}

// read all of stdin 
ssize_t syndicate_read_stdin( char* stdin_buf ) {
   ssize_t nr = 0;
   size_t r = 0;
   
   while( true ) {
      r = read( STDIN_FILENO, stdin_buf + nr, SYNDICATE_OPTS_STDIN_MAX - nr );
      if( r == 0 ) {
         // EOF 
         break;
      }
      if( nr >= SYNDICATE_OPTS_STDIN_MAX ) {
         // too much 
         nr = -EOVERFLOW;
         break;
      }
      
      nr += r;
   }
   
   return nr;
}

// get options from stdin.
// on success, set argc and argv with the actual arguments (the caller must free them) and return 0
// on failure, return negative
int syndicate_read_opts_from_stdin( int* argc, char*** argv ) {
   
   // realistically speaking, stdin can't really be longer than SYNDICATE_OPTS_STDIN_MAX 
   char* stdin_buf = CALLOC_LIST( char, SYNDICATE_OPTS_STDIN_MAX + 1 );
   ssize_t stdin_len = syndicate_read_stdin( stdin_buf );
   
   if( stdin_len < 0 ) {
      errorf("Failed to read stdin, rc = %zd\n", stdin_len );
      return -ENODATA;
   }
   
   // lex it 
   wordexp_t expanded_stdin;
   memset( &expanded_stdin, 0, sizeof(wordexp_t) );
   
   int rc = wordexp( stdin_buf, &expanded_stdin, WRDE_SHOWERR );
   if( rc != 0 ) {
      errorf("wordexp rc = %d\n", rc );
      
      wordfree( &expanded_stdin );
      free( stdin_buf );
      return -EINVAL;
   }
   else {
      // got it!
      *argc = expanded_stdin.we_wordc;
      *argv = expanded_stdin.we_wordv;
      free( stdin_buf );
      return 0;
   }
}


// clean up the opts structure, freeing things we don't need 
int syndicate_cleanup_opts( struct syndicate_opts* opts ) {
   struct mlock_buf* to_free[] = {
      &opts->user_pkey_pem,
      &opts->gateway_pkey_pem,
      &opts->gateway_pkey_decryption_password,
      &opts->password,
      NULL
   };
   
   for( int i = 0; to_free[i] != NULL; i++ ) {
      if( to_free[i]->ptr != NULL ) {
         mlock_free( to_free[i] );
      }
   }
   
   return 0;
}

// load an optarg into an mlock'ed buffer.
// exit on failure, since this is used to load sensitive (i.e. secret) information.
int syndicate_load_mlock_buf( struct mlock_buf* buf, char* str ) {
   size_t len = strlen(str);
   int rc = mlock_calloc( buf, len + 1 );
   if( rc != 0 ) {
      errorf("mlock_calloc rc = %d\n", rc );
      exit(1);
   }
   else {
      memcpy( buf->ptr, str, len );
      buf->len = len;
   }
   
   return 0;
}

// parse opts from argv.
// optionally supply the optind after parsing (if it's not NULL)
// return 0 on success
// return -1 on failure
int syndicate_parse_opts_impl( struct syndicate_opts* opts, int argc, char** argv, int* out_optind, char const* special_opts, int (*special_opt_handler)(int, char*), bool no_stdin ) {
   
   static struct option syndicate_options[] = {
      {"config-file",     required_argument,   0, 'c'},
      {"volume-name",     required_argument,   0, 'v'},
      {"username",        required_argument,   0, 'u'},
      {"password",        required_argument,   0, 'p'},
      {"gateway",         required_argument,   0, 'g'},
      {"MS",              required_argument,   0, 'm'},
      {"volume-pubkey",   required_argument,   0, 'V'},
      {"gateway-pkey",    required_argument,   0, 'G'},
      {"syndicate-pubkey", required_argument,  0, 'S'},
      {"gateway-pkey-password", required_argument, 0, 'K'},
      {"tls-pkey",        required_argument,   0, 'T'},
      {"tls-cert",        required_argument,   0, 'C'},
      {"no-flush-replicas", no_argument,       0, 'F'},
      {"storage-root",    required_argument,   0, 'r'},
      {"cache-soft-limit", required_argument,  0, 'l'},
      {"cache-hard-limit", required_argument,  0, 'L'},
      {"read-stdin",      no_argument,         0, 'R'},
      {"user-pkey",       required_argument,   0, 'U'},
      {"user-pkey-pem",   required_argument,   0, 'P'},
      {"debug-level",     required_argument,   0, 'd'},
      {"anonymous",       no_argument,         0, 'a'},
      {0, 0, 0, 0}
   };

   struct option* all_options = NULL;
   
   int rc = 0;
   int opt_index = 0;
   int c = 0;
   
   char const* default_optstr = "c:v:u:p:P:m:Fg:V:G:S:T:C:K:l:L:r:RU:d:a";
   
   int num_default_options = 0;         // needed for freeing special arguments
   char* optstr = NULL;
   
   // merge in long-opts for special options
   if( special_opts != NULL ) {
      optstr = CALLOC_LIST( char, strlen(default_optstr) + strlen(special_opts) + 1 );
      sprintf( optstr, "%s%s", default_optstr, special_opts );
      
      // how many options?
      int num_options = 0;
      int num_special_options = 0;
      
      for( int i = 0; syndicate_options[i].name != NULL; i++ ) {
         num_options ++;
         num_default_options ++;
      }
      
      for( int i = 0; special_opts[i] != '\0'; i++ ) {
         if( special_opts[i] != ':' ) {
            num_options ++;
            num_special_options ++;
         }
      }
      
      // make a new options table
      all_options = CALLOC_LIST( struct option, num_options + 1 );
      
      memcpy( all_options, syndicate_options, num_default_options * sizeof(struct option) );
      
      int k = 0;
      for( int i = 0; i < num_special_options; i++ ) {
         int ind = i + num_default_options;
         
         int has_arg = no_argument;
         if( k + 1 < (signed)strlen(special_opts) && special_opts[k+1] == ':' ) {
            has_arg = required_argument;
         }
         
         if( special_opts[k] == '\0' ) {
            // shouldn't happen, but check anyway 
            errorf("%s", "Ran out of special options early\n");
            break;
         }
         
         char* buf = CALLOC_LIST( char, 12 );
         sprintf(buf, "special-%c", special_opts[k] );
         
         all_options[ind].name = buf;
         all_options[ind].has_arg = has_arg;
         all_options[ind].flag = NULL;
         all_options[ind].val = special_opts[k];
         
         k++;
         if( has_arg )
            k++;
      }
   }
   else {
      optstr = (char*)default_optstr;
      all_options = syndicate_options;
   }
   
   c = 0;
   while(rc == 0 && c != -1) {
      
      c = getopt_long(argc, argv, optstr, all_options, &opt_index);
      
      if( c == -1 )
         break;
      
      switch( c ) {
         case 'v': {
            opts->volume_name = optarg;
            break;
         }
         case 'c': {
            opts->config_file = optarg;
            break;
         }
         case 'u': {
            opts->username = optarg;
            break;
         }
         case 'p': {
            syndicate_load_mlock_buf( &opts->password, optarg );
            memset( optarg, 0, strlen(optarg) );
            break;
         }
         case 'm': {
            opts->ms_url = optarg;
            break;
         }
         case 'g': {
            opts->gateway_name = optarg;
            break;
         }
         case 'F': {
            // don't flush replicas
            opts->flush_replicas = false;
            break;
         }
         case 'V': {
            opts->volume_pubkey_path = optarg;
            break;
         }
         case 'G': {
            opts->gateway_pkey_path = optarg;
            break;
         }
         case 'S': {
            opts->syndicate_pubkey_path = optarg;
            break;
         }
         case 'T': {
            opts->tls_pkey_path = optarg;
            break;
         }
         case 'C': {
            opts->tls_cert_path = optarg;
            break;
         }
         case 'r': {
            opts->storage_root = optarg;
            break;
         }
         case 'K': {
            syndicate_load_mlock_buf( &opts->gateway_pkey_decryption_password, optarg );
            memset( optarg, 0, strlen(optarg) );
            break;
         }
         case 'U': {
            // read the file...
            int rc = md_load_secret_as_string( &opts->user_pkey_pem, optarg );
            if( rc != 0 ) {
               errorf("md_load_secret_as_string(%s) rc = %d\n", optarg, rc );
               rc = -1;
               break;
            }
            break;
         }
         case 'P': {
            syndicate_load_mlock_buf( &opts->user_pkey_pem, optarg );
            memset( optarg, 0, strlen(optarg) );
            break;
         }
         case 'l': {
            long lim = 0;
            rc = syndicate_parse_long( c, optarg, &lim );
            if( rc == 0 ) {
               opts->cache_soft_limit = (size_t)lim;
            }
            else {
               errorf("Failed to parse -l, rc = %d\n", rc );
               rc = -1;
            }
            break;
         }
         case 'L': {
            long lim = 0;
            rc = syndicate_parse_long( c, optarg, &lim );
            if( rc == 0 ) {
               opts->cache_hard_limit = (size_t)lim;
            }
            else {
               errorf("Failed to parse -L, rc = %d\n", rc );
               rc = -1;
            }
            break;
         }
         case 'R': {
            if( no_stdin ) {
               // this is invalid--we're already parsing from stdin 
               fprintf(stderr, "Invalid argument: cannot process -R when reading args from stdin\n");
               return -EINVAL;
            }
            else {
               dbprintf("%s", "Reading arguments from stdin\n");
               opts->read_stdin = true;
            }
            break;
         }
         case 'd': {
            long debug_level = 0;
            rc = syndicate_parse_long( c, optarg, &debug_level );
            if( rc == 0 ) {
               opts->debug_level = (int)debug_level;
            }
            else {
               errorf("Failed to parse -d, rc = %d\n", rc );
               rc = -1;
            }
            break;
         }
         case 'a': {
            opts->anonymous = true;
            break;
         }
         default: {
            rc = -1;
            if( special_opt_handler ) {
               rc = special_opt_handler( c, optarg );
            }
            if( rc == -1 ) {
               fprintf(stderr, "Unrecognized option -%c\n", c );
               rc = -1;
            }
            break;
         }
      }
   }

   if( optstr != default_optstr ) {
      free( optstr );
   }
   
   if( all_options != syndicate_options ) {
      for( int i = num_default_options; all_options[i].name != NULL; i++ ) {
         free( (void*)all_options[i].name );
      }
      free( all_options );
   }
   
   if( out_optind != NULL ) {
      *out_optind = optind;
   }
   
   if( optind < argc ) {
      opts->mountpoint = argv[optind];
   }
   
   
   return rc;
}

// parse syndicate options
int syndicate_parse_opts( struct syndicate_opts* opts, int argc, char** argv, int* out_optind, char const* special_opts, int (*special_opt_handler)(int, char*) ) {
   int rc = syndicate_parse_opts_impl( opts, argc, argv, out_optind, special_opts, special_opt_handler, false );
   if( rc != 0 ) {
      return rc;
   }
   
   // do we need to parse stdin?
   if( opts->read_stdin ) {
      int new_argc = 0;
      char** new_argv = NULL;
      
      int rc = syndicate_read_opts_from_stdin( &new_argc, &new_argv );
      if( rc != 0 ) {
         errorf("Failed to read args from stdin, rc = %d\n", rc );
         return -ENODATA;
      }
      else {
         // clear opts 
         syndicate_default_opts( opts );
         
         // NOTE: set this to 0, since otherwise getopt_long decides to skip the first argument
         optind = 0;
         
         // got new data.  parse it
         rc = syndicate_parse_opts_impl( opts, new_argc, new_argv, out_optind, special_opts, special_opt_handler, true );
         
         // NOTE: don't free new_argv--opts currently points to its strings.  It's fine to let this leak for now.
      }
   }
   
   return rc;
}

// print usage and exit
void syndicate_common_usage( char* progname ) {
   fprintf(stderr, "\
Usage of %s\n\
Common Syndicate command-line options\n\
Required arguments:\n\
   -m, --MS MS_URL\n\
            URL to your Metadata Service\n\
   -u, --username USERNAME\n\
            Syndicate account username\n\
   -p, --password PASSWORD\n\
            Syndicate account password.\n\
            Required if -U is not given.\n\
   -U, --user-pkey PATH\n\
            Path to user private key.\n\
            Required if -p is not given.\n\
   -P, --user-pkey-pem STRING\n\
            Raw PEM-encoded user private key.\n\
            Can be used in place of -U.\n\
   -v, --volume VOLUME_NAME\n\
            Name of the Volume you are going to access\n\
   -g, --gateway GATEWAY_NAME\n\
            Name of this gateway\n\
   -a, --anonymous\n\
            Sign in anonymously.  You will have read-only\n\
            permissions.  If you use this option, you do not\n\
            need -U, -P, -g, -u, or -p.\n\
\n\
Optional arguments:\n\
   -V, --volume-pubkey VOLUME_PUBLIC_KEY_PATH\n\
            Path to the Volume's metadata public key\n\
   -S, --syndicate-pubkey SYNDICATE_PUBLIC_KEY_PATH\n\
            Path to the Syndicate public key.  If not given,\n\
            it will be downloaded and logged when the gateway\n\
            starts.\n\
   -T, --tls-pkey TLS_PRIVATE_KEY_PATH\n\
            Path to this gateway's TLS private key\n\
   -C, --tls-cert TLS_CERTIFICATE_PATH\n\
            Path to this gateway's TLS certificate\n\
   -F, --no-flush-replicas\n\
            If given, flush all ongoing replicas before exiting\n\
   -r, --storage-root STORAGE_ROOT\n\
            Cache local state at a particular location\n\
   -G, --gateway-pkey GATEWAY_PRIVATE_KEY_PATH\n\
            Path to this gateway's private key.  If no private key\n\
            is given, then it will be downloaded from the MS.\n\
   -K, --gateway-pkey-decryption-password DECRYPTION_PASSWORD\n\
            Password to decrypt the private key.\n\
   -l, --cache-soft-limit LIMIT\n\
            Soft limit on the size of the local cache (bytes).\n\
   -L, --cache-hard-limit LIMIT\n\
            Hard limit on the size of the local cache (bytes).\n\
   -R, --read-stdin\n\
            If set, read all command-line options from stdin.\n\
   -d, --debug-level DEBUG_LEVEL\n\
            Debugging level.\n\
            Pass 0 (the default) for no debugging output.\n\
            Pass 1 for global debugging messages.\n\
            Pass 2 to add locking debugging.\n\
\n", progname );
}

