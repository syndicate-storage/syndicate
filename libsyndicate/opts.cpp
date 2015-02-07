/*
   Copyright 2014 The Trustees of Princeton University

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

#include "libsyndicate/opts.h"

// fill opts with defaults
int md_opts_default( struct md_opts* opts ) {
   memset( opts, 0, sizeof(struct md_opts) );
   return 0;
}

// parse a long 
int md_opts_parse_long( int c, char* opt, long* result ) {
   char* tmp = NULL;
   int rc = 0;
   
   errno = 0;
   *result = strtol( opt, &tmp, 10 );
   
   if( tmp == opt || errno != 0 ) {
      fprintf(stderr, "Invalid value for option -%c", c );
      rc = -1;
   }
   return rc;
}

// read all of stdin 
// return the number of bytes read on success
// return -EOVERFLOW if we read more than SYNDICATE_OPTS_STDIN_MAX
ssize_t md_read_stdin( char* stdin_buf ) {
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
   
   // remove trailing newlines 
   for( int i = nr; i >= 0; i-- ) {
      if( stdin_buf[i] == '\0' ) {
         continue;
      }
      
      if( stdin_buf[i] == '\r' || stdin_buf[i] == '\n' ) {
         stdin_buf[i] = '\0';
         continue;
      }
      
      break;
   }
   
   return nr;
}

// get options from stdin.
// return 0 on success, and set *argc and *argv with the actual arguments (the caller must free them) and return 0
// return -ENODATA if we failed to read stdin 
// return -EINVAL if we failed to parse stdin
// return -ENOMEM if OOM
int md_read_opts_from_stdin( int* argc, char*** argv ) {
   
   // realistically speaking, stdin can't really be longer than SYNDICATE_OPTS_STDIN_MAX 
   char* stdin_buf = SG_CALLOC( char, SYNDICATE_OPTS_STDIN_MAX + 1 );
   if( stdin_buf == NULL ) {
      return -ENOMEM;
   }
   
   ssize_t stdin_len = md_read_stdin( stdin_buf );
   
   if( stdin_len < 0 ) {
      SG_error("Failed to read stdin, rc = %zd\n", stdin_len );
      return -ENODATA;
   }
   
   // lex it 
   wordexp_t expanded_stdin;
   memset( &expanded_stdin, 0, sizeof(wordexp_t) );
   
   int rc = wordexp( stdin_buf, &expanded_stdin, WRDE_SHOWERR );
   if( rc != 0 ) {
      SG_error("wordexp rc = %d\n", rc );
      
      wordfree( &expanded_stdin );
      SG_safe_free( stdin_buf );
      return -EINVAL;
   }
   else {
      // got it!
      *argc = expanded_stdin.we_wordc;
      *argv = expanded_stdin.we_wordv;
      SG_safe_free( stdin_buf );
      return 0;
   }
}


// clean up the opts structure, freeing things we don't need 
// always succeeds
int md_opts_cleanup( struct md_opts* opts ) {
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

// free the opts structure 
// always succeeds 
int md_opts_free( struct md_opts* opts ) {
   
   md_opts_cleanup( opts );
   
   char* to_free[] = {
      opts->config_file,
      opts->username,
      opts->volume_name,
      opts->ms_url,
      opts->gateway_name,
      opts->volume_pubkey_path,
      opts->gateway_pkey_path,
      opts->syndicate_pubkey_path,
      opts->hostname,
      opts->storage_root,
      opts->first_nonopt_arg,
      opts->volume_pubkey_pem,
      opts->syndicate_pubkey_pem,
      opts->tls_pkey_path,
      opts->tls_cert_path,
      NULL
   };
   
   for( int i = 0; to_free[i] != NULL; i++ ) {
      SG_safe_free( to_free[i] );
   }
   
   memset( opts, 0, sizeof(struct md_opts) );
   
   return 0;
}

// load an optarg into an mlock'ed buffer.
// return 0 on success
// return negative on error (see mlock_calloc)
int md_load_mlock_buf( struct mlock_buf* buf, char* str ) {
   size_t len = strlen(str);
   int rc = mlock_calloc( buf, len + 1 );
   
   if( rc != 0 ) {
      SG_error("mlock_calloc rc = %d\n", rc );
      return rc;
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
// return -EINVAL if there are duplicate short opt definitions
// return -ENOMEM if out of memory
int md_opts_parse_impl( struct md_opts* opts, int argc, char** argv, int* out_optind, char const* special_opts, int (*special_opt_handler)(int, char*), bool no_stdin ) {
   
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
      {"storage-root",    required_argument,   0, 'r'},
      {"read-stdin",      no_argument,         0, 'R'},
      {"user-pkey",       required_argument,   0, 'U'},
      {"user-pkey-pem",   required_argument,   0, 'P'},
      {"debug-level",     required_argument,   0, 'd'},
      {"hostname",        required_argument,   0, 'H'},
      {"foreground",      no_argument,         0, 'f'},
      {"cache-soft-limit",required_argument,   0, 'l'},
      {"cache-hard-limit",required_argument,   0, 'L'},
      {0, 0, 0, 0}
   };

   struct option* all_options = NULL;
   char** special_longopts = NULL;
   int special_longopts_idx = 0;
   
   int rc = 0;
   int opt_index = 0;
   int c = 0;
   
   char const* default_optstr = "c:v:u:p:g:m:V:G:S:K:T:C:r:RU:P:d:H:fl:L:";
   
   int num_default_options = 0;         // needed for freeing special arguments
   char* optstr = NULL;
   
   // merge in long-opts for special options
   if( special_opts != NULL ) {
      
      bool has_dups = false;
      
      // sanity check--verify no duplicates 
      for( unsigned int i = 0; i < strlen(special_opts); i++ ) {
         
         if( special_opts[i] == ':' ) {
            continue; 
         }
         
         if( index( default_optstr, special_opts[i] ) != NULL ) {
            SG_error("BUG: Duplicate option '%c'\n", special_opts[i] );
            has_dups = true;
         }
      }
      
      if( has_dups ) {
         // can't continue 
         return -EINVAL;
      }
      
      // update the optstr
      optstr = SG_CALLOC( char, strlen(default_optstr) + strlen(special_opts) + 1 );
      if( optstr == NULL ) {
         return -ENOMEM;
      }
      
      // store long-options 
      special_longopts = SG_CALLOC( char*, strlen(special_opts) + 1 );
      if( special_longopts == NULL ) {
      
         SG_safe_free( optstr );
         return -ENOMEM;
      }
      
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
      all_options = SG_CALLOC( struct option, num_options + 1 );
      if( all_options == NULL ) {
         
         SG_safe_free( optstr );
         return -ENOMEM;
      }
      
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
            SG_error("%s", "Ran out of special options early\n");
            break;
         }
         
         char* buf = SG_CALLOC( char, 12 );
         if( buf == NULL ) {
            
            // free all previously-alloced long options, and free everything else 
            SG_FREE_LIST( special_longopts, free );
            SG_safe_free( all_options );
            SG_safe_free( optstr );
         }
         
         // save this so we can free it later
         special_longopts[ special_longopts_idx ] = buf;
         special_longopts_idx++;
         
         sprintf(buf, "special-%c", special_opts[k] );
         
         all_options[ind].name = buf;
         all_options[ind].has_arg = has_arg;
         all_options[ind].flag = NULL;
         all_options[ind].val = special_opts[k];
         
         k++;
         if( has_arg ) {
            k++;
         }
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
            
            opts->volume_name = SG_strdup_or_null( optarg );
            if( opts->volume_name == NULL ) {
               rc = -ENOMEM;
               break;
            }
            
            break;
         }
         case 'c': {
            
            opts->config_file = SG_strdup_or_null( optarg );
            if( opts->config_file == NULL ) {
               rc = -ENOMEM;
               break;
            }
            
            break;
         }
         case 'u': {
            
            opts->username = SG_strdup_or_null( optarg );
            if( opts->username == NULL ) {
               rc = -ENOMEM;
               break;
            }
            
            break;
         }
         case 'p': {
            
            rc = md_load_mlock_buf( &opts->password, optarg );
            if( rc != 0 ) {
               break;
            }
            
            // clear this--it's sensitive
            memset( optarg, 0, strlen(optarg) );
            break;
         }
         case 'm': {
            
            opts->ms_url = SG_strdup_or_null( optarg );
            if( opts->ms_url == NULL ) {
               rc = -ENOMEM;
               break;
            }
            
            break;
         }
         case 'g': {
            
            opts->gateway_name = SG_strdup_or_null( optarg );
            if( opts->gateway_name == NULL ) {
               rc = -ENOMEM;
               break;
            }
            
            break;
         }
         case 'V': {
            
            opts->volume_pubkey_path = SG_strdup_or_null( optarg );
            if( opts->volume_pubkey_path == NULL ) {
               rc = -ENOMEM;
               break;
            }
            break;
         }
         case 'G': {
            
            opts->gateway_pkey_path = SG_strdup_or_null( optarg );
            if( opts->gateway_pkey_path == NULL ) {
               rc = -ENOMEM;
               break;
            }
            break;
         }
         case 'S': {
            
            opts->syndicate_pubkey_path = SG_strdup_or_null( optarg );
            if( opts->syndicate_pubkey_path == NULL ) {
               rc = -ENOMEM;
               break;
            }
            break;
         }
         case 'T': {
            
            opts->tls_pkey_path = SG_strdup_or_null( optarg );
            if( opts->tls_pkey_path == NULL ) {
               rc = -ENOMEM;
               break;
            }
            break;
         }
         case 'C': {
            
            opts->tls_cert_path = SG_strdup_or_null( optarg );
            if( opts->tls_pkey_path == NULL ) {
               rc = -ENOMEM;
               break;
            }
            break;
         }
         case 'r': {
            
            opts->storage_root = SG_strdup_or_null( optarg );
            if( opts->storage_root == NULL ) {
               rc = -ENOMEM;
               break;
            }
            break;
         }
         case 'K': {
            rc = md_load_mlock_buf( &opts->gateway_pkey_decryption_password, optarg );
            if( rc != 0 ) {
               break;
            }
            
            // clear this--it's sensitive
            memset( optarg, 0, strlen(optarg) );
            break;
         }
         case 'U': {
            
            // read the file...
            rc = md_load_secret_as_string( &opts->user_pkey_pem, optarg );
            if( rc != 0 ) {
               
               SG_error("md_load_secret_as_string(%s) rc = %d\n", optarg, rc );
               rc = -1;
               break;
            }
            break;
         }
         case 'P': {
            
            rc = md_load_mlock_buf( &opts->user_pkey_pem, optarg );
            if( rc != 0 ) {
               break;
            }
            
            // clear this--it's sensitive
            memset( optarg, 0, strlen(optarg) );
            break;
         }
         case 'R': {
            
            if( no_stdin ) {
               // this is invalid--we're already parsing from stdin 
               fprintf(stderr, "Invalid argument: cannot process -R when reading args from stdin\n");
               rc = -EINVAL;
            }
            else {
               SG_debug("%s", "Reading arguments from stdin\n");
               opts->read_stdin = true;
            }
            break;
         }
         case 'd': {
            
            long debug_level = 0;
            rc = md_opts_parse_long( c, optarg, &debug_level );
            if( rc == 0 ) {
               opts->debug_level = (int)debug_level;
            }
            else {
               fprintf(stderr, "Failed to parse -d, rc = %d\n", rc );
               rc = -1;
            }
            break;
         }
         case 'f': {
            opts->foreground = true;
            break;
         }
         case 'H': {
            
            opts->hostname = SG_strdup_or_null( optarg );
            if( opts->hostname == NULL ) {
               rc = -ENOMEM;
               break;
            }
            break;
         }
         case 'l': {
            
            long l = 0;
            rc = md_opts_parse_long( c, optarg, &l );
            if( rc == 0 ) {
               opts->cache_soft_limit = l;
            }
            else {
               fprintf(stderr, "Failed to parse -l, rc = %d\n", rc );
               rc = -1;
            }
            break;
         }
         case 'L': {
            
            long l = 0;
            rc = md_opts_parse_long( c, optarg, &l );
            if( rc == 0 ) {
               opts->cache_hard_limit = l;
            }
            else {
               fprintf(stderr, "Failed to parse -L, rc = %d\n", rc );
               rc = -1;
            }
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
      SG_safe_free( optstr );
   }
   if( special_longopts != NULL ) {
      
      SG_FREE_LIST( special_longopts, free );
   }
   
   if( all_options != syndicate_options ) {
      
      for( int i = num_default_options; all_options[i].name != NULL; i++ ) {
         free( (void*)all_options[i].name );
         all_options[i].name = NULL;
      }
      SG_safe_free( all_options );
   }
   
   if( rc == 0 ) {
      if( out_optind != NULL ) {
         *out_optind = optind;
      }
      
      if( optind < argc ) {
         opts->first_nonopt_arg = argv[optind];
      }
   }
   else {
      // blow away the options 
      md_opts_free( opts );
   }
   
   return rc;
}

// parse syndicate options
int md_opts_parse( struct md_opts* opts, int argc, char** argv, int* out_optind, char const* special_opts, int (*special_opt_handler)(int, char*) ) {
   
   int rc = md_opts_parse_impl( opts, argc, argv, out_optind, special_opts, special_opt_handler, false );
   if( rc != 0 ) {
      return rc;
   }
   
   // do we need to parse stdin?
   if( opts->read_stdin ) {
      
      int new_argc = 0;
      char** new_argv = NULL;
      
      int rc = md_read_opts_from_stdin( &new_argc, &new_argv );
      if( rc != 0 ) {
         SG_error("Failed to read args from stdin, rc = %d\n", rc );
         return -ENODATA;
      }
      else {
         // clear opts 
         md_opts_free( opts );
         md_opts_default( opts );
         
         // NOTE: set this to 0, since otherwise getopt_long decides to skip the first argument
         optind = 0;
         
         // got new data.  parse it
         rc = md_opts_parse_impl( opts, new_argc, new_argv, out_optind, special_opts, special_opt_handler, true );
         
         SG_FREE_LIST( new_argv, free );
      }
   }
   
   return rc;
}

// print common usage
void md_common_usage( char const* progname ) {
   fprintf(stderr, "\
Usage of %s\n\
Common Syndicate command-line options\n\
Common required arguments:\n\
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
\n\
Common optional arguments:\n\
   -f, --foreground\n\
            Run in the foreground.\n\
            Don't detach from the controlling TTY, and don't fork.\n\
            Print all logging information to stdout.\n\
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
   -r, --storage-root STORAGE_ROOT\n\
            Cache local state at a particular location\n\
   -G, --gateway-pkey GATEWAY_PRIVATE_KEY_PATH\n\
            Path to this gateway's private key.  If no private key\n\
            is given, then it will be downloaded from the MS.\n\
   -K, --gateway-pkey-decryption-password DECRYPTION_PASSWORD\n\
            Password to decrypt the private key.\n\
   -H, --hostname HOSTNAME\n\
            Hostname to run under (default: look up via DNS).\n\
   -R, --read-stdin\n\
            If set, read all command-line options from stdin.\n\
   -d, --debug-level DEBUG_LEVEL\n\
            Debugging level.\n\
            Pass 0 (the default) for no debugging output.\n\
            Pass 1 for info messages.\n\
            Pass 2 for info and debugging messages.\n\
            Pass 3 for info, debugging, and locking messages.\n\
   -l, --cache-soft-limit LIMIT\n\
            Soft on-disk cache size limit (in bytes)\n\
   -L, --cache-hard-limit LIMIT\n\
            Hard on-disk cache size limit (in bytes)\n\
\n", progname );
}

