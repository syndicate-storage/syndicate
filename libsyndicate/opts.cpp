/*
   Copyright 2015 The Trustees of Princeton University

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

#include "libsyndicate/private/opts.h"
#include "libsyndicate/opts.h"

// new opt struct 
struct md_opts* md_opts_new( int count ) {
   return SG_CALLOC( struct md_opts, count );
}

// fill opts with defaults
int md_opts_default( struct md_opts* opts ) {
   memset( opts, 0, sizeof(struct md_opts) );
   
   opts->config_file = SG_strdup_or_null( SG_DEFAULT_CONFIG_PATH );
   if( opts->config_file == NULL ) {
      return -ENOMEM;
   }
   
   return 0;
}

// get the client flag 
bool md_opts_get_client( struct md_opts* opts ) {
   return opts->client;
}

// get the ignore-driver disposition 
bool md_opts_get_ignore_driver( struct md_opts* opts ) {
   return opts->ignore_driver;
}

uint64_t md_opts_get_gateway_type( struct md_opts* opts ) {
   return opts->gateway_type;
}

char const* md_opts_get_config_file( struct md_opts* opts ) {
   return opts->config_file;
}

// set the "client" override 
void md_opts_set_client( struct md_opts* opts, bool client ) {
   opts->client = client;
}

// set the "ignore_driver" override 
void md_opts_set_ignore_driver( struct md_opts* opts, bool ignore_driver ) {
   opts->ignore_driver = ignore_driver;
}

// set the "gateway_type" field 
void md_opts_set_gateway_type( struct md_opts* opts, uint64_t type ) {
   opts->gateway_type = type;
}

// set path to config file 
void md_opts_set_config_file( struct md_opts* opts, char* config_filepath ) {
   opts->config_file = config_filepath;
}

// set username 
void md_opts_set_username( struct md_opts* opts, char* username ) {
   opts->username = username;
}

// set volume name 
void md_opts_set_volume_name( struct md_opts* opts, char* volume_name ) {
   opts->volume_name = volume_name;
}

// set gateway name 
void md_opts_set_gateway_name( struct md_opts* opts, char* gateway_name ) {
   opts->gateway_name = gateway_name;
}

// set MS url 
void md_opts_set_ms_url( struct md_opts* opts, char* ms_url ) {
   opts->ms_url = ms_url;
}

// toggle running in the foreground 
void md_opts_set_foreground( struct md_opts* opts, bool foreground ) {
   opts->foreground = foreground;
}

// set driver options 
void md_opts_set_driver_config( struct md_opts* opts, char const* driver_exec_str, char const** driver_roles, size_t num_driver_roles ) {
   opts->driver_exec_str = driver_exec_str;
   opts->driver_roles = driver_roles;
   opts->num_driver_roles = num_driver_roles;
}

// parse a long 
int md_opts_parse_long( int c, char* opt, long* result ) {
   char* tmp = NULL;
   int rc = 0;
   
   errno = 0;
   *result = strtol( opt, &tmp, 10 );
   
   if( tmp == opt ) {
      fprintf(stderr, "Invalid value '%s' for option -%c\n", opt, c );
      rc = -1;
   }
   return rc;
}


// free the opts structure 
// always succeeds 
int md_opts_free( struct md_opts* opts ) {
   
   char EOL = 0;
   
   char* to_free[] = {
      opts->config_file,
      opts->username,
      opts->volume_name,
      opts->ms_url,
      opts->gateway_name,
      &EOL
   };
   
   for( int i = 0; to_free[i] != &EOL; i++ ) {
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
// return 1 if the caller wanted help
int md_opts_parse_impl( struct md_opts* opts, int argc, char** argv, int* out_optind, char const* special_opts, int (*special_opt_handler)(int, char*) ) {
   
   static struct option syndicate_options[] = {
      {"config-file",     required_argument,   0, 'c'},
      {"volume-name",     required_argument,   0, 'v'},
      {"username",        required_argument,   0, 'u'},
      {"gateway",         required_argument,   0, 'g'},
      {"MS",              required_argument,   0, 'm'},
      {"debug-level",     required_argument,   0, 'd'},
      {"foreground",      no_argument,         0, 'f'},
      {"help",            no_argument,         0, 'h'},
      {0, 0, 0, 0}
   };

   struct option* all_options = NULL;
   char** special_longopts = NULL;
   int special_longopts_idx = 0;
   
   int rc = 0;
   int opt_index = 0;
   int c = 0;
   
   char const* default_optstr = "c:v:u:g:m:d:fh";
   
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
      
      if( c == -1 ) {
         break;
      }
      
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
         
         case 'h': {
            
            rc = 1;
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
   }
   else {
      // blow away the options 
      md_opts_free( opts );
   }
   
   return rc;
}

// parse syndicate options
int md_opts_parse( struct md_opts* opts, int argc, char** argv, int* out_optind, char const* special_opts, int (*special_opt_handler)(int, char*) ) {
  
   optind = 1;

   int rc = md_opts_parse_impl( opts, argc, argv, out_optind, special_opts, special_opt_handler );
   if( rc != 0 ) {
      return rc;
   }
   
   optind = 0;
   
   return rc;
}

// print common usage
void md_common_usage() {
   fprintf(stderr, "\
Syndicate required arguments:\n\
   -u, --username USERNAME\n\
            Syndicate account username\n\
   -v, --volume VOLUME_NAME\n\
            Name of the Volume you are going to access\n\
   -g, --gateway GATEWAY_NAME\n\
            Name of this gateway\n\
\n\
Syndicate optional arguments:\n\
   -m, --MS MS_URL\n\
            URL to your Metadata Service.\n\
            Loaded from the Syndicate config file if not given.\n\
   -c, --config-file CONFIG_FILE_PATH\n\
            Path to the config file to use.\n\
            Default is '%s'\n\
   -f, --foreground\n\
            Run in the foreground.\n\
            Don't detach from the controlling TTY, and don't fork.\n\
            Print all logging information to stdout.\n\
   -d, --debug-level DEBUG_LEVEL\n\
            Debugging level.\n\
            Pass 0 (the default) for no debugging output.\n\
            Pass 1 for info messages.\n\
            Pass 2 for info and debugging messages.\n\
            Pass 3 for info, debugging, and locking messages.\n\
\n", SG_DEFAULT_CONFIG_PATH );
}

