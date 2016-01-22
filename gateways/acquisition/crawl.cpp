/*
   Copyright 2016 The Trustees of Princeton University

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

/*
 Crawl format is a four-line stanza:
 * command string
 * metadata string
 * path string
 * terminator string
 
 command string is a 1-character, newline-terminated line:
 * C for 'create'
 * U for 'update'
 * D for 'delete'
 
 mode string is a two-field newline-terminated line:
 * "$type $mode $size", where
    * $type is 'D' for directory or 'F' for file
    * $mode is the octal mode
    * $size is the size of the file
 
 path string is a well-formed absolute path; accepted as-is minus the newline 

 terminator string is a null character, and a newline.
*/

#include "crawl.h"
#include "core.h"

#define AG_CRAWL_CMD_CREATE  'C'
#define AG_CRAWL_CMD_UPDATE  'U'
#define AG_CRAWL_CMD_DELETE  'D'

// indexes into a single stanza
#define AG_CRAWL_STANZA_CMD 0
#define AG_CRAWL_STANZA_MD  1
#define AG_CRAWL_STANZA_PATH 2
#define AG_CRAWL_STANZA_TERM 3

// copy a mode-string and path-string into an md_entry
// only the type, mode, and name will be set.  Everything else will be left as-is
// return 0 on success
// return -EINVAL on bad input
// return -ENOMEM on OOM 
static int AG_crawl_parse_metadata( char const* md_linebuf, char const* path_linebuf, struct md_entry* data ) {

   int rc = 0;

   char type = 0;
   mode_t mode = 0;
   uint64_t size = 0;

   rc = sscanf( md_linebuf, "%c %o %" PRIu64 "\n", &type, &mode, &size );
   if( rc != 2 ) {
      SG_error("Invalid mode string '%s'\n", md_linebuf );
      return -EINVAL;
   }

   if( type != 'D' && type != 'F' ) {
      SG_error("Invalid mode string type '%c'\n", type );
      return -EINVAL;
   }
   
   data->type = (type == 'D' ? MD_ENTRY_DIR : MD_ENTRY_FILE);
   data->mode = mode & 0666;    // force read-only for now
   data->size = size;
   data->name = md_basename( path_linebuf, NULL );

   if( data->name == NULL ) {
      return -ENOMEM;
   }

   return 0;
}


// obtain the crawl command from a crawl command string 
// return 0 on success, and set *cmd
// return -EINVAL on bad input
static int AG_crawl_parse_command( char const* cmd_linebuf, int* cmd ) {

   int rc = 0;
   char cmd_type = 0;
   
   rc = sscanf( cmd_linebuf, "%c\n", &cmd_type );
   if( rc != 1 ) {
      SG_error("Invalid command string '%s'\n", cmd_linebuf );
      return -EINVAL;
   }

   if( cmd_type != AG_CRAWL_CMD_CREATE && cmd_type != AG_CRAWL_CMD_UPDATE && cmd_type != AG_CRAWL_CMD_DELETE ) {
      SG_error("Invalid command '%c'\n", cmd_type );
      return -EINVAL;
   }

   *cmd = cmd_type;
   return 0;
}


// read a stanza from a FILE*
// return 0 on success, and populate **lines (which must have at least 3 slots)
// return -EINVAL if we did not find the terminating string
// return -ENOMEM on OOM
// Recovery will be attempted by reading ahead to the next terminating string (if one is not found immediately)
static int AG_crawl_read_stanza( FILE* input, char** lines ) {

   int rc = 0;
   size_t len = 0;
   ssize_t nr = 0;
   int cnt = 0;
   char* linebuf = NULL;

   for( int i = 0; i < 4; i++ ) {
      lines[i] = NULL;
   }

   for( int i = 0; i < 3; i++ ) {
    
      linebuf = NULL;
      len = 0;
      nr = getline( &linebuf, &len, input );
      if( nr < 0 ) {
         
         rc = -errno;
         SG_error("getline rc = %d\n", rc);
         break;
      }

      // is this a premature terminator line?
      if( nr == 2 && linebuf[0] == '\0' ) {
         SG_error("early terminator at stanza line %d\n", i );
         rc = -EINVAL;
         break;
      }

      lines[i] = linebuf;
   }

   // read terminator 
   linebuf = NULL;
   len = 0;
   nr = getline( &linebuf, &len, input );
   if( nr < 0 ) {

      rc = -errno;
      SG_error("getline rc = %d\n", rc );
   }

   if( nr != 2 || (nr >= 1 && linebuf[0] != '\0' )) {

      // not a terminator 
      SG_error("Missing terminator at end of stanza (got '%s')\n", linebuf);
      SG_safe_free( linebuf );
      
      // try to consume until we find a terminator 
      while( 1 ) {

         linebuf = NULL;
         len = 0;
         nr = getline( &linebuf, &len, input );
         if( nr == 2 && linebuf[0] == '\0' ) {
            // termiated!
            SG_error("Terminator found %d lines after end of stanza\n", cnt );
            rc = -EINVAL;
            SG_safe_free( linebuf );
            break;
         }

         SG_safe_free( linebuf );
         cnt++;
      }
   }

   if( rc != 0 ) {

      // clean up 
      for( int i = 0; i < 4; i++ ) {
         if( lines[i] != NULL ) {
            SG_safe_free( lines[i] );
            lines[i] = NULL;
         }
      }
   }

   return rc;
}


// given a stanza, parse it into an md_entry 
// return 0 on success
// return -EINVAL if the stanza is bad
// return -ENOMEM on OOM 
static int AG_crawl_parse_stanza( char** lines, int* cmd, char** path, struct md_entry* entry ) {

    int rc = 0;

    rc = AG_crawl_parse_command( lines[AG_CRAWL_STANZA_CMD], cmd );
    if( rc != 0 ) {
       SG_error("Failed to parse command line, rc = %d\n", rc);
       return -EINVAL;
    }

    *path = SG_strdup_or_null( lines[AG_CRAWL_STANZA_PATH] );
    if( *path == NULL ) {
       return -ENOMEM;
    }

    rc = AG_crawl_parse_metadata( lines[AG_CRAWL_STANZA_MD], lines[AG_CRAWL_STANZA_PATH], entry );
    if( rc != 0 ) {
       SG_error("Failed to parse metadata line, rc = %d\n", rc);
       SG_safe_free( *path );
       *path = NULL;
       return -EINVAL;
    }

    return 0;
}


// handle one crawl command
// return 0 on success
// return -ENOMEM on OOM
// return -ENOENT if we requested an update or delete on a non-existant entry
// return -EEXIST if we tried to create an entry that already existed
// return -EACCES on permission error
// return -EPERM on operation error
// return -EREMOTEIO on failure to communicate with the MS
int AG_crawl_process( struct AG_state* core, int cmd, char const* path, struct md_entry* ent ) {

   int rc = 0;
   struct SG_gateway* gateway = AG_state_gateway( core );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = AG_state_ug( core );
   struct timespec now;
   struct SG_client_WRITE_data* update = NULL;
   UG_handle_t* h = NULL;

   // enforce these...
   ent->coordinator = SG_gateway_id( gateway );
   ent->volume = ms_client_get_volume_id( ms );

   switch( cmd ) {
      case AG_CRAWL_CMD_CREATE: {

         ent->file_id = (uint64_t)md_random64();

         // try to create or mkdir 
         if( ent->type == MD_ENTRY_FILE ) {

             clock_gettime( CLOCK_REALTIME, &now );

             ent->manifest_mtime_sec = now.tv_sec;
             ent->manifest_mtime_nsec = now.tv_nsec;
             ent->mtime_sec = now.tv_sec;
             ent->mtime_nsec = now.tv_nsec;
             ent->ctime_sec = now.tv_sec;
             ent->ctime_nsec = now.tv_nsec;

             h = UG_publish( ug, path, ent, &rc );
             if( h == NULL ) {

                SG_error("UG_publish_file(%s) rc = %d\n", path, rc );
                if( rc != -ENOMEM && rc != -EPERM && rc != -EACCES && rc != -EEXIST ) {
                    rc = -EREMOTEIO;
                }
                break;
             }

             rc = UG_close( ug, h );
             if( rc != 0 ) {

                SG_error("UG_close(%s) rc = %d\n", path, rc );
                break;
             }

             h = NULL;
             break;
         }

         else {

            rc = UG_mkdir( ug, path, ent->mode );
            if( rc != 0 ) {
               
               SG_error("UG_mkdir(%s) rc = %d\n", path, rc );
               if( rc != -ENOMEM && rc != -EPERM && rc != -EACCES && rc != -EEXIST ) {
                   rc = -EREMOTEIO;
               }
               break;
            }
         }

         break;
      }
   
      case AG_CRAWL_CMD_UPDATE: {
      
          update = SG_client_WRITE_data_new();
          if( update == NULL ) {
             rc = -ENOMEM;
             break;
          }


          clock_gettime( CLOCK_REALTIME, &now );

          SG_client_WRITE_data_set_mtime( update, &now );
          SG_client_WRITE_data_set_mode( update, ent->mode );
          SG_client_WRITE_data_set_owner_id( update, ent->owner );

          rc = UG_update( ug, path, update );

          SG_safe_free( update );

          if( rc != 0 ) {

             SG_error("UG_update(%s) rc = %d\n", path, rc );
             if( rc != -ENOMEM && rc != -EPERM && rc != -EACCES && rc != -ENOENT ) {
                rc = -EREMOTEIO;
             }
             break;
          }

          break;
      }
   
      case AG_CRAWL_CMD_DELETE: {

         if( ent->type == MD_ENTRY_FILE ) {
            
            rc = UG_unlink( ug, path );
            if( rc != 0 ) {

               SG_error("UG_unlink(%s) rc = %d\n", path, rc );
               if( rc != -ENOMEM && rc != -EPERM && rc != -EACCES && rc != -ENOENT ) {
                  rc = -EREMOTEIO;
               }
               break;
            }
         }
         
         else {

            rc = UG_rmdir( ug, path );
            if( rc != 0 ) {

               SG_error("UG_rmdir(%s) rc = %d\n", path, rc );
               if( rc != -ENOMEM && rc != -EPERM && rc != -EACCES && rc != -ENOENT ) {
                  rc = -EREMOTEIO;
               }
               break;
            }
         }

         break;
      }

      default: {
         SG_error("Unknown command type '%c'\n", cmd );
         rc = -EINVAL;
         break;
      }
   }

   return rc;
}


// get the next metadata entry and command from the crawler, process it, and reply the result
// return 0 on success, and fill in *block
// return -ENOMEM on OOM 
// return -ENOENT if the block does not exist
// return -EIO if the driver did not fulfill the request (driver error)
// return -ENODATA if we couldn't request the data, for whatever reason (no processes free)
int AG_crawl_next_entry( struct AG_state* core ) {
 
   int rc = 0;
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct SG_gateway* gateway = AG_state_gateway( core );
   struct UG_state* ug_core = AG_state_ug( core );
   int cmd = 0;
   char* path = NULL;
   struct md_entry ent;
   SG_messages::DriverRequest driver_req;
   char* lines[4] = {
      NULL,
      NULL,
      NULL,
      NULL
   };

   memset( &ent, 0, sizeof(struct md_entry) );
   
   UG_state_rlock( ug_core );
   AG_state_rlock( core );  

   // find a crawler
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "crawl" );
   if( group != NULL ) {
      
      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
      
         // nothing running
         rc = -ENODATA;
         goto AG_crawl_next_entry_finish;
      }
    
      // get the stanza 
      rc = AG_crawl_read_stanza( SG_proc_stdout_f( proc ), lines );
      if( rc < 0 ) {
         SG_error("AG_crawl_read_stanza rc = %d\n", rc );
         rc = -EIO;
         goto AG_crawl_next_entry_finish;
      }

      // parse stanza
      rc = AG_crawl_parse_stanza( lines, &cmd, &path, &ent );
      if( rc < 0 ) {
         SG_error("AG_crawl_parse_stanza rc = %d\n", rc );
         rc = -EIO;
         goto AG_crawl_next_entry_finish;
      }

      // consume the stanza 
      rc = AG_crawl_process( core, cmd, path, &ent );
      if( rc < 0 ) {
         SG_error("AG_crawl_process(%s) rc = %d\n", path, rc );
         goto AG_crawl_next_entry_finish;
      }
   }
   else {
      
      // no way to do work--no process group 
      rc = -ENODATA;
   }
   
AG_crawl_next_entry_finish:

   if( group != NULL && proc != NULL ) {
   
      // reply the status 
      rc = SG_proc_write_int64( SG_proc_stdin( proc ), rc );
      if( rc < 0 ) {
         SG_error("SG_proc_write_int64(%d) rc = %d\n", SG_proc_stdin(proc), rc );
      }

      SG_proc_group_release( group, proc );
   }

   AG_state_unlock( core );
   UG_state_unlock( ug_core );

   SG_safe_free( path );
   md_entry_free( &ent );
   
   return rc;
}


