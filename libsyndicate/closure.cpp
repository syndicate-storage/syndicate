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

#include "closure.h"
#include "crypt.h"
#include "ms-client.h"



// duplicate a callback table 
static struct md_closure_callback_entry* md_closure_callback_table_from_prototype( struct md_closure_callback_entry* prototype ) {
   // count them...
   int num_cbs = 0;
   for( int i = 0; prototype[i].sym_name != NULL; i++ ) {
      num_cbs++;
   }
   
   struct md_closure_callback_entry* ret = CALLOC_LIST( struct md_closure_callback_entry, num_cbs + 1 );
   
   for( int i = 0; prototype[i].sym_name != NULL; i++ ) {
      ret[i].sym_name = strdup( prototype[i].sym_name );
      ret[i].sym_ptr = NULL;
   }
   
   return ret;
}


// free a callback table 
static void md_closure_callback_table_free( struct md_closure_callback_entry* callbacks ) {
   for( int i = 0; callbacks[i].sym_name != NULL; i++ ) {
      if( callbacks[i].sym_name ) {
         free( callbacks[i].sym_name );
         callbacks[i].sym_name = NULL;
      }
   }
}

// load a string as a JSON object 
static int md_parse_json_object( struct json_object** jobj_ret, char const* obj_json, size_t obj_json_len ) {
   
   char* tmp = CALLOC_LIST( char, obj_json_len + 1 );
   
   for( size_t i = 0; i < obj_json_len; i++ ) {
      tmp[i] = obj_json[i];
   }
   tmp[ obj_json_len ] = 0;
   
   // obj_json should be a valid json string that contains a single dictionary.
   struct json_tokener* tok = json_tokener_new();
   struct json_object* jobj = json_tokener_parse_ex( tok, tmp, obj_json_len );
   
   json_tokener_free( tok );
   
   if( jobj == NULL ) {
      
      errorf("Failed to parse JSON object %p '%s'\n", obj_json, tmp );
      
      free( tmp );
      
      return -EINVAL;
   }
   
   free( tmp );
   
   // should be an object
   enum json_type jtype = json_object_get_type( jobj );
   if( jtype != json_type_object ) {
      errorf("%s", "JSON config is not a JSON object\n");
      
      json_object_put( jobj );
      return -EINVAL;
   }
   
   *jobj_ret = jobj;
   return 0;
}

// load a base64-encoded string into a JSON object 
static int md_parse_b64_object( struct json_object** jobj_ret, char const* obj_b64, size_t obj_b64_len ) {
   // deserialize...
   char* obj_json = NULL;
   size_t obj_json_len = 0;
   
   int rc = 0;
   
   rc = Base64Decode( obj_b64, obj_b64_len, &obj_json, &obj_json_len );
   if( rc != 0 ) {
      errorf("Base64Decode rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_parse_json_object( jobj_ret, obj_json, obj_json_len );
   if( rc != 0 ) {
      errorf("md_parse_json_object rc = %d\n", rc );
   }
   
   free( obj_json );
   
   return rc;
}


// parse the config 
static int md_parse_closure_config( md_closure_conf_t* closure_conf, char const* closure_conf_b64, size_t closure_conf_b64_len ) {
   
   struct json_object* jobj = NULL;
   
   int rc = md_parse_b64_object( &jobj, closure_conf_b64, closure_conf_b64_len );
   if( rc != 0 ) {
      errorf("Failed to parse JSON object, rc = %d\n", rc );
      return rc;
   }
   
   // iterate through the fields 
   json_object_object_foreach( jobj, key, val ) {
      // each field needs to be a string...
      enum json_type jtype = json_object_get_type( val );
      if( jtype != json_type_string ) {
         errorf("%s is not a JSON string\n", key );
         rc = -EINVAL;
         break;
      }
      
      // get the value 
      char const* value = json_object_get_string( val );
      size_t value_len = strlen(value);         // json_object_get_string_len( val );
      
      // put it into the config 
      string key_s( key );
      string value_s( value, value_len );
      
      (*closure_conf)[ key_s ] = value_s;
   }
   
   // done with this 
   json_object_put( jobj );
   
   return rc;
}


// decrypt secrets and put the plaintext into an mlock'ed buffer 
static int md_decrypt_secrets( EVP_PKEY* gateway_pubkey, EVP_PKEY* gateway_pkey, struct json_object** jobj, char const* closure_secrets_b64, size_t closure_secrets_b64_len ) {
   // deserialize...
   char* obj_ctext = NULL;
   size_t obj_ctext_len = 0;
   
   int rc = 0;
   
   rc = Base64Decode( closure_secrets_b64, closure_secrets_b64_len, &obj_ctext, &obj_ctext_len );
   if( rc != 0 ) {
      errorf("Base64Decode rc = %d\n", rc );
      return -EINVAL;
   }
   
   // decrypt...
   char* obj_json = NULL;
   size_t obj_json_len = 0;
   
   rc = md_decrypt( gateway_pubkey, gateway_pkey, obj_ctext, obj_ctext_len, &obj_json, &obj_json_len );
   
   free( obj_ctext );
   
   if( rc != 0 ) {
      errorf("md_decrypt rc = %d\n", rc );
      return -EINVAL;
   }
   
   // parse 
   rc = md_parse_json_object( jobj, obj_json, obj_json_len );
   free( obj_json );
   
   if( rc != 0 ) {
      errorf("md_parse_json_object rc = %d\n", rc );
   }
   
   return rc;
}

// parse the secrets 
static int md_parse_closure_secrets( EVP_PKEY* gateway_pubkey, EVP_PKEY* gateway_pkey, md_closure_secrets_t* closure_secrets, char const* closure_secrets_b64, size_t closure_secrets_b64_len ) {
   
   struct json_object* jobj = NULL;
   
   int rc = md_decrypt_secrets( gateway_pubkey, gateway_pkey, &jobj, closure_secrets_b64, closure_secrets_b64_len );
   if( rc != 0 ) {
      errorf("Failed to decrypt, rc = %d\n", rc );
      return rc;
   }
   
   // iterate through the fields 
   json_object_object_foreach( jobj, key, val ) {
      // each field needs to be a string...
      enum json_type jtype = json_object_get_type( val );
      if( jtype != json_type_string ) {
         errorf("%s is not a JSON string\n", key );
         rc = -EINVAL;
         break;
      }
      
      // get the value 
      char const* encrypted_value = json_object_get_string( val );
      size_t encrypted_value_len = strlen(encrypted_value);
      
      // put it into the config 
      string key_s( key );
      string value_s( encrypted_value, encrypted_value_len );
      
      (*closure_secrets)[ key_s ] = value_s;
   }
   
   // done with this 
   json_object_put( jobj );
   
   return rc;
}

// load a string by key 
// return NULL if not found
static char const* md_load_json_string_by_key( struct json_object* obj, char const* key, size_t* _val_len ) {
   
   // look up the keyed value
   // TODO: use json_object_object_get_ex at some point, when we can move away from old libjson
   struct json_object* key_obj = NULL;
   key_obj = json_object_object_get( obj, key );
   if( key_obj == NULL ) {
      errorf("No such key '%s'\n", key );
      return NULL;
   }
   
   // verify it's a string 
   enum json_type jtype = json_object_get_type( key_obj );
   if( jtype != json_type_string ) {
      errorf("'%s' is not a string\n", key );
      return NULL;
   }
   
   char const* val = json_object_get_string( key_obj );
   
   *_val_len = strlen(val);  // json_object_get_string_len( val );
   return val;
}


// load a chunk of data directly 
static int md_parse_json_b64_string( struct json_object* toplevel_obj, char const* key, char** val, size_t* val_len ) {
   int rc = 0;
   
   // look up the keyed value
   size_t b64_len = 0;
   char const* b64 = md_load_json_string_by_key( toplevel_obj, key, &b64_len );
   
   if( b64 == NULL || b64_len == 0 ) {
      errorf("No value for '%s'\n", key);
      rc = -ENOENT;
   }
   else {
      char* tmp = NULL;
      size_t tmp_len = 0;
      
      // load it directly...
      rc = Base64Decode( b64, b64_len, &tmp, &tmp_len );
      if( rc != 0 ) {
         errorf("Failed to decode %s, rc = %d\n", key, rc );
      }
      else {
         *val = tmp;
         *val_len = tmp_len;
      }
   }
   
   return rc;
}


// parse the MS-supplied closure.
static int md_parse_closure( struct ms_client* client,
                             md_closure_conf_t* closure_conf,
                             md_closure_secrets_t* closure_secrets,
                             char** driver_bin, size_t* driver_bin_len,
                             char const* closure_text, size_t closure_text_len ) {
      
   // closure_text should be a JSON object...
   struct json_object* toplevel_obj = NULL;
   
   int rc = md_parse_json_object( &toplevel_obj, closure_text, closure_text_len );
   if( rc != 0 ) {
      errorf("md_parse_json_object rc = %d\n", rc );
      return -EINVAL;
   }
   
   // requested config?
   if( rc == 0 && closure_conf ) {
      // get the closure conf JSON 
      size_t json_b64_len = 0;
      char const* json_b64 = md_load_json_string_by_key( toplevel_obj, "config", &json_b64_len );
      
      if( json_b64 != NULL && json_b64_len != 0 ) {
         // load it
         rc = md_parse_closure_config( closure_conf, json_b64, json_b64_len );
         if( rc != 0 ) {
            errorf("md_parse_closure_config rc = %d\n", rc );
         }
      }
   }
   
   // requested secrets?
   if( rc == 0 && closure_secrets ) {
      // get the closure secrets JSON 
      size_t json_b64_len = 0;
      char const* json_b64 = md_load_json_string_by_key( toplevel_obj, "secrets", &json_b64_len );
      
      if( json_b64 != NULL || json_b64_len != 0 ) {
         // load it 
         rc = md_parse_closure_secrets( client->my_pubkey, client->my_key, closure_secrets, json_b64, json_b64_len );
         if( rc != 0 ) {
            errorf("md_parse_closure_config rc = %d\n", rc );
         }
      }
   }
   
   // requested driver?
   if( rc == 0 && driver_bin != NULL && driver_bin_len != NULL ) {
      rc = md_parse_json_b64_string( toplevel_obj, "driver", driver_bin, driver_bin_len );
   
      // not an error if not present...
      if( rc == -ENOENT ) {
         rc = 0;
      }
   }
   
   if( rc != 0 ) {
      // failed somewhere...clean up
      if( *driver_bin ) {
         free( *driver_bin );
         *driver_bin = NULL;
         *driver_bin_len = 0;
      }
      if( closure_conf ) {
         closure_conf->clear();
      }
      if( closure_secrets ) {
         closure_secrets->clear();
      }
   }
   
   json_object_put( toplevel_obj );
   
   return rc;
}

// initialize a gateway's closure.
// if gateway_specific is true, then this closure is specific to a gateway, and it can contain secrets encrypted with the gateway's public key.
// otherwise, it's volume-wide, and no secrets will be processed.
int md_closure_init( struct ms_client* client, struct md_closure* closure, struct md_closure_callback_entry* driver_prototype, char const* closure_text, size_t closure_text_len, bool gateway_specific, bool ignore_stubs ) {
   memset( closure, 0, sizeof(struct md_closure) );
   
   md_closure_conf_t* closure_conf = new md_closure_conf_t();
   md_closure_secrets_t* closure_secrets = NULL;
   
   if( gateway_specific )
      closure_secrets = new md_closure_secrets_t();
   
   char* driver_bin = NULL;
   size_t driver_bin_len = 0;
   
   int rc = md_parse_closure( client, closure_conf, closure_secrets, &driver_bin, &driver_bin_len, closure_text, closure_text_len );
   if( rc != 0 ) {
      errorf("md_parse_closure rc = %d\n", rc );
      
      delete closure_conf;
      
      if( closure_secrets )
         delete closure_secrets;
      
      return rc;
   }
   
   // intialize the closure 
   pthread_rwlock_init( &closure->reload_lock, NULL );
   
   // load the information into the closure 
   closure->closure_conf = closure_conf;
   closure->closure_secrets = closure_secrets;
   
   closure->gateway_specific = gateway_specific;
   closure->ignore_stubs = ignore_stubs;
   
   // initialize the callbacks from the prototype
   closure->callbacks = md_closure_callback_table_from_prototype( driver_prototype );
   
   // initialize the driver
   rc = md_closure_driver_reload( client->conf, closure, driver_bin, driver_bin_len );
   if( rc != 0 ) {
      errorf("md_closure_driver_reload rc = %d\n", rc );
      
      md_closure_shutdown( closure );
   }
   else {
      // ready to roll!
      closure->running = 1;
   }
   
   free( driver_bin );
   
   return rc;
}


// initialize a closure from an on-disk .so file.
// do not bother trying to load configuration or secrets
int md_closure_init_bin( struct md_syndicate_conf* conf, struct md_closure* closure, char const* so_path, struct md_closure_callback_entry* driver_prototype, bool ignore_stubs ) {
   
   memset( closure, 0, sizeof(struct md_closure));
   
   // intialize the closure 
   pthread_rwlock_init( &closure->reload_lock, NULL );
   
   closure->ignore_stubs = ignore_stubs;
   closure->callbacks = md_closure_callback_table_from_prototype( driver_prototype );
   closure->so_path = strdup(so_path);
   
   // initialize the driver
   int rc = md_closure_driver_reload( conf, closure, NULL, 0 );
   if( rc != 0 ) {
      errorf("md_closure_driver_reload rc = %d\n", rc );
      
      md_closure_shutdown( closure );
   }
   else {
      // ready to roll!
      closure->running = 1;
   }
   
   return 0;
}


// write the MS-supplied closure to a temporary file
// return the path to it on success
int md_write_driver( struct md_syndicate_conf* conf, char** _so_path_ret, char const* driver_text, size_t driver_text_len ) {
   
   char* so_path = md_fullpath( conf->data_root, MD_CLOSURE_TMPFILE_NAME, NULL );
   
   int rc = md_write_to_tmpfile( so_path, driver_text, driver_text_len, _so_path_ret );
   
   if( rc != 0 ) {
      errorf("md_write_to_tmpfile(%s) rc = %d\n", so_path, rc );
   }
   
   free( so_path );
   return rc;
}

// read and link MS-supplied closure from a file on disk
int md_load_driver( struct md_closure* closure, char const* so_path, struct md_closure_callback_entry* closure_symtable ) {
   closure->so_handle = dlopen( so_path, RTLD_LAZY );
   if ( closure->so_handle == NULL ) {
      errorf( "dlopen error = %s\n", dlerror() );
      return -ENODATA;
   }
   
   // load each symbol into its respective address
   for( int i = 0; closure_symtable[i].sym_name != NULL; i++ ) {
      
      void* sym_ptr = dlsym( closure->so_handle, closure_symtable[i].sym_name );
      closure_symtable[i].sym_ptr = sym_ptr;
      
      if( closure_symtable[i].sym_ptr == NULL ) {
         
         if( closure->ignore_stubs ) {
            errorf("WARN: unable to resolve method '%s', error = %s\n", closure_symtable[i].sym_name, dlerror() );
         }
         else {
            errorf("dlsym(%s) error = %s\n", closure_symtable[i].sym_name, dlerror());
         
            dlclose( closure->so_handle );
            closure->so_handle = NULL;
            return -ENOENT;
         }
      }
      else {
         dbprintf("Loaded '%s' at %p\n", closure_symtable[i].sym_name, closure_symtable[i].sym_ptr );
      }
   }
   
   return 0;
}

int md_closure_rlock( struct md_closure* closure ) {
   return pthread_rwlock_rdlock( &closure->reload_lock );
}


int md_closure_wlock( struct md_closure* closure ) {
   return pthread_rwlock_wrlock( &closure->reload_lock );
}


int md_closure_unlock( struct md_closure* closure ) {
   return pthread_rwlock_unlock( &closure->reload_lock );
}


// reload the given closure's driver.  Shut it down, get the new code and state, and start it back up again.
// If we fail to load or initialize the new closure, then keep the old one around.
// if NULL is passed for driver_text_len, this method reloads from the closure's so_path member
// closure must be write-locked!
int md_closure_driver_reload( struct md_syndicate_conf* conf, struct md_closure* closure, char const* driver_text, size_t driver_text_len ) {
   int rc = 0;
   
   struct md_closure new_closure;
   
   memset( &new_closure, 0, sizeof(struct md_closure) );
   
   // preserve closure-handling preferences
   new_closure.ignore_stubs = closure->ignore_stubs;
   
   bool stored_to_disk = false;
   char* new_so_path = NULL;
   
   if( driver_text != NULL ) {
      // store to disk    
      rc = md_write_driver( conf, &new_so_path, driver_text, driver_text_len );
      if( rc != 0 && rc != -ENOENT ) {
         errorf("Failed to save driver, rc = %d\n", rc);
         return -ENODATA;
      }
      
      stored_to_disk = true;
   }
   else if( closure->so_path != NULL ) {
      // reload from disk 
      new_so_path = strdup( closure->so_path );
   }
   else {
      // invalid arguments 
      rc = -EINVAL;
   }
   
   if( rc == 0 ) {
      
      // shut down the existing closure
      void* shutdown_sym = md_closure_find_callback( closure, "closure_shutdown" );
      if( shutdown_sym ) {
         
         md_closure_shutdown_func shutdown_cb = reinterpret_cast<md_closure_shutdown_func>( shutdown_sym );
         
         int closure_shutdown_rc = shutdown_cb( closure->cls );
         
         if( closure_shutdown_rc != 0 ) {
            errorf("WARN: closure->shutdown rc = %d\n", closure_shutdown_rc );
         }
      }
      
      // there's closure code to be had...
      new_closure.callbacks = md_closure_callback_table_from_prototype( closure->callbacks );
      
      rc = md_load_driver( &new_closure, new_so_path, new_closure.callbacks );
      if( rc != 0 ) {
         errorf("closure_load(%s) rc = %d\n", new_so_path, rc );
         
         if( stored_to_disk ) {
            unlink( new_so_path );
         }
         
         free( new_so_path );
      }
      else {
         // success so far... initialize it 
         void* init_sym = md_closure_find_callback( &new_closure, "closure_init" );
         if( init_sym ) {
            
            md_closure_init_func init_cb = reinterpret_cast<md_closure_init_func>( init_sym );
            
            int closure_init_rc = init_cb( &new_closure, &new_closure.cls );
            
            if( closure_init_rc != 0 ) {
               errorf("closure->init() rc = %d\n", closure_init_rc );
               rc = closure_init_rc;
            }  
         }
         
         if( rc == 0 ) {
            // successful initialization!
            
            // copy over the dynamic link handle
            void* old_so_handle = closure->so_handle;
            
            closure->so_handle = new_closure.so_handle;
            
            if( old_so_handle )
               dlclose( old_so_handle );            
            
            // load the new callbacks
            struct md_closure_callback_entry* old_callbacks = closure->callbacks;
            
            closure->callbacks = new_closure.callbacks;
            
            if( old_callbacks ) {
               md_closure_callback_table_free( old_callbacks );
               free( old_callbacks );
            }
            
            // clean up old cached closure code
            if( closure->so_path ) {
               
               if( stored_to_disk ) {
                  unlink( closure->so_path );
               }
               free( closure->so_path );
            }
            
            closure->so_path = new_so_path;
         }
      }
   }
   if( rc != 0 ) {
      // failed to load or initialize.  Clean up
      if( new_closure.callbacks ) {
         md_closure_callback_table_free( new_closure.callbacks );
         free( new_closure.callbacks );
         new_closure.callbacks = NULL;
      }
      
      if( new_closure.so_handle ) {
         dlclose( new_closure.so_handle );
         new_closure.so_handle = NULL;
      }
      
      if( new_closure.so_path ) {
         
         if( stored_to_disk ) {
            unlink( new_closure.so_path );
         }
         
         free( new_closure.so_path );
         new_closure.so_path = NULL;
      }
   }
   
   return rc;
}


// reload the closure 
int md_closure_reload( struct ms_client* client, struct md_closure* closure, char const* closure_text, size_t closure_text_len ) {
   md_closure_wlock( closure );
   
   // attempt to reload the essentials...
   md_closure_conf_t* closure_conf = new md_closure_conf_t();
   md_closure_secrets_t* closure_secrets = NULL;
   
   if( closure->gateway_specific )
      closure_secrets = new md_closure_secrets_t();
   
   char* driver_bin = NULL;
   size_t driver_bin_len = 0;
   
   int rc = md_parse_closure( client, closure_conf, closure_secrets, &driver_bin, &driver_bin_len, closure_text, closure_text_len );
   if( rc != 0 ) {
      errorf("md_parse_closure rc = %d\n", rc );
      
      delete closure_conf;
      delete closure_secrets;
      
      md_closure_unlock( closure );
      return rc;
   }
   
   // copy over the new conf and secrets...
   md_closure_conf_t* old_closure_conf = closure->closure_conf;
   md_closure_secrets_t* old_closure_secrets = closure->closure_secrets;
   
   closure->closure_conf = closure_conf;
   closure->closure_secrets = closure_secrets;
   
   // attempt to reload the driver...
   rc = md_closure_driver_reload( client->conf, closure, driver_bin, driver_bin_len );
   if( rc != 0 ) {
      errorf("md_closure_driver_reload rc = %d\n", rc );
      
      // revert 
      closure->closure_conf = old_closure_conf;
      closure->closure_secrets = old_closure_secrets;
      
      delete closure_conf;
      
      if( closure_secrets )
         delete closure_secrets;
      
   }
   else {
      // success!
      if( old_closure_conf )
         delete old_closure_conf;
      
      if( old_closure_secrets )
         delete old_closure_secrets;
   }
   
   md_closure_unlock( closure );
   
   free( driver_bin );
   
   return rc;
}


int md_closure_shutdown( struct md_closure* closure ) {
   // call the closure shutdown...
   int rc = 0;
   
   md_closure_wlock( closure );
   
   closure->running = 0;
   
   void* shutdown_sym = md_closure_find_callback( closure, "closure_shutdown" );
   if( shutdown_sym ) {
      
      md_closure_shutdown_func shutdown_cb = reinterpret_cast<md_closure_shutdown_func>( shutdown_sym );
      
      int closure_shutdown_rc = shutdown_cb( closure->cls );
      
      if( closure_shutdown_rc != 0 ) {
         errorf("WARN: closure->shutdown rc = %d\n", closure_shutdown_rc );
      }
   }
   
   if( closure->so_path ) {
      unlink( closure->so_path );
      free( closure->so_path );
      closure->so_path = NULL;
   }
   
   if( closure->so_handle ) {
      dlclose( closure->so_handle );
      closure->so_handle = NULL;
   }
   
   if( closure->callbacks ) {
      md_closure_callback_table_free( closure->callbacks );
      free( closure->callbacks );
      closure->callbacks = NULL;
   }
   
   if( closure->closure_conf ) {
      delete closure->closure_conf;
      closure->closure_conf = NULL;
   }
   
   if( closure->closure_secrets ) {
      delete closure->closure_secrets;
      closure->closure_secrets = NULL;
   }
   
   md_closure_unlock( closure );
   pthread_rwlock_destroy( &closure->reload_lock );
 
   return rc;
}

// look up a callback 
void* md_closure_find_callback( struct md_closure* closure, char const* cb_name ) {
   if( closure == NULL )
      return NULL;
   
   if( closure->running == 0 )
      return NULL;
   
   if( closure->callbacks == NULL )
      return NULL;
   
   md_closure_rlock( closure );
   
   void* ret = NULL;
   
   for( int i = 0; closure->callbacks[i].sym_name != NULL; i++ ) {
      if( strcmp( cb_name, closure->callbacks[i].sym_name ) == 0 ) {
         ret = closure->callbacks[i].sym_ptr;
      }
   }
   
   md_closure_unlock( closure );
   
   return ret;
}

// get a config value 
int md_closure_get_config( struct md_closure* closure, char const* key, char** value, size_t* len ) {
   md_closure_rlock( closure );
   
   int rc = 0;
   string key_s( key );
   
   md_closure_conf_t::iterator itr = closure->closure_conf->find( key_s );
   
   if( itr != closure->closure_conf->end() ) {
      
      size_t ret_len = itr->second.size() + 1;
      char* ret = CALLOC_LIST( char, ret_len );
      memcpy( ret, itr->second.data(), ret_len );
      
      *value = ret;
      *len = ret_len;
   }
   else {
      rc = -ENOENT;
   }
   
   md_closure_unlock( closure );
   return rc;
}

// get a secret value 
int md_closure_get_secret( struct md_closure* closure, char const* key, char** value, size_t* len ) {
   md_closure_rlock( closure );
   
   if( closure->closure_secrets == NULL ) {
      md_closure_unlock( closure );
      return -ENOENT;
   }
   
   int rc = 0;
   string key_s( key );
   
   md_closure_conf_t::iterator itr = closure->closure_secrets->find( key_s );
   
   if( itr != closure->closure_secrets->end() ) {
      
      size_t ret_len = itr->second.size() + 1;
      char* ret = CALLOC_LIST( char, ret_len );
      memcpy( ret, itr->second.data(), ret_len );
      
      *value = ret;
      *len = ret_len;
   }
   else {
      rc = -ENOENT;
   }
   
   md_closure_unlock( closure );
   return rc;
}
