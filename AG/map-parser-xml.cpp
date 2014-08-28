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

#include "map-parser-xml.h"
#include "core.h"
#include "driver.h"

// prototypes
void attr_handle_perm( AG_XMLMapParserHandler* handler, char* perm_str );
void attr_handle_query_type( AG_XMLMapParserHandler* handler, char* query_type_str );
void attr_handle_reval( AG_XMLMapParserHandler* handler, char* rt_str );

struct AG_XMLMapParserHandler::attr_tag_str_to_type_t AG_XMLMapParserHandler::attr_tag_str_to_type[] =  {
   { AG_TAG_MAP_NAME,    AG_TAG_MAP_ID  },
   { AG_TAG_PAIR_NAME,   AG_TAG_PAIR_ID },
   { AG_TAG_CONFIG_NAME, AG_TAG_CONFIG_ID},
   { AG_TAG_FILE_NAME,   AG_TAG_FILE_ID },
   { AG_TAG_DIR_NAME,    AG_TAG_DIR_ID },
   { AG_TAG_QUERY_NAME,  AG_TAG_QUERY_ID},
   { NULL,               0 }
};

struct AG_XMLMapParserHandler::attr_dispatch_table_t AG_XMLMapParserHandler::attr_dispatch_table[] = {
   { AG_TAG_FILE_ID,                 AG_ATTR_PERM_NAME,              attr_handle_perm },
   { AG_TAG_DIR_ID,                  AG_ATTR_PERM_NAME,              attr_handle_perm },
   { AG_TAG_QUERY_ID,                AG_ATTR_QUERYTYPE_NAME,         attr_handle_query_type },
   { AG_TAG_PAIR_ID,                 AG_ATTR_REVAL_NAME,             attr_handle_reval },
   { -1,                             NULL,                           NULL }
};
   

AG_XMLMapParserHandler::AG_XMLMapParserHandler( struct AG_state* state )
{
   this->query_type = NULL;
   this->element_buf = NULL;
   
   this->xmlmap = new AG_fs_map_t();
   this->config = new AG_config_t();
   
   this->in_config = false;
   this->has_query_type = false;
   this->has_reval_secs = false;
   this->has_file_path = false;
   this->has_query_string = false;
   
   this->query_type = NULL;
   this->query_string = NULL;
   this->file_path = NULL;
   this->config_tag = NULL;
   this->element_buf = NULL;
   
   this->element_buf_len = 0;
   
   this->reset_element_parse_state( -1 );
   
   this->state = state;
}

AG_XMLMapParserHandler::~AG_XMLMapParserHandler() {
   
   this->reset_element_parse_state( -1 );
   
   if( this->config != NULL ) {
      delete this->config;
      this->config = NULL;
   }
   
   // free the map 
   if( this->xmlmap != NULL ) {
      AG_fs_map_free( this->xmlmap );
      delete this->xmlmap;
      this->xmlmap = NULL;
   }
}
   

// reset parser state by freeing and zeroing some of the fields, based on which tag we're dealing with.
// pass 0 for tag_id to reset only state that's common across all tags.
// pass -1 for tag_id to reset all state
// pass >0 to reset state for a specific tag.  Note that state for tags contained within the given tag will be reset as well.
void AG_XMLMapParserHandler::reset_element_parse_state( int tag_id ) {
   
   bool all = (tag_id < 0);
   
   // Pair contains File and Query, so if we're feeing Pair, free File and Query as well 
   
   if( all || tag_id == AG_TAG_PAIR_ID ) {
      
      // holds reval 
      this->reval_secs = 0;
      this->has_reval_secs = false;
   }
   
   if( all || (tag_id == AG_TAG_QUERY_ID || tag_id == AG_TAG_PAIR_ID) ) {
      
      // holds query string and query type
      this->has_query_type = false;
      this->has_query_string = false;
      
      if( this->query_type != NULL ) {
         free( this->query_type );
         this->query_type = NULL;
      }
      
      if( this->query_string != NULL ) {
         free( this->query_string );
         this->query_string = NULL;
      }
   }
   
   if( all || (tag_id == AG_TAG_FILE_ID || tag_id == AG_TAG_DIR_ID || tag_id == AG_TAG_PAIR_ID) ) {
      
      // holds perm and path 
      this->has_file_path = false;
      this->has_file_perm = false;
      this->file_path_type = 0;
      this->file_perm = 0;
      
      if( this->file_path != NULL ) {
         free( this->file_path );
         this->file_path = NULL;
      }
   }
   
   if( all || tag_id == AG_TAG_CONFIG_ID ) {
         
      // holds config and config element
      if( this->config_tag != NULL ) {
         free( this->config_tag );
         this->config_tag = NULL;
      }
   }
   
   if( this->element_buf != NULL ) {
      free( this->element_buf );
      this->element_buf = NULL;
   }
   
   this->element_buf_len = 0;
}

// extract the type from a tag 
// return -ENOENT if not found
int AG_XMLMapParserHandler::tag_type_id_from_str( char const* tag_str ) {
   
   
   int ret = -ENOENT;
   
   for( int i = 0; AG_XMLMapParserHandler::attr_tag_str_to_type[i].name != NULL; i++ ) {
      if( strcmp( tag_str, AG_XMLMapParserHandler::attr_tag_str_to_type[i].name ) == 0 ) {
         ret = AG_XMLMapParserHandler::attr_tag_str_to_type[i].type;
         break;
      }
   }
   
   return ret;
}

// parse permissions attribute 
static int AG_attr_parse_perm( char const* perm_str, mode_t* perm ) {
   if( strlen(perm_str) < 3 ) {
      errorf("Invalid permissions string '%s'\n", perm_str);
      return -EINVAL;
   }
   
   char* tmp = NULL;
   
   mode_t mode = (mode_t)strtol( perm_str, &tmp, 8 );
   
   if( mode == 0 || tmp == NULL ) {
      errorf("Invalid permissions string '%s'\n", perm_str );
      return -EINVAL;
   }
      
   // sanity check--there should be no writability 
   if( (mode & 0222) != 0 ) {
      errorf("Invalid permissions string '%s'; entries must be read-only\n", perm_str);
      return -EINVAL;
   }
   
   *perm = mode;
   
   return 0;
}


// verify that the attr is appropriate for the tag.
// NOTE: this requires that attr was allocated by the caller
static inline int AG_assert_valid_tag_attribute( int tag_id, int required_tag_id, char* attr_name, char* attr_value ) {
   // only valid for file paths 
   if( tag_id != required_tag_id ) {
      return -EINVAL;
   }
   
   return 0;
}


void attr_handle_perm( AG_XMLMapParserHandler* handler, char* perm_str ) {

   if( perm_str == NULL ) {
      return;
   }
   
   mode_t perm = 0;
   
   int rc = AG_attr_parse_perm( perm_str, &perm );
   if( rc != 0 ) {
      errorf("AG_attr_parse_perm(%s) rc = %d\n", perm_str, rc );
      
      throw SAXException("AG_attr_parse_perm");
   }
   
   handler->has_file_perm = true;
   handler->file_perm = perm;
   
   dbprintf("Parsed attr '%s' = '%s' as %o\n", AG_ATTR_PERM_NAME, perm_str, handler->file_perm );
}


void attr_handle_query_type( AG_XMLMapParserHandler* handler, char* query_type_str ) {
   
   if( query_type_str == NULL ) {
      return;
   }
   
   if( handler->query_type != NULL ) {
      errorf("WARN: overriding query type '%s' with '%s'\n", handler->query_type, query_type_str );
      free( handler->query_type );
   }  
   
   handler->has_query_type = true;
   handler->query_type = strdup( query_type_str );
   
   dbprintf("Parsed attr '%s' = '%s'\n", AG_ATTR_PERM_NAME, handler->query_type );
}


void attr_handle_reval( AG_XMLMapParserHandler* handler, char* rt_str ) {

   if( rt_str == NULL ) {
      return;
   }
   
   // revalidate time 
   int64_t rt_secs = AG_XMLMapParserHandler::parse_time(rt_str);
   
   if( rt_secs < 0 ) {
      errorf("AG_XMLMapParserHandler::parse_time( '%s' ) rc = %" PRId64 "\n", rt_str, rt_secs);
      
      throw SAXException( "Unable to parse revalidation time" );
   }
   
   handler->has_reval_secs = true;
   handler->reval_secs = rt_secs;
   
   dbprintf("Parsed attr '%s' = '%s' as %" PRIu64 "\n", AG_ATTR_REVAL_NAME, rt_str, rt_secs );
}


// NOTE: attr_name and attr_value must be dynamically allocated.  This method will free them if it can consume them.
void AG_XMLMapParserHandler::consume_attr( int tag_id, char* attr_name, char* attr_value ) {
   
   bool handled = false;
   
   for( int i = 0; AG_XMLMapParserHandler::attr_dispatch_table[i].tag_id >= 0; i++ ) {
      
      AG_XMLMapParserHandler::attr_dispatch_table_t* attr_dispatch = &AG_XMLMapParserHandler::attr_dispatch_table[i];
      
      // is this the attr?
      if( strcmp( attr_dispatch->attr_name, attr_name ) == 0 ) {
         
         // is this the appropriate tag?
         int rc = AG_assert_valid_tag_attribute( tag_id, attr_dispatch->tag_id, attr_name, attr_value );
         
         if( rc == 0 ) {
            
            // consume 
            (*attr_dispatch->attr_handler)( this, attr_value );
            
            // handled!
            handled = true;
            break;
         }
      }
   }
   
   // free 
   XMLString::release( &attr_name );
   
   if( attr_value ) {
      XMLString::release( &attr_value );
   }
   
   if( !handled ) {
      errorf("ERR: could not consume attr '%s' = '%s' for tag ID %d\n", attr_name, attr_value, tag_id );
      throw SAXException("Invalid attribute in tag");
   }
}


// callback to start parsing an XML element.  this sets up parser state for this element
void AG_XMLMapParserHandler::startElement(const   XMLCh* const    uri,
                                          const   XMLCh* const    localname,
                                          const   XMLCh* const    qname,
                                          const   Attributes&     attrs)
{
   
   char* tag = XMLString::transcode(localname);
   char* qname_str = XMLString::transcode( qname );
   
   dbprintf("start element '%s' at '%s'\n", tag, qname_str );
   
   XMLString::release(&qname_str);
   
   // if we're in a Config tag, then just consume each sub-tag within it.  No need to worry about attributes.
   if( this->in_config ) {
      
      // sanity check--this cannot be another "Config" element.  Nested Config is not supported.
      int tag_id = AG_XMLMapParserHandler::tag_type_id_from_str( tag );
      if( tag_id == AG_TAG_CONFIG_ID ) {
         
         errorf("Nesting '%s' elements is not supported\n", AG_TAG_CONFIG_NAME );
         
         XMLString::release(&tag);
      
         throw SAXException( "Invalid nesting" );
      }
      
      // otherwise, just store this current tag name
      if( this->config_tag ) {
         free( this->config_tag );
      }
      
      this->config_tag = strdup(tag);
      
      
      dbprintf("Config tag '%s'\n", this->config_tag );
   }
   else {
      // Otherwise, make sure this is a valid tag, and then get its attributes
      int tag_id = AG_XMLMapParserHandler::tag_type_id_from_str( tag );
      
      if( tag_id < 0 ) {
         
         errorf("Unrecognized tag '%s'\n", tag );
         XMLString::release(&tag);
         
         throw SAXException( "Unrecognized tag" );
      }
      
      // parse attrs for this tag
      for( XMLSize_t i = 0; i < attrs.getLength(); i++ ) {

         // get current attr
         char* attr = XMLString::transcode(attrs.getLocalName(i));
         char* value = XMLString::transcode(attrs.getValue(i));

         dbprintf("%s '%s' = '%s'\n", tag, attr, value );
         
         this->consume_attr( tag_id, attr, value );
      }
      
      // update parser state
      if( tag_id == AG_TAG_CONFIG_ID ) {
         this->in_config = true;
      }
   }
   
   // done with this tag 
   XMLString::release(&tag);
}

int AG_XMLMapParserHandler::pair_check_missing_fields(const XMLCh* const qname) {
   
   char* qname_str = XMLString::transcode( qname );
   
   if( !this->has_file_path ) {
      errorf("ERR: element '%s' has no '%s' or '%s' tag\n", qname_str, AG_TAG_FILE_NAME, AG_TAG_DIR_NAME );
      
      XMLString::release( &qname_str );
      return -EINVAL;
   }
   
   if( !this->has_file_perm ) {
      errorf("ERR: element '%s' has no '%s' attribute\n", qname_str, AG_ATTR_PERM_NAME );
      
      XMLString::release( &qname_str );
      return -EINVAL;
   }
   
   if( !this->has_query_type ) {
      errorf("ERR: element '%s' has no '%s' attribute\n", qname_str, AG_ATTR_QUERYTYPE_NAME );
      
      XMLString::release( &qname_str );
      return -EINVAL;
   }
   
   if( !this->has_reval_secs ) {
      errorf("ERR: element '%s' has no '%s' attribute\n", qname_str, AG_ATTR_REVAL_NAME );
      
      XMLString::release( &qname_str );
      return -EINVAL;
   }
   
   if( !this->has_query_string ) {
      errorf("ERR: element '%s' has no '%s' tag\n", qname_str, AG_TAG_QUERY_NAME );
      
      XMLString::release( &qname_str );
      return -EINVAL;
   }
   
   XMLString::release( &qname_str );
   return 0;
}


// sanitize the element buffer--strip leading whitespace
static char* sanitize_element_buffer( char const* ro_element_buf ) {
   
   char* element_buf = strdup( ro_element_buf );
   char* tok_save;
   
   char const* delim = " \n\r\t";
   
   char* tok = strtok_r( element_buf, delim, &tok_save );
   
   if( tok == NULL ) {
      // nothing to match 
      free( element_buf );
      return NULL;
   }
   else {
      char* ret = strdup(tok);
      free( element_buf );
      return ret;
   }
}

// callback to stop parsing an xml element.
// generate a map_info at this point.
void AG_XMLMapParserHandler::endElement( const   XMLCh *const    uri,
                                         const   XMLCh *const    localname,
                                         const   XMLCh *const    qname) 
{
   // make sure this is a valid tag
   char* tag = XMLString::transcode(localname);
   char* qname_str = XMLString::transcode( qname );
   dbprintf("end element '%s' at '%s'\n", tag, qname_str );
   
   int rc = 0;
   int tag_id = AG_XMLMapParserHandler::tag_type_id_from_str( tag );
   
   if( tag_id < 0 ) {
      
      if( !this->in_config ) {
         // not in Config (which can have arbitrary internal tags), so this isn't recognized
         errorf("Unrecognized tag '%s'\n", tag );
         XMLString::release(&tag);
         
         throw SAXException( "Unrecognized tag" );
      }
      else {
         
         // this is a config element 
         char* value = sanitize_element_buffer( this->element_buf );
         
         dbprintf("Config element '%s'\n", value );
         
         // update config 
         (*this->config)[ string(this->config_tag) ] = string(value);
         
         free( value );
      }
      
      // prepare for next tag 
      this->reset_element_parse_state( 0 );
   }
   
   else if( tag_id == AG_TAG_FILE_ID || tag_id == AG_TAG_DIR_ID ) {
      
      // consume the file path 
      if( this->element_buf != NULL ) {
         
         if( this->has_file_path && this->file_path != NULL ) {
            // dup 
            errorf("WARN: ignoring duplicate %s in %s\n", AG_TAG_FILE_NAME, qname_str );
            
         }
         else {
            
            // this is a file path 
            this->file_path = sanitize_element_buffer( this->element_buf );
            md_sanitize_path( this->file_path );
            
            this->has_file_path = true;
            
            dbprintf("File path element '%s'\n", this->file_path );
            
            if( tag_id == AG_TAG_FILE_ID ) {
               this->file_path_type = MD_ENTRY_FILE;
            }
            else {
               this->file_path_type = MD_ENTRY_DIR;
            }
         }
      }
      else {
         errorf("WARN: missing file path for %s\n", qname_str );
      }
      
      // prepare for next tag 
      this->reset_element_parse_state( 0 );
   }
   
   else if( tag_id == AG_TAG_QUERY_ID ) {
      
      // consume the query string 
      if( this->element_buf != NULL ) {
         
         if( this->has_query_string && this->query_string != NULL ) {
            // dup 
            errorf("WARN: ignoring duplicate %s in %s\n", AG_TAG_QUERY_NAME, qname_str );
         }
         else {
            
            // this is a query string 
            this->query_string = sanitize_element_buffer( this->element_buf );
            this->has_query_string = true;
            
            dbprintf("Query string element '%s'\n", this->query_string );
         }
      }
      else {
         errorf("WARN: missing query string for %s\n", qname_str );
      }
      
      // prepare for next tag 
      this->reset_element_parse_state( 0 );
   }
      
   else if( tag_id == AG_TAG_PAIR_ID ) {
      
      // got a whole pair.  Can consume everything and make an AG_map_info
      // sanity check 
      rc = this->pair_check_missing_fields( qname );
      if( rc < 0 ) {
         errorf("ERR: could not process '%s'\n", qname_str);
      }
      
      else {
         
         // convert path to string 
         string path_s( this->file_path );
         
         // look for dups 
         if( this->xmlmap->count( path_s ) > 0 ) {
            errorf("WARN: ignoring duplicate entry for %s in %s\n", this->file_path, qname_str );
         }
         else {
            
            struct AG_driver* driver = AG_lookup_driver( this->state->drivers, this->query_type );
            if( driver == NULL ) {
               // can't load this 
               errorf("ERR: No driver loaded for %s (query type '%s')\n", this->file_path, this->query_type );
            }
            
            else {
               // we have enough information to make a map_info.
               struct AG_map_info* mi = CALLOC_LIST( struct AG_map_info, 1 );
               
               // populate the map info 
               AG_map_info_init( mi, this->file_path_type, this->file_path, this->file_perm, this->reval_secs, driver );
               
               // deadline for refreshing
               struct timespec now;
               clock_gettime( CLOCK_MONOTONIC, &now );
               
               mi->refresh_deadline = now.tv_sec + mi->reval_sec;
               
               // put this into our map 
               (*this->xmlmap)[ path_s ] = mi;
            }
         }
      }
      
      // reset parser state for the entire Pair
      this->reset_element_parse_state( AG_TAG_PAIR_ID );
   }
   else if( tag_id == AG_TAG_CONFIG_ID ) {
      // out of config 
      this->in_config = false;
   }
   else if( tag_id != AG_TAG_MAP_ID ) {
      errorf("ERR: unhandled tag %s\n", tag );
   }
   
   // done with this tag
   XMLString::release(&tag);
   XMLString::release(&qname_str);
}


// accumulate the element's text
void AG_XMLMapParserHandler::characters( const   XMLCh *const    chars,
                                         const   unsigned int    length)
{
    char* element = XMLString::transcode(chars);
    if ( element == NULL ) {
      return;
    }
    
    off_t offset = 0;
    
    // store null-terminated element text 
    if( this->element_buf == NULL ) {
       this->element_buf = CALLOC_LIST( char, length + 1 );
       this->element_buf_len = length;
    }
    else {
       char* tmp = (char*)realloc( this->element_buf, this->element_buf_len + length + 1 );
       
       if( tmp == NULL ) {
          errorf("%s", "Out of memory");
          exit(1);
       }
       
       memset( tmp + this->element_buf_len, 0, length + 1 );
       
       this->element_buf = tmp;
       
       offset = this->element_buf_len;
       this->element_buf_len += length;
    }
    
    strncpy( this->element_buf + offset, element, length );
    
    XMLString::release(&element);
}


// callback to catch XML parsing errors
void AG_XMLMapParserHandler::fatalError(const SAXParseException& exception)
{
    char* message = XMLString::transcode(exception.getMessage());
    XMLString::release(&message);
}


// parse a time string into the number of seconds
int64_t AG_XMLMapParserHandler::parse_time(char *tm_str) {
   
   // Parse the string, any character can be in between different time units.
   int64_t secs = 0;
   char* tmp = NULL;
   char* tm_tok = NULL;
   char* tm_tok_save = NULL;
   char const* delim = " \r\n\t";
   char* tm_str_buf = strdup(tm_str);
   
   struct _time_unit_pair {
      int unit_id;
      int64_t time;
   };
   
   static struct _time_unit_pair time_units[] = {
      { AG_REVAL_WEEK, AG_WEEK_SECS },
      { AG_REVAL_DAY,  AG_DAY_SECS  },
      { AG_REVAL_HOUR, AG_HOUR_SECS },
      { AG_REVAL_MIN,  AG_MIN_SECS  },
      { AG_REVAL_SEC,  1L           },
      { 0,             0            }
   };
   
   tm_tok = tm_str_buf;
   
   while( true ) {
      // tokenize by whitespace
      char* tok = strtok_r( tm_tok, delim, &tm_tok_save );
      tm_tok = NULL;
      
      // done?
      if( tok == NULL || strlen(tok) == 0 ) {
         break;
      }
      
      // parse token 
      bool handled = false;
      
      // extrat time unit (should be the last character)
      int time_unit = tok[ strlen(tok) - 1 ];
      tok[ strlen(tok) - 1 ] = 0;
      
      // find time value 
      for( int j = 0; time_units[j].unit_id != 0; j++ ) {
         
         if( time_unit == time_units[j].unit_id ) {
            
            tmp = NULL;
            
            int64_t n = (int64_t)strtoll( tok, &tmp, 10 );
            if( tmp == NULL ) {
               errorf("Invalid time value '%s'\n", tok );
               
               free( tm_str_buf );
               return -EINVAL;
            }
            else {
               secs += n * time_units[j].time;
            }
            
            handled = true;
            break;
         }
      }
      
      if( !handled ) {
         errorf("Unrecognized time unit '%c' in '%s'\n", time_unit, tm_str);
         
         free( tm_str_buf );
         return -EINVAL;
      }
   }

   free( tm_str_buf );
   
   return secs;
}

// parse a spec_file (as text) into a new map and config
int AG_parse_spec( struct AG_state* state, char const* spec_file_text, size_t spec_file_text_len, AG_fs_map_t** new_map, AG_config_t** new_config )
{
   try {
      XMLPlatformUtils::Initialize();
   }
   catch (const XMLException& toCatch) {
      char* message = XMLString::transcode(toCatch.getMessage());
      
      errorf("FATAL: %s\n", message);
      
      XMLString::release(&message);
      return 1;
   }
   SAX2XMLReader* parser = XMLReaderFactory::createXMLReader();
   
   // activate parsers
   parser->setFeature(XMLUni::fgSAX2CoreValidation, true);
   parser->setFeature(XMLUni::fgSAX2CoreNameSpaces, true);   // optional
   
   AG_XMLMapParserHandler* mph = new AG_XMLMapParserHandler( state );
   
   parser->setContentHandler(mph);
   parser->setErrorHandler(mph);
   
   MemBufInputSource xml_buf( (const XMLByte*)spec_file_text, spec_file_text_len, "XML spec file (in RAM)" );
   
   try {
      parser->parse(xml_buf);
   }
   catch (const XMLException& toCatch) {
      char* message = XMLString::transcode(toCatch.getMessage());
      
      errorf("FATAL: %s\n", message);
      
      XMLString::release(&message);
         
      delete parser;
      delete mph;
      return -1;
   }
   catch (const SAXParseException& toCatch) {
      char* message = XMLString::transcode(toCatch.getMessage());
      
      errorf("FATAL: %s\n", message);
      
      XMLString::release(&message);
         
      delete parser;
      delete mph;
      return -1;
   }
   catch (...) {
      return -1;
   }
   
   // extract the map and config 
   *new_map = mph->extract_map();
   *new_config = mph->extract_config();
   
   delete parser;
   delete mph;
   return 0;
}
