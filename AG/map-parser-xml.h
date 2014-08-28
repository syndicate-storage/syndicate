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

#ifndef _AG_MAP_PARSER_XML_H_
#define _AG_MAP_PARSER_XML_H_

#include <map>
#include <set>
#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <string.h>
#include <inttypes.h>
#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/sax2/SAX2XMLReader.hpp>
#include <xercesc/sax2/XMLReaderFactory.hpp>
#include <xercesc/framework/MemBufInputSource.hpp>
#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/sax2/Attributes.hpp>

#include "AG.h"
#include "map-info.h"

using namespace std;
using namespace xercesc;

#define AG_TAG_MAP_NAME                 "Map"
#define AG_TAG_PAIR_NAME                "Pair"
#define AG_TAG_CONFIG_NAME              "Config"
#define AG_TAG_FILE_NAME                "File"
#define AG_TAG_DIR_NAME                 "Dir"
#define AG_TAG_QUERY_NAME               "Query"

#define AG_TAG_MAP_ID       1
#define AG_TAG_PAIR_ID      2
#define AG_TAG_CONFIG_ID    3
#define AG_TAG_FILE_ID      4
#define AG_TAG_DIR_ID       5
#define AG_TAG_QUERY_ID     6

#define AG_ATTR_PERM_NAME               "perm"
#define AG_ATTR_QUERYTYPE_NAME          "type"
#define AG_ATTR_REVAL_NAME              "reval"

#define AG_REVAL_WEEK          'w'
#define AG_REVAL_DAY           'd'
#define AG_REVAL_HOUR          'h'
#define AG_REVAL_MIN           'm'
#define AG_REVAL_SEC           's'

#define AG_WEEK_SECS       604800
#define AG_DAY_SECS        86400
#define AG_HOUR_SECS       3600
#define AG_MIN_SECS        60
#define AG_YEAR_SECS       (52 * WEEK_SECS)

// prototypes
struct AG_state;

class AG_XMLMapParserHandler : public DefaultHandler {

public:
   
   // are we in a Config tag?
   bool in_config;
   
   // current tag's element text
   char* element_buf;
   size_t element_buf_len;
   
   // current query type attr
   bool has_query_type;
   char* query_type;
   
   // current file revalidation attr
   bool has_reval_secs;
   uint64_t reval_secs;
   
   // current file permissions attr
   bool has_file_perm;
   mode_t file_perm;
   
   // current file path 
   bool has_file_path;
   int file_path_type;
   char* file_path;
   
   // current query string 
   bool has_query_string;
   char* query_string;
   
   // current configuration tag 
   char* config_tag;
   
   // map we're building up 
   AG_fs_map_t* xmlmap;
   
   // configuration section 
   AG_config_t* config;
   
   // reference to the AG's running state 
   struct AG_state* state;
   
   static int64_t parse_time(char *tm_str);
   
   void reset_element_parse_state( int tag_id );
   
   // dispatch table for parsing attributes 
   struct attr_dispatch_table_t {
      int tag_id;
      char const* attr_name;
      void (*attr_handler)( AG_XMLMapParserHandler* handler, char* value );
   };
   
   // bind attr name and type
   struct attr_tag_str_to_type_t {
      char const* name;
      int type;
   };
   
   // route attr to parser
   static struct attr_dispatch_table_t attr_dispatch_table[];
   
   // associated type ID with type name
   static struct attr_tag_str_to_type_t attr_tag_str_to_type[];
   
   // attr dispatcher 
   void consume_attr( int tag_id, char* attr_name, char* attr_value );
   
   // tag string to id 
   static int tag_type_id_from_str( char const* tag_str );
   
   // sanity checkers 
   int pair_check_missing_fields(const XMLCh* const uri);
   
   AG_XMLMapParserHandler(struct AG_state* state);
   ~AG_XMLMapParserHandler();
   
   void startElement(
            const   XMLCh* const    uri,
            const   XMLCh* const    localname,
            const   XMLCh* const    qname,
            const   Attributes&     attrs
            );
   void endElement (   
            const   XMLCh *const    uri,
            const   XMLCh *const    localname,
            const   XMLCh *const    qname    
            );
   void characters (   
            const   XMLCh *const    chars,
            const   unsigned int    length   
            );   
   
   void fatalError(const SAXParseException&);
   
   AG_fs_map_t* extract_map() {
      AG_fs_map_t* map = this->xmlmap;
      this->xmlmap = NULL;
      return map;
   }
   
   AG_config_t* extract_config() {
      AG_config_t* ret = this->config;
      this->config = NULL;
      return ret;
   }
};

// public C method 
int AG_parse_spec( struct AG_state* state, char const* spec_file_text, size_t spec_file_text_len, AG_fs_map_t** new_map, AG_config_t** new_config );


#endif //_AG_MAP_PARSER_XML_H_

