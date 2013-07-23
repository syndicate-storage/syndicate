/* </summary>
 *   map-parser.h: Header for map-parser.cpp 
 * <summary>
 * <date>06/24/2013</date>
 * <email>wathsala@princeton.edy</email>
 * <author>Wathsala Vithanage</author>
 * </copyright>
 * All Rights Reserved
 * Copyright 2013 The Trustees of Princeton University
 * <copyright>
 * <copyright>
 * Copyright 2013 The Trustees of Princeton University
 * All Rights Reserved
 * </copyright>
 * <author>Wathsala Vithanage</author>
 * <email>wathsala@princeton.edy</email>
 * <date>06/24/2013</date>
 * <summary>
 *   map-parser.h: Header for map-parser.cpp 
 * </summary>
 */
#ifndef _MAP_PARSER_H_
#define _MAP_PARSER_H_
#include <map>
#include <iostream>
#include <string>
#include <string.h>
#include <inttypes.h>
#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/sax2/SAX2XMLReader.hpp>
#include <xercesc/sax2/XMLReaderFactory.hpp>
#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/sax2/Attributes.hpp>

using namespace std;
using namespace xercesc;

#define MAP_TAG		    "Map"
#define PAIR_TAG	    "Pair"
#define CONFIG_TAG	    "Config"
#define DSN_TAG		    "DSN"
#define KEY_TAG		    "File"
#define VALUE_TAG	    "Query"
#define PERM_ATTR	    "perm"
#define QUERY_TYPE_ATTR     "type"

#define	QUERY_TYPE_SHELL		    0
#define	QUERY_TYPE_STR_SHELL		    "sell"
#define	QUERY_TYPE_BOUNDED_SQL		    1
#define	QUERY_TYPE_STR_BOUNDED_SQL	    "bounded-sql"
#define QUERY_TYPE_UNBOUNDED_SQL	    2
#define QUERY_TYPE_STR_UNBOUNDED_SQL	    "unbounded-sql"

#define QUERY_TYPE_DEFAULT	QUERY_TYPE_BOUNDED_SQL

struct map_info {
    union {
	unsigned char* shell_command;
	struct {
	    unsigned char* query;
	    unsigned char* unbounded_query;
	};
    };
    uint64_t id;
    uint16_t file_perm;
};

class MapParserHandler : public DefaultHandler {
    private:
	bool open_key;
	bool open_val;
	char* element_buff;
	char* current_key;
	char* bounded_query;
	char* unbounded_query;
	char* shell_cmd;
	int current_perm;
	unsigned int type;
	unsigned char* dsn_str;
	bool open_dsn;
	uint64_t current_id;

	map<string, struct map_info>* xmlmap;
    public:
	MapParserHandler(map<string, struct map_info> *xmlmap);
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
	unsigned char* get_dsn();
};

class MapParser {
    private:
	map<string, struct map_info> *FS2SQLMap;
	char *mapfile;
	unsigned char* dsn_str;
    public:
	MapParser( char* mapfile );
	map<string, struct map_info>* get_map( );
	int parse();
	unsigned char* get_dsn();
};
#endif //_MAP_PARSER_H_

