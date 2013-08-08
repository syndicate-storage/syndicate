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
#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/sax2/Attributes.hpp>

using namespace std;
using namespace xercesc;

#define MAP_TAG			    "Map"
#define PAIR_TAG		    "Pair"
#define CONFIG_TAG		    "Config"
#define DSN_TAG			    "DSN"
#define KEY_TAG			    "File"
#define VALUE_TAG		    "Query"
#define VOLUME_SET_TAG		    "Volumes"
#define VOLUME_TAG		    "Volume"
#define PERM_ATTR		    "perm"
#define QUERY_TYPE_ATTR		    "type"
#define MAP_REVALIDATE_ATTR	    "reval"

#define	QUERY_TYPE_SHELL		    0
#define	QUERY_TYPE_STR_SHELL		    "sell"
#define	QUERY_TYPE_BOUNDED_SQL		    1
#define	QUERY_TYPE_STR_BOUNDED_SQL	    "bounded-sql"
#define QUERY_TYPE_UNBOUNDED_SQL	    2
#define QUERY_TYPE_STR_UNBOUNDED_SQL	    "unbounded-sql"

#define	MAP_REVAL_WEEK			    'w'
#define	MAP_REVAL_DAY			    'd'
#define MAP_REVAL_HOUR			    'h'
#define	MAP_REVAL_MIN			    'm'
#define	MAP_REVAL_SEC			    's'

#define WEEK_SECS	604800
#define DAY_SECS	86400
#define HOUR_SECS	3600
#define MIN_SECS	60

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
    uint64_t reval_sec;
    uint64_t mi_time;
    void* entry;
    void (*invalidate_entry)(void*);
    void* mentry;
    void (*reversion_entry)(void*);
};

void delete_map_info(struct map_info *mi);
void delete_map_info_map(map<string, struct map_info*> *mi_map);
void update_fs_map(map<string, struct map_info*> *new_map,
		   map<string, struct map_info*> *old_map);

class MapParserHandler : public DefaultHandler {
    private:
	bool open_key;
	bool open_val;
	bool open_volume;
	char* element_buff;
	char* current_key;
	char* bounded_query;
	char* unbounded_query;
	char* shell_cmd;
	uint64_t reval_secs;
	int current_perm;
	unsigned int type;
	unsigned char* dsn_str;
	bool open_dsn;
	uint64_t current_id;
	map<string, struct map_info*>* xmlmap;
	set<string>* volumes;
	void set_time(char *tm_str);

    public:
	MapParserHandler(map<string, struct map_info*> *xmlmap,
			 set<string>* volumes);
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
	map<string, struct map_info*> *FS2SQLMap;
	set<string> *volumes;
	char *mapfile;
	unsigned char* dsn_str;
	uint64_t reval_secs;
    public:
	MapParser( char* mapfile );
	map<string, struct map_info*>* get_map( );
	set<string>* get_volumes_set();
	int parse();
	unsigned char* get_dsn();
};
#endif //_MAP_PARSER_H_

