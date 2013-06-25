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
#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/sax2/SAX2XMLReader.hpp>
#include <xercesc/sax2/XMLReaderFactory.hpp>
#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/util/XMLString.hpp>
using namespace std;
using namespace xercesc;
#define MAP_TAG     "Map"
#define PAIR_TAG    "Pair"
#define KEY_TAG     "Key"
#define VALUE_TAG   "Value"
class MapParserHandler : public DefaultHandler {
    private:
	bool open_key;
	bool open_val;
	char* element_buff;
	char* current_key;
	char* current_val;
	map<string, string>* xmlmap;
    public:
	MapParserHandler(map<string, string> *xmlmap);
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
};

class MapParser {
    private:
	map<string, string> *FS2SQLMap;
	char *mapfile;
    public:
	MapParser( char* mapfile );
	map<string, string>* get_map( );
	int parse();
};
#endif //_MAP_PARSER_H_

