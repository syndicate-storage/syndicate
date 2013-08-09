/* <copyright>
 * Copyright 2013 The Trustees of Princeton University
 * All Rights Reserved
 * </copyright>
 * <author>Wathsala Vithanage</author>
 * <email>wathsala@princeton.edy</email>
 * <date>06/24/2013</date>
 * <summary>
 *   map-parser.cpp: Parses an XML file in the form 
 *   <?xml version="1.0"?>
 *   <Map>
 *     <Pair>
 *       <File>/foo/bar</File>
 *       <Query>SQL</Query>
 *     </Pair>
 *   </Map>
 * </summary>
 */

#include "map-parser.h"

void delete_map_info(struct map_info *mi) {
    if (mi != NULL) {
	free(mi);
	mi = NULL;
    }
}

void delete_map_info_map(map<string, struct map_info*> *mi_map) {
    map<string, struct map_info*>::iterator itr;
    for (itr = mi_map->begin(); itr != mi_map->end(); itr++) {
	delete_map_info(itr->second);
    }
    delete mi_map;
}

void update_volume_set(set<string> *new_set,
		   map<string> *old_set,
		   void (*driver_disconnect_volume)(string)) {
	set<string> diff0, diff1, minter;
	set<string>::iterator itr;
	// minter = (old_set ^ new_set)
	// diff0 = (old_set - minter)
	// Delte everything in diff0 from old_set
	// diff1 = (new_set - minter)
	// Add everything in diff1 to old_set

	set_intersection(new_set->begin(), new_set->end(), 
			 old_set->begin(), old_set->end(),
			 inserter(minter, minter.begin()), 
			 new_set->value_comp());

	set_difference( old_set->begin(), old_set->end(), 
			minter.begin(), minter.end(),
			inserter(diff0, diff0.begin()),
			old_set->value_comp());

	set_difference(new_set->begin(), new_set->end(), 
			minter.begin(), minter.end(),
			inserter(diff1, diff1.begin()),
			new_set->value_comp());	
	
	// minter, the intersecion of old_set and new_set will remain in old_set.
	// Delte everything in diff0 from old_set and disconnect the AG from those volumes.
	for (itr = diff0.begin(); itr != diff0.end(); itr++) {
	    string del_vol_str= *itr;
	    old_set->erase(del_vol_str);
	    //Disconnect the AG from volume del_vol_str...
	    if (driver_disconnect_volume != NULL)
		driver_disconnect_volume(&del_vol_str);
	}
	// Add everything in diff1 to old_set
	for (itr = diff1.begin(); itr != diff1.end(); itr++) {
	    old_set->insert(*itr);
	}
}

void update_fs_map(map<string, struct map_info*> *new_map,
		   map<string, struct map_info*> *old_map,
		   void (*driver_inval_mi)(string)) {
	map<string, struct map_info*> diff0, diff1, minter;
	map<string, struct map_info*>::iterator itr;
	// minter = (old_map ^ new_map)
	// diff0 = (old_map - minter)
	// Delte everything in diff0 from old_map
	// diff1 = (new_map - minter)
	// Add everything in diff1 to old_map
	// Update values of every map_info in old_map that are also in minter

	set_intersection(new_map->begin(), new_map->end(), 
			 old_map->begin(), old_map->end(),
			 inserter(minter, minter.begin()), 
			 new_map->value_comp());

	set_difference( old_map->begin(), old_map->end(), 
			minter.begin(), minter.end(),
			inserter(diff0, diff0.begin()),
			old_map->value_comp());

	set_difference(new_map->begin(), new_map->end(), 
			minter.begin(), minter.end(),
			inserter(diff1, diff1.begin()),
			new_map->value_comp());	
	
	// Update values of every map_info in old_map that are also in minter
	for (itr = minter.begin(); itr != minter.end(); itr++) {
	    //cout<<"Updating "<<itr->second->shell_command<<endl;
	    struct map_info* umi = (*old_map)[itr->first];
	    umi->file_perm = itr->second->file_perm;
	    umi->reval_sec = itr->second->reval_sec;
	}
	// Delte everything in diff0 from old_map and invalidate those map_infos
	for (itr = diff0.begin(); itr != diff0.end(); itr++) {
	    struct map_info* emi = (*old_map)[itr->first];
	    //cout<<"Deleting "<<itr->second->shell_command<<endl;
	    old_map->erase(itr->first);
	    emi->invalidate_entry(emi);
	    if (driver_inval_mi)
		driver_inval_mi(itr->first);
	    delete_map_info(emi);
	}
	// Add everything in diff1 to old_map
	for (itr = diff1.begin(); itr != diff1.end(); itr++) {
	    //cout<<"Adding "<<itr->second->shell_command<<endl;
	    old_map->insert(pair<string, struct map_info*>(itr->first, itr->second));
	}
}

MapParserHandler::MapParserHandler(map<string, struct map_info*>* xmlmap, set<string>* volumes)
{
    this->xmlmap = xmlmap;
    this->volumes = volumes;
    open_key = false;
    open_val = false;
    open_volume = false;
    open_dsn = false;
    element_buff = NULL;
    current_key = NULL;
    bounded_query = NULL;
    unbounded_query = NULL;
    shell_cmd = NULL;
    dsn_str = NULL;
    type = QUERY_TYPE_DEFAULT;
    current_id = 0;
    reval_secs = 0;
}

void MapParserHandler::startElement(const   XMLCh* const    uri,
	const   XMLCh* const    localname,
	const   XMLCh* const    qname,
	const   Attributes&     attrs)
{
    char* tag = XMLString::transcode(localname);
    if (!strncmp(tag, KEY_TAG, strlen(tag))) {
	open_key = true;
    }
    if (!strncmp(tag, VALUE_TAG, strlen(tag))) {
	open_key = true;
    }
    if (!strncmp(tag, DSN_TAG, strlen(tag))) {
	open_dsn = true;
    }
    if (!strncmp(tag, VOLUME_TAG, strlen(tag))) {
	open_volume = true;
    }
    for (XMLSize_t i=0; i < attrs.getLength(); i++) {
	char* attr = XMLString::transcode(attrs.getLocalName(i));
	if (!strncmp(attr, PERM_ATTR, strlen(PERM_ATTR))) {
	    char* perm_str = XMLString::transcode(attrs.getValue(i));
	    if (perm_str) {
		uint16_t usr = (uint16_t)perm_str[0];
		usr <<= 6;
		uint16_t grp = (uint16_t)perm_str[1];
		grp <<= 3;
		uint16_t oth = (uint16_t)perm_str[2];
		current_perm = (usr | grp | oth);
		XMLString::release(&perm_str);
	    }
	}
	if (!strncmp(attr, QUERY_TYPE_ATTR, strlen(QUERY_TYPE_ATTR))) {
	    char* type_str = XMLString::transcode(attrs.getValue(i));
	    if (type_str == NULL || (type_str != NULL && 
			!strncmp(type_str, QUERY_TYPE_STR_BOUNDED_SQL, strlen(QUERY_TYPE_STR_BOUNDED_SQL)))) {
		type = QUERY_TYPE_BOUNDED_SQL;
	    }
	    else if (!strncmp(type_str, QUERY_TYPE_STR_SHELL, strlen(QUERY_TYPE_STR_SHELL))) {
		type = QUERY_TYPE_SHELL;
		XMLString::release(&type_str);
	    }
	    else if (!strncmp(type_str, QUERY_TYPE_STR_UNBOUNDED_SQL, strlen(QUERY_TYPE_STR_UNBOUNDED_SQL))) {
		type = QUERY_TYPE_UNBOUNDED_SQL;
		XMLString::release(&type_str);
	    }
	}
	if (!strncmp(attr, MAP_REVALIDATE_ATTR, strlen(MAP_REVALIDATE_ATTR))) {
	    char* rt_str = XMLString::transcode(attrs.getValue(i));
	    set_time(rt_str);
	}
	XMLString::release(&attr);
    }
    XMLString::release(&tag);
}

void MapParserHandler::endElement (   
	const   XMLCh *const    uri,
	const   XMLCh *const    localname,
	const   XMLCh *const    qname) 
{
    char* tag = XMLString::transcode(localname);
    if (!strncmp(tag, DSN_TAG, strlen(tag)) && open_dsn) {
	open_dsn = false;
	dsn_str = (unsigned char*)strdup(element_buff);
    }
    if (!strncmp(tag, KEY_TAG, strlen(tag)) && open_key) {
	open_key = false;
	current_key = strdup(element_buff);
    }
    if (!strncmp(tag, VALUE_TAG, strlen(tag)) && open_key 
	    && (type == QUERY_TYPE_BOUNDED_SQL)) {
	open_key = false;
	bounded_query = strdup(element_buff);
	type = QUERY_TYPE_DEFAULT;
    }
    if (!strncmp(tag, VALUE_TAG, strlen(tag)) && open_key 
	    && (type == QUERY_TYPE_UNBOUNDED_SQL)) {
	open_key = false;
	unbounded_query = strdup(element_buff);
	type = QUERY_TYPE_DEFAULT;
    }
    if (!strncmp(tag, VALUE_TAG, strlen(tag)) && open_key 
	    && (type == QUERY_TYPE_SHELL)) {
	open_key = false;
	shell_cmd = strdup(element_buff);
	type = QUERY_TYPE_DEFAULT;
    }
    if (!strncmp(tag, VOLUME_TAG, strlen(tag)) && open_volume) {
	open_volume = false;
	volumes->insert(string(element_buff));
    }
    if (!strncmp(tag, PAIR_TAG, strlen(tag))) {
	if (current_key) {
	    struct map_info* mi = (struct map_info*)malloc(sizeof(struct map_info));
	    mi->query = NULL;
	    mi->unbounded_query = NULL;
	    mi->file_perm = current_perm;
	    if (bounded_query != NULL) {
		size_t bounded_query_len = strlen(bounded_query);
		mi->query = (unsigned char*)malloc(bounded_query_len + 1);
		strncpy((char*)mi->query, bounded_query, bounded_query_len);
		mi->query[bounded_query_len] = 0;
		free(bounded_query);
		bounded_query = NULL;
	    }
	    if (unbounded_query != NULL) {
		size_t unbounded_query_len = strlen(unbounded_query);
		mi->unbounded_query = (unsigned char*)malloc(unbounded_query_len + 1);
		strncpy((char*)mi->unbounded_query, unbounded_query, unbounded_query_len);
		mi->unbounded_query[unbounded_query_len] = 0;
		free(unbounded_query);
		unbounded_query = NULL;
	    }
	    if (shell_cmd != NULL) {
		size_t shell_cmd_len = strlen(shell_cmd);
		mi->shell_command = (unsigned char*)malloc(shell_cmd_len + 1);
		strncpy((char*)mi->shell_command, shell_cmd, shell_cmd_len);
		mi->shell_command[shell_cmd_len] = 0;
		free(shell_cmd);
		shell_cmd = NULL;
	    }
	    mi->reval_sec = reval_secs;
	    reval_secs = 0;
	    mi->mi_time = 0;
	    mi->id = current_id;
	    mi->entry = NULL;
	    mi->mentry = NULL;
	    mi->invalidate_entry = NULL;
	    mi->reversion_entry = NULL;
	    current_id++;
	    (*xmlmap)[string(current_key)] =mi;
	    free(current_key);
	    current_key = NULL;
	    if (element_buff) {
		free(element_buff);
		element_buff = NULL;
	    }
	    XMLString::release(&tag);
	}
    }
    if (element_buff) {
	free(element_buff);
	element_buff = NULL;
    }
}

void MapParserHandler::characters (   
	const   XMLCh *const    chars,
	const   unsigned int    length)
{
    char* element = XMLString::transcode(chars);
    if (!element)
	return;
    if (open_key || open_dsn || open_volume) {
	if (!element_buff) { 
	    element_buff = (char*)malloc(length+1);
	    element_buff[length] = 0;
	    strncpy(element_buff, element, length);
	}
	else { 
	    int current_len = strlen(element_buff); 
	    element_buff = (char*)realloc
		(element_buff, current_len + length + 1);
	    element_buff[current_len + length] = 0;
	    strncat(element_buff + current_len, element, length);
	}
	XMLString::release(&element);
    }
    /*if (open_dsn) {
	if (!element_buff) { 
	    element_buff = (char*)malloc(length+1);
	    element_buff[length] = 0;
	    strncpy(element_buff, element, length);
	}
	else { 
	    int current_len = strlen(element_buff); 
	    element_buff = (char*)realloc
		(element_buff, current_len + length + 1);
	    element_buff[current_len + length] = 0;
	    strncat(element_buff + current_len, element, length);
	}
	XMLString::release(&element);
    }*/
}

void MapParserHandler::fatalError(const SAXParseException& exception)
{
    char* message = XMLString::transcode(exception.getMessage());
    XMLString::release(&message);
}   

unsigned char* MapParserHandler::get_dsn()
{
    return dsn_str;
}

void MapParserHandler::set_time(char *tm_str) {
    //Parse the string, any character can be in between different time units.
    stringstream strstream;
    int tm_str_len = strlen(tm_str);
    int count = 0;
    uint64_t secs = 0;
    for (; count < tm_str_len; count++) {
	if (tm_str[count] < 58 && tm_str[count] > 47) {
	    strstream << tm_str[count];
	}
	else if (tm_str[count] == MAP_REVAL_WEEK) {
	    string week_str = strstream.str();
	    secs += (uint64_t)atol(week_str.c_str()) * WEEK_SECS;
	    strstream.clear();
	    strstream.str(string());
	}
	else if (tm_str[count] == MAP_REVAL_DAY) {
	    string day_str = strstream.str();
	    secs += (uint64_t)atol(day_str.c_str()) * DAY_SECS;
	    strstream.clear();
	    strstream.str(string());
	}
	else if (tm_str[count] == MAP_REVAL_HOUR) {
	    string hour_str = strstream.str();
	    secs += (uint64_t)atol(hour_str.c_str()) * HOUR_SECS;
	    strstream.clear();
	    strstream.str(string());
	}
	else if (tm_str[count] == MAP_REVAL_MIN) {
	    string min_str = strstream.str();
	    secs += (uint64_t)atol(min_str.c_str()) * MIN_SECS;
	    strstream.clear();
	    strstream.str(string());
	}
	else if (tm_str[count] == MAP_REVAL_SEC) {
	    string sec_str = strstream.str();
	    secs += (uint64_t)atol(sec_str.c_str());
	    strstream.clear();
	    strstream.str(string());
	}
	else {
	    //Ignore...
	}
    }
    reval_secs = secs;
}

MapParser::MapParser( char* mapfile)
{
    this->mapfile = mapfile;
}

int MapParser::parse()
{
    try {
	XMLPlatformUtils::Initialize();
    }
    catch (const XMLException& toCatch) {
	char* message = XMLString::transcode(toCatch.getMessage());
	XMLString::release(&message);
	return 1;
    }
    SAX2XMLReader* parser = XMLReaderFactory::createXMLReader();
    parser->setFeature(XMLUni::fgSAX2CoreValidation, true);
    parser->setFeature(XMLUni::fgSAX2CoreNameSpaces, true);   // optional
    FS2SQLMap = new map<string, struct map_info*>;
    volumes = new set<string>; 
    MapParserHandler* mph = new MapParserHandler(this->FS2SQLMap, this->volumes);
    parser->setContentHandler(mph);
    parser->setErrorHandler(mph);
    try {
	parser->parse(mapfile);
    }
    catch (const XMLException& toCatch) {
	char* message = XMLString::transcode(toCatch.getMessage());
	XMLString::release(&message);
	return -1;
    }
    catch (const SAXParseException& toCatch) {
	char* message = XMLString::transcode(toCatch.getMessage());
	XMLString::release(&message);
	return -1;
    }
    catch (...) {
	return -1;
    }
    dsn_str = mph->get_dsn();
    delete parser;
    delete mph;
    return 0;
}
	
map<string, struct map_info*>* MapParser::get_map()
{
    return FS2SQLMap;
}

unsigned char* MapParser::get_dsn()
{
    return dsn_str;
}


set<string>* MapParser::get_volume_set() 
{
    return volumes;
}

