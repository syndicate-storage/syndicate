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

#ifndef _ODBC_HANDLER_H_
#define _ODBC_HANDLER_H_

#include <stdlib.h>
#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>

#include <sstream>
#include <iostream>
#include <vector>

#include "AG-core.h"
#include "block-index.h"
#include "gateway-ctx.h"
#include "map-parser.h"

using namespace std;

struct invalidation_info {
	BlockIndex  *blk_index; 
	char	    *file_path;
};

void invalidate_entry(void *cls);

class ODBCHandler 
{
    private:
	//ODBC components...
	SQLHENV		    env;
	SQLHDBC		    dbc;
	static ODBCHandler& odh;
	BlockIndex	    blk_index; 

	ODBCHandler();
	ODBCHandler(unsigned char* con_str);
	ODBCHandler(ODBCHandler const&);

    public:
	static  ODBCHandler&  get_handle(unsigned char* con_str);
	void    execute_query(struct gateway_ctx *ctx, struct map_info *mi, ssize_t read_size); 
	string  get_tables();
	string  execute_query(unsigned char* query, ssize_t threashold, off_t *row_count, ssize_t *len, ssize_t *last_row_len); 
	string  get_db_info();
	string  extract_error(SQLHANDLE handle, SQLSMALLINT type);
	ssize_t	encode_results(stringstream& str_stream, char* column, 
				bool row_bound);
	void    print();
};


#endif //_ODBC_HANDLER_H_

