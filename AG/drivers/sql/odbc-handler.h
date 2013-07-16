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

#include <block-index.h>
#include <gateway-ctx.h>

using namespace std;

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
	void    execute_query(struct gateway_ctx<ODBCHandler> *ctx, ssize_t read_size, ssize_t block_size); 
	string  get_tables();
	string  execute_query(unsigned char* query, ssize_t threashold, off_t *row_count, ssize_t *len, ssize_t *last_row_len); 
	string  get_db_info();
	string  extract_error(SQLHANDLE handle, SQLSMALLINT type);
	ssize_t	encode_results(stringstream& str_stream, char* column, 
				bool row_bound);
	void    print();
};


#endif //_ODBC_HANDLER_H_

