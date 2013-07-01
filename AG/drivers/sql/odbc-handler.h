#ifndef _ODBC_HANDLER_H_
#define _ODBC_HANDLER_H_

#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>

#include <sstream>
#include <iostream>

//#include "libsyndicate.h"

using namespace std;

class ODBCHandler 
{
    private:
	//ODBC components...
	SQLHENV env;
	SQLHDBC dbc;
	static ODBCHandler& odh;

	ODBCHandler();
	ODBCHandler(unsigned char* con_str);
	ODBCHandler(ODBCHandler const&);

    public:
	static  ODBCHandler&  get_handle(unsigned char* con_str);
	char*   execute_query(unsigned char* sql_query);
	string  get_tables();
	string  get_db_info();
	string  extract_error(SQLHANDLE handle, SQLSMALLINT type);
	//void operator=(ODBCHandler const&);
	void    print();
};


#endif //_ODBC_HANDLER_H_

