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

    public:
	ODBCHandler();
	ODBCHandler(unsigned char* con_str);
	char*	    executeQuery(unsigned char* sql_query);
	static string extract_error( char *fn, SQLHANDLE handle, 
		SQLSMALLINT type);
};


#endif //_ODBC_HANDLER_H_

