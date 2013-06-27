#include "odbc-handler.h"

ODBCHandler::ODBCHandler()
{
}


ODBCHandler::ODBCHandler(unsigned char *con_str) 
{
    //SQLCHAR outstr[1024];
    //SQLSMALLINT outstrlen;
    string ODBC_error; 
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, (SQLCHAR*)con_str, SQL_NTS,
	    NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (SQL_SUCCEEDED(ret)) {
	if (ret == SQL_SUCCESS_WITH_INFO) {
	    ODBC_error = extract_error(dbc, SQL_HANDLE_DBC);
	}
	SQLDisconnect(dbc);
    } else {
	ODBC_error = extract_error(dbc, SQL_HANDLE_DBC);
	if (&dbc)
	    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	if (&env)
	    SQLFreeHandle(SQL_HANDLE_ENV, env);
	//errorf("Failed to connect data source: %s : %s\n", con_str, 
	//	ODBC_error);
    }
}

char* ODBCHandler::executeQuery(unsigned char* sql_query) 
{
    SQLHSTMT stmt;
    SQLRETURN ret; 
    //SQLCHAR outstr[1024];
    //SQLSMALLINT outstrlen;
    SQLSMALLINT nr_columns;
    int nr_rows = 0;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR*)"TABLE", SQL_NTS);
    SQLNumResultCols(stmt, &nr_columns);
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
	SQLUSMALLINT i;
	cout<<"Row "<<nr_rows++<<endl;;
	/* Loop through the nr_columns */
	for (i = 1; i <= nr_columns; i++) {
	    SQLLEN indicator;
	    char buf[512];
	    /* retrieve column data as a string */
	    ret = SQLGetData(stmt, i, SQL_C_CHAR,
		    buf, sizeof(buf), &indicator);
	    if (SQL_SUCCEEDED(ret)) {
		/* Handle null nr_columns */
		if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
		cout<<"  Column "<<i<<":"<<buf<<endl;
	    }
	}
    }
    return NULL;
}

string ODBCHandler::extract_error(SQLHANDLE handle, SQLSMALLINT type)
{
    SQLINTEGER		i = 0;
    SQLINTEGER		native;
    SQLCHAR		state[ 7 ];
    SQLCHAR		text[256];
    SQLSMALLINT		len;
    SQLRETURN		ret;
    stringstream	error_stream;


    do
    {
	ret = SQLGetDiagRec(type, handle, ++i, state, &native, text,
		sizeof(text), &len );
	if (SQL_SUCCEEDED(ret))
	    error_stream<<state<<":"<<i<<":"<<native<<":"<<text;

    }
    while( ret == SQL_SUCCESS );
    return error_stream.str();
}


