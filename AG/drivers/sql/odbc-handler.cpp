#include "odbc-handler.h"

ODBCHandler::ODBCHandler()
{
}

ODBCHandler::ODBCHandler(unsigned char *con_str) 
{
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
    } else {
	ODBC_error = extract_error(dbc, SQL_HANDLE_DBC);
	if (&dbc)
	    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	if (&env)
	    SQLFreeHandle(SQL_HANDLE_ENV, env);
	cout<<ODBC_error<<endl;
    }
}

ODBCHandler&  ODBCHandler::get_handle(unsigned char* con_str)
{
    static ODBCHandler instance = ODBCHandler(con_str);
    return instance;
}

string ODBCHandler::get_db_info()
{
    stringstream    info_stream;
    string	    ODBC_error; 
    SQLCHAR	    dbms_name[256], dbms_ver[256];
    SQLUINTEGER	    getdata_support;
    SQLUSMALLINT    max_concur_act;

    SQLGetInfo(dbc, SQL_DBMS_NAME, (SQLPOINTER)dbms_name,
	    sizeof(dbms_name), NULL);
    SQLGetInfo(dbc, SQL_DBMS_VER, (SQLPOINTER)dbms_ver,
	    sizeof(dbms_ver), NULL);
    SQLGetInfo(dbc, SQL_GETDATA_EXTENSIONS, (SQLPOINTER)&getdata_support,
	    0, 0);
    SQLGetInfo(dbc, SQL_MAX_CONCURRENT_ACTIVITIES, &max_concur_act, 0, 0);

    info_stream<<"DBMS Name: "<<dbms_name<<endl;
    info_stream<<"DBMS Version: "<<dbms_ver<<endl;
    if (max_concur_act == 0) {
	info_stream<<"Maximum concurrent activities: Unlimited or Undefined."<<endl;
    } else {
	info_stream<<"Maximum concurrent activities: "<<max_concur_act<<"."<<endl;
    }
    if (getdata_support & SQL_GD_ANY_ORDER)
	info_stream<<"Column read order: Any order."<<endl;
    else
	info_stream<<"Column read order: Must be retreived in order."<<endl;
    if (getdata_support & SQL_GD_ANY_COLUMN)
	info_stream<<"Column bound: Can retrieve columns before last bound one.";
    else
	info_stream<<"Column bound: Must be retrieved after last bound one.";
    return info_stream.str();
}

string ODBCHandler::get_tables()
{
    SQLHSTMT	    stmt;
    SQLRETURN	    ret; 
    SQLSMALLINT	    nr_columns;
    stringstream    tbl_list;
    string	    ODBC_error; 
    bool	    list_start = false;

    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, (unsigned char*)"TABLE",
	    SQL_NTS);
    SQLNumResultCols(stmt, &nr_columns);
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
	SQLUSMALLINT i;
	tbl_list<<"{";
	for (i = 1; i <= nr_columns; i++) {
	    SQLLEN indicator;
	    char buf[512];
	    ret = SQLGetData(stmt, i, SQL_C_CHAR,
		    buf, sizeof(buf), &indicator);
	    if (SQL_SUCCEEDED(ret)) {
		if (indicator != SQL_NULL_DATA) {//strcpy(buf, "NULL");
		    if (list_start)
			tbl_list<<",";
		    else
			list_start = true;
		    tbl_list<<buf;
		}
	    }
	}
	tbl_list<<"}";
    }
    return tbl_list.str();
}

string ODBCHandler::execute_query(unsigned char* sql_query, ssize_t read_size, 
				    off_t byte_offset, off_t block_offset, ssize_t block_size) 
{
    SQLHSTMT	    stmt;
    SQLRETURN	    ret; 
    SQLSMALLINT	    nr_columns;
    string	    ODBC_error; 
    stringstream    result_str;
    stringstream    shadow_result_str;
    string	    ret_str;
    ssize_t	    partial_size = 0;
    bool	    start_bound = false;
    bool	    end_bound = false;
    bool	    row_bound = false;
    off_t	    discard_offset = 
		    ( block_offset * block_size ) + byte_offset; 
    off_t	    end_offset = discard_offset + read_size;
    off_t	    row_count = 0;
    
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    ret = SQLPrepare(stmt, sql_query , SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) { 
	ODBC_error = extract_error(stmt, SQL_HANDLE_STMT);
	cout<<ODBC_error<<endl;
    }
    ret = SQLExecute(stmt);
    if (!SQL_SUCCEEDED(ret)) { 
	ODBC_error = extract_error(stmt, SQL_HANDLE_STMT);
	cout<<ODBC_error<<endl;
    }

    SQLNumResultCols(stmt, &nr_columns);
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
	SQLUSMALLINT i;
	if (end_bound)
	    break;
	row_count++;
	for (i = 1; i <= nr_columns; i++) {
	    SQLLEN indicator;
	    char buf[512];
	    ret = SQLGetData(stmt, i, SQL_C_CHAR,
		    buf, sizeof(buf), &indicator);
	    if (i == nr_columns)
		row_bound = true;
	    if (SQL_SUCCEEDED(ret)) {
		if (indicator == SQL_NULL_DATA) 
		    strcpy(buf, "NULL");
		if (!start_bound)
		    encode_results(shadow_result_str, buf, row_bound);
		else
		    encode_results(result_str, buf, row_bound);
	    }
	}
	row_bound = false;
	if (!start_bound)
	    partial_size = shadow_result_str.str().size();
	else
	    partial_size = result_str.str().size();
	if ( discard_offset < partial_size &&
		!start_bound ) {
	    const string tmp_buff_str =  shadow_result_str.str();
	    const char* tmp_buff_cstr = tmp_buff_str.c_str();
	    const char* buff_boundary = tmp_buff_cstr + discard_offset; 
	    result_str<<buff_boundary;
	    start_bound = true;
	}
	else if (end_offset <= partial_size) {
	    ret_str = result_str.str();
	    ret_str.erase( ret_str.begin() + read_size, ret_str.end());
	    end_bound = true;
	}
    }
    if (end_offset > partial_size) {
	ret_str = result_str.str();
    }
    return ret_str;
}

void ODBCHandler::encode_results(stringstream& str_stream, char* column, bool row_bound)
{
    if (column)
	str_stream<<column;
    if (!row_bound)
	str_stream<<",";
    else
	str_stream<<"\n";
}

string ODBCHandler::extract_error(SQLHANDLE handle, SQLSMALLINT type)
{
    SQLINTEGER		i = 0;
    SQLINTEGER		native;
    SQLCHAR		state[ 7 ];
    SQLCHAR		text[1024];
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
/*
void ODBCHandler::operator=(ODBCHandler const& x) 
{
    if (init)
	x = odh;
}*/

void ODBCHandler::print() 
{
    cout<<&dbc<<endl;
}
