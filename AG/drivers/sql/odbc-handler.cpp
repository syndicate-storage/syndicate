#include <odbc-handler.h>

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

string ODBCHandler::execute_query(struct gateway_ctx<ODBCHandler> *ctx, ssize_t read_size, ssize_t block_size) 
{
    SQLHSTMT		stmt;
    SQLRETURN		ret; 
    SQLSMALLINT		nr_columns;
    string		ODBC_error; 
    stringstream	result_str;
    stringstream	shadow_result_str;
    string		ret_str;
    ssize_t		partial_size = 0;
    bool		start_bound = false;
    bool		end_bound = false;
    bool		row_bound = false;
    ssize_t		row_size = 0;
    off_t		discard_offset = 
			    ( ctx->block_id * block_size ) + ctx->data_offset; 
    off_t		end_offset = discard_offset + read_size;
    off_t		row_count = 0;
    const block_index_entry	*blkie = NULL;
    block_index_entry		*new_blkie = NULL;
    bool		create_blkie = false;
    ssize_t		query_len = 0;
    unsigned char*	query = NULL;

    //Get the block_index_entry for ctx->block_id of the file_name.
    //If it is there, skip rows. Else get the last block_index_entry
    //for the file. If its block id is ctx->block_id - 1 then process 
    //from there and update block index for ctx->block_id block. 
    //Else go through last block to ctx->block_id and update block index
    //for each block.

    blkie = blk_index.get_block(ctx->file_path, ctx->block_id);
    if (blkie != NULL) {
	if (ctx->sql_query_bounded == NULL)
	    return NULL;
	//I'm feeling lucky...
	query_len = strlen((const char*)ctx->sql_query_bounded) + 21;
	query = (unsigned char*)malloc(query_len);
	memset(query, 0, query_len);
	snprintf((char*)query, query_len, (const char*)ctx->sql_query_bounded, 
	     (blkie->end_row - blkie->start_row), blkie->start_row);
    }
    else {
	blkie = blk_index.get_last_block(ctx->file_path);
	if (blkie != NULL) {

	}
	else {
	    //ctx->block_id == 0, we haven't built the block index yet.
	    if (ctx->sql_query_unbounded == NULL)
		return NULL;
	    query_len = strlen((const char*)ctx->sql_query_unbounded) + 11;
	    query = (unsigned char*)malloc(query_len);
	    memset(query, 0, query_len);
	    snprintf((char*)query, query_len, (const char*)ctx->sql_query_unbounded, 0);
	    create_blkie = true;
	}
    }
    
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    ret = SQLPrepare(stmt, query , SQL_NTS);
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
	row_size = 0;
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
		    row_size += encode_results(shadow_result_str, buf, row_bound);
		else
		    row_size += encode_results(result_str, buf, row_bound);
	    }
	}
	row_bound = false;
	if (!start_bound)
	    partial_size = shadow_result_str.str().size();
	else {
	    partial_size = result_str.str().size();
	    ctx->row_offset++;
	}
	if ( discard_offset < partial_size &&
		!start_bound ) {
	    const string tmp_buff_str =  shadow_result_str.str();
	    const char* tmp_buff_cstr = tmp_buff_str.c_str();
	    const char* buff_boundary = tmp_buff_cstr + discard_offset; 
	    result_str<<buff_boundary;
	    start_bound = true;
	    if (create_blkie) {
		if (ctx->blkie == NULL) {
		    new_blkie = blk_index.alloc_block_index_entry();
		    new_blkie->start_row = ctx->row_offset;
		    new_blkie->start_byte_offset = discard_offset;
		    ctx->blkie = new_blkie; 
		}
		else {
		    new_blkie = ctx->blkie;
		}
	    }
	}
	else if (end_offset <= partial_size) {
	    ret_str = result_str.str();
	    ret_str.erase( ret_str.begin() + read_size, ret_str.end());
	    end_bound = true;
	    if (create_blkie) {
		if (partial_size >= block_size) {
		    new_blkie->end_row += ctx->row_offset;
		    new_blkie->end_byte_offset = row_size - (partial_size - end_offset);
		    blk_index.update_block_index(ctx->file_path, ctx->block_id, 
			    new_blkie);
		}
	    }
	}
    }
    if (end_offset > partial_size) {
	ret_str = result_str.str();
	if (create_blkie) {
	    new_blkie->end_row = row_count - 1;
	    new_blkie->end_byte_offset = row_size;
	    blk_index.update_block_index(ctx->file_path, ctx->block_id, 
		    new_blkie);
	}
    }
    return ret_str;
}

ssize_t ODBCHandler::encode_results(stringstream& str_stream, char* column, bool row_bound)
{
    if (column)
	str_stream<<column;
    else 
	return 0;
    if (!row_bound)
	str_stream<<",";
    else
	str_stream<<"\n";
    return strlen(column) + 1;
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
