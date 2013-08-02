#include <libgateway.h>
#include <odbc-handler.h>

#define BLK_SIZE (global_conf->blocking_factor)

void invalidate_entry(void *cls)
{    
    struct invalidation_info *inval = (struct invalidation_info*)cls;
    if (inval == NULL)
	return;
    if (inval->file_path != NULL) { 
	string file_path(inval->file_path);
	if (inval->blk_index != NULL)
	    inval->blk_index->invalidate_entry(file_path);
    }
}

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

string ODBCHandler::execute_query(unsigned char* query, ssize_t threashold, 
				  off_t *row_count, ssize_t *len, ssize_t *last_row_len)
{
    SQLHSTMT		stmt;
    SQLRETURN		ret; 
    SQLSMALLINT		nr_columns;
    string		ODBC_error; 
    stringstream	result_str;
    bool		row_bound = false;

    cout<<"Query: "<<query<<endl;
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
    *row_count = 0;
    *len = 0;
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
	SQLUSMALLINT i;
	*len += *last_row_len;
	(*last_row_len) = 0;
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
		*last_row_len += encode_results(result_str, buf, row_bound);
	    }
	}
	if (*len + *last_row_len >= threashold)
	    break;
	(*row_count)++;
	row_bound = false;
    }
    return result_str.str();
}

void ODBCHandler::execute_query(struct gateway_ctx *ctx, struct map_info* mi, ssize_t read_size) 
{
    stringstream	result_str;
    stringstream	shadow_result_str;
    string		ret_str;
    const block_index_entry	*blkie = NULL;
    block_index_entry	*new_blkie = NULL;
    ssize_t		query_len = 0;
    unsigned char*	query = NULL;
    off_t		last_blk_id = 0;
    bool		query_unbound = false;
    off_t		row_count = 0;
    ssize_t		len = 0, last_row_len = 0;
    string		results;
    const char*		results_cstr;
    off_t		block_start_byte_offset = 0;
    ssize_t		db_read_size = 0;

    //Get the block_index_entry for ctx->block_id of the file_name.
    //If it is there, skip rows. Else get the last block_index_entry
    //for the file. If its block id is ctx->block_id - 1 then process 
    //from there and update block index for ctx->block_id block. 
    //Else go through last block to ctx->block_id and update block index
    //for each block.

    blkie = blk_index.get_block(ctx->file_path, ctx->block_id);
    if (blkie != NULL) {
	if (ctx->sql_query_bounded == NULL)
	    return;
	//I'm feeling lucky...
	query_len = strlen((const char*)ctx->sql_query_bounded) + 21;
	query = (unsigned char*)malloc(query_len);
	memset(query, 0, query_len);
	snprintf((char*)query, query_len, (const char*)ctx->sql_query_bounded, 
	     (blkie->end_row - blkie->start_row) + 1, blkie->start_row);
    }
    else {
	blkie = blk_index.get_last_block(ctx->file_path, &last_blk_id);
	if (ctx->sql_query_unbounded == NULL)
	    return;
	query_unbound = true;
	query_len = strlen((const char*)ctx->sql_query_unbounded) + 11;
	query = (unsigned char*)malloc(query_len);
    }

    //Invalidation info...
    struct invalidation_info *inval = new struct invalidation_info;
    inval->file_path = strdup(ctx->file_path);
    inval->blk_index = &blk_index;
    mi->entry = inval;
    mi->invalidate_entry = invalidate_entry;
    
    if (query_unbound) {
	off_t blk_count = (blkie != NULL)?last_blk_id+1:0;
	off_t start_row = (blkie != NULL)?blkie->end_row:0;
	off_t start_byte_offset = (blkie != NULL)?blkie->end_byte_offset:0;
	for (; blk_count < ctx->block_id+1; blk_count++) {
	    row_count = 0;
	    len = 0;
	    last_row_len = 0;
	    memset(query, 0, query_len);
	    snprintf((char*)query, query_len, (const char*)ctx->sql_query_unbounded, start_row);
	    db_read_size = BLK_SIZE + start_byte_offset;
	    results = execute_query(query, db_read_size, &row_count, &len, &last_row_len);

	    //If there is no data do not proceed...
	    if (!(len + last_row_len)) {
		break;
	    }

	    //Update the block index
	    new_blkie = blk_index.alloc_block_index_entry();
	    new_blkie->start_row = start_row;
	    new_blkie->start_byte_offset = start_byte_offset;
	    block_start_byte_offset = start_byte_offset;
	    new_blkie->end_row = start_row + row_count;
	    new_blkie->end_byte_offset = db_read_size - len;
	    len += last_row_len;
	    len = (BLK_SIZE > len - start_byte_offset)?len - start_byte_offset:BLK_SIZE;
	    blk_index.update_block_index(ctx->file_path, blk_count, new_blkie);

	    //Update start_row and start_byte_offset for the next block
	    start_row += row_count;
	    start_byte_offset = new_blkie->end_byte_offset;
	}
    }
    else {
	db_read_size = BLK_SIZE + blkie->start_byte_offset;
	results = execute_query(query, db_read_size, &row_count, &len, &last_row_len);
	len += last_row_len;
	len = (BLK_SIZE > len - blkie->start_byte_offset)?len - blkie->start_byte_offset:BLK_SIZE;
	block_start_byte_offset = blkie->start_byte_offset;
    }
    if (len) {
	results_cstr = results.c_str();
	ctx->data_len = len;
	ctx->data = (char*)malloc(ctx->data_len);
	memcpy(ctx->data, results_cstr+block_start_byte_offset, ctx->data_len);
    }
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
