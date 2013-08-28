AG - SQL Driver
=============

This is a driver for AG that maps file names to SQL queries and publishes them to volumes attached to it.

Block Index:

    To server content efficiently, AG - SQL driver  maintains  a block index. AG - SQL driver  has its own block 
    size  which is a multiple of  the underlying PAGE_SIZE. For each incoming  block request, driver  translates 
    the requested block - id based  on volume’s block size to one  or more driver specific block - ids that maps 
    to the data range of the requested block. 
    
    Currently  the  size of a block index entry is 32 bytes, each block index entry is backed by a block of 40960 
    bytes (on a  platform  that has 4KB Pages). That is roughly 1MB block index for a 1GB file. This ratio could 
    be further increased by  increasing drivers’ internal block size (see AG_BLOCK_SIZE() in block-index.h). 
    
    The block index is built lazily when block is requested for the first time. When a block is requested, driver 
    checks whether a block entry is already in the index for the requested block, if that’s in the entry it reads 
    start and end row offsets to query the database using cursors to limit the result set size (if the block index 
    entry is backed  by DRAM or distributed memory in the future, no database query would be required). If driver 
    does not find a block index entry for the block then it will search for the block index entry available in the 
    block  index  for the  largest  block-id smaller than the requested block-id. Driver will then build the block 
    index starting from that  block  to  the  requested  block. Building a block  index entry is relatively  cheap 
    when the previous block of the requested block is available in the index as it can be used to bound the size of 
    result  set by using database cursors. Worst  case scenario  for this algorithm is  reading a previously unread 
    file  after  seeking  multiple  blocks  to the  front, in  such cases the driver will have to map data from the 
    database to a previously unknown number of blocks.

Invalidation of Mappings:

    Data - curator can set timeout values for each filename to SQL query mapping in the configuration file. When a 
    mapping times out the file will be reversioned  in the MS and  whatever the driver has in  its caches about the
    mapping will be invalidated.

Mapping XML:
    Given below is a mapping XML used to map filenames to SQL qureis. 
    
    <?xml version="1.0"?>
    <Map>
        <Config>
	        <DSN>sqlite</DSN>
        </Config>
        <Pair reval="2m">
	        <File perm="740">/foo/bar</File>
	        <Query>SELECT * FROM mytable WHERE id > 100 LIMIT %i OFFSET %i</Query>
	        <Query type="unbounded-sql">SELECT * FROM mytable WHERE id > 100 LIMIT -1 OFFSET %i</Query>
        </Pair>
    </Map>

    <Map> Tag
    ---------
    Everything should be enclosed with <Map> tags. Within <Map> tags there can be any number of <Pair> tags but only
    one <Config> tag.
    
    <Config> Tag
    ------------
    <Config> tag contains configuration information for this mapping. At the moment only <DSN> tag is meaningful to
    SQL driver.
    
    <DSN> Tag
    ---------
    <DSN> tag encloses the ODBC "Data Souce Name" string. Data curator knows the DSN string for his database and he
    should place it within <DSN> tags so that the SQL driver will be able to find the database to talk to via ODBC.
    
    <Pair> Tag
    ----------
    <Pair> glues a file name enclosed in <File> tag to SQL queries it's mapped to which are enclosed in <Query> tags.
    The SQL driver needs two <Query> tags within a <Pair> tag but only one <File> tag is permitted.
    
    "reval" attribute can specify a timeout value for the mapping. After this amount of time the driver will revalidate 
    this mapping as mentioned in Ivalidations of Mappings section.
    
    Time can be specified in weeks(w), days(d), hours(h), minutes(m) and seconds(s). Given below are some examples on
    specifying time.
    
        - Timeout in 30 seconds
            reval="30s"
        
        - Timeout in 90 seconds
            reval="90s"
            reval="1m 30s"
        
        - Timeout in 48 weeks and two days
            reval="48w 2d"
    
    Default value for "reval" attribute is 48 weeks.
    
    
    <File> Tag
    ----------
    <File> tag encloses the name of the file the SQL query is mapped to. File name should be followed by an absolute 
    path starting from the root (/). It is under this name we publish this mapping to MS.
    
    "perm" attribute can be associated with <File> tag to specify file permission bits (POSIX). However AGs only support
    read only files, therefore values assigned to "perm" will be overidden by the driver to read only.
    
    <Query> Tag
    -----------
    <Query> tag encloses the SQL query mapped to the file name specified in <File> tag. There has to be exactly two 
    <Query> tags together withing a one <Pair> tag. This is due to block - indexing algorithm of the SQL driver. See
    the section on "type" attribute for more information.
    
    "type" attribute specifies the type of query the <Query> tag encloses. SQL driver has SQL queries of two types 
    that needs to be mapped to a file. The first type is 'bounded-sql', which has the following format.
    
    	- "SELECT * FROM mytable WHERE id > 100 LIMIT %i OFFSET %i"
    
    	This type of qurey will limit the result set to a pre-specified number of raws using keywords such as "LIMIT"
    	as shown in the example. Please refer to your database manuel to find out how such queries should be composed.
    
	The other query type is 'unbounded-sql', which has the following format.
    
    	- "SELECT * FROM mytable WHERE id > 100 LIMIT -1 OFFSET %i"
    
    	This query does not limit the result set as in the previous query due to "LIMIT -1" used in the query. Also 
    	note that two queries in the two <Query> tags in the <Pair> tag should be  'bounded-sql' and 'unbouded-sql' 
    	versions of the same query if 'LIMIT's are disregarded.  
    
    The default value used for "type" is "bounded-sql".

Build Instructions:

    - scons AG/drivers/sql
    - scons AG-SQL-driver-install




