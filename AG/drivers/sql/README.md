AG - SQL Driver

=============

This is a driver for AG that maps file names to SQL queries and publishes them to volumes attached to it.

Block Index:

    To server content efficiently, AG - SQL driver maintains a block index. AG - SQL driver has its own block size which is a multiple of the underlying PAGE_SIZE. For each incoming block request, driver translates the requested block - id based on volume’s block size to one or more driver specific block - ids that maps to the data range of the requested block. Currently the size of a block index entry is 32 bytes, each block index entry is backed by a block of 40960 bytes (on a platform that has 4KB Pages). That is roughly 1MB block index for a 1GB file. This ratio could be further increased by increasing drivers’ internal block size (see AG_BLOCK_SIZE() in block-index.h). 
    The block index is built lazily when block is requested for the first time. When a block is requested, driver checks whether a block entry is already in the index for the requested block, if that’s in the entry it reads start and end row offsets to query the database using cursors to limit the result set size (if the block index entry is backed by DRAM or distributed memory in the future, no database query would be required). If driver does not find a block index entry for the block then it will search for the block index entry available in the block index for the largest block-id smaller than the requested block-id. Driver will then build the block index starting from that block to the requested block. Building a block index entry is relatively cheap when the previous block of the requested block is available in the index as it can be used to bound the size of result set by using database cursors. Worst case scenario for this algorithm is reading a previously unread file after seeking multiple blocks to the front, in such cases the driver will have to map data from the database to a previously unknown number of blocks.

Consistency:

    Data - curator can set timeout values for each filename to SQL query mapping in the configuration file. When a mapping times out the file will be reversioned in the MS and whatever the driver has in its caches about the mapping will be invalidated.

Mapping XML:


Build Instructions:

    - scons AG/drivers/sql
    - scons AG-SQL-driver-install




