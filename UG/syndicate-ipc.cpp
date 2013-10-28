/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
 */

#include "libsyndicate.h"
#include "stats.h"
#include "log.h"
#include "fs.h"
#include "replication.h"
#include "collator.h"
#include "syndicate.h"

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <signal.h>
#include <getopt.h>

using boost::asio::ip::tcp;

/*
 * Syndicate Related
 */
struct syndicateipc_context {
    struct syndicate_state* syndicate_state_data;
    struct md_HTTP syndicate_http;
};

syndicateipc_context native_context;

/*
 * Structures
 */
const int MAX_PATH_SIZE = 1024;
const int MAX_XATTR_NAME_SIZE = 1024;
const int MAX_XATTR_VALUE_SIZE = 1024;
const int MAX_XATTR_LIST_INITIAL_SIZE = 1024;

struct IPCFileInfo {
    long long int handle;
};
const int SIZE_IPCFILEINFO = 8;

struct IPCStat {
    int st_mode;
    int st_uid;
    int st_gid; // 12
    long long int st_size;
    long long int st_blksize;
    long long int st_blocks;
    long long int st_atim;
    long long int st_mtim; // 52
};
const int SIZE_IPCSTAT = 52;

/*
 * Private definitions
 */
enum IPCMessageOperations {
    OP_GET_STAT = 0,
    OP_DELETE = 1,
    OP_REMOVE_DIRECTORY = 2,
    OP_RENAME = 3,
    OP_MKDIR = 4,
    OP_READ_DIRECTORY = 5,
    OP_GET_FILE_HANDLE = 6,
    OP_CREATE_NEW_FILE = 7,
    OP_READ_FILEDATA = 8,
    OP_WRITE_FILE_DATA = 9,
    OP_FLUSH = 10,
    OP_CLOSE_FILE_HANDLE = 11,
    OP_TRUNCATE_FILE = 12,
    OP_GET_EXTENDED_ATTR = 13,
    OP_LIST_EXTENDED_ATTR = 14,
};

/*
 * Private functions
 */
static syndicateipc_context* syndicateipc_get_context() {
    return &native_context;
}

#define SYNDICATEFS_DATA (syndicateipc_get_context()->syndicate_state_data)

class packetUtil {
public:

    static int getIntFromBytes(const char* buf) {
        // big endian
        int value;
        char* bytePtr = (char*) &value;
        bytePtr[0] = buf[3];
        bytePtr[1] = buf[2];
        bytePtr[2] = buf[1];
        bytePtr[3] = buf[0];

        return value;
    }

    static void writeIntToBuffer(char* bytes_ptr, int value) {
        char* bytePtr = (char*) &value;
        bytes_ptr[0] = bytePtr[3];
        bytes_ptr[1] = bytePtr[2];
        bytes_ptr[2] = bytePtr[1];
        bytes_ptr[3] = bytePtr[0];
    }

    static long long int getLongFromBytes(const char* buf) {
        long long int value;
        char* bytePtr = (char*) &value;
        bytePtr[0] = buf[7];
        bytePtr[1] = buf[6];
        bytePtr[2] = buf[5];
        bytePtr[3] = buf[4];
        bytePtr[4] = buf[3];
        bytePtr[5] = buf[2];
        bytePtr[6] = buf[1];
        bytePtr[7] = buf[0];

        return value;
    }

    static void writeLongToBuffer(char* bytes_ptr, long long int value) {
        char* bytePtr = (char*) &value;
        bytes_ptr[0] = bytePtr[7];
        bytes_ptr[1] = bytePtr[6];
        bytes_ptr[2] = bytePtr[5];
        bytes_ptr[3] = bytePtr[4];
        bytes_ptr[4] = bytePtr[3];
        bytes_ptr[5] = bytePtr[2];
        bytes_ptr[6] = bytePtr[1];
        bytes_ptr[7] = bytePtr[0];
    }

    static void copyStatToIPCStat(struct IPCStat* ipcstat, const struct stat* stat) {
        ipcstat->st_mode = stat->st_mode;
        ipcstat->st_uid = stat->st_uid;
        ipcstat->st_gid = stat->st_gid;

        ipcstat->st_size = stat->st_size;
        ipcstat->st_blksize = stat->st_blksize;
        ipcstat->st_blocks = stat->st_blocks;
        ipcstat->st_atim = stat->st_atim.tv_sec;
        ipcstat->st_mtim = stat->st_mtim.tv_sec;
    }
};

/*
 * Incoming Packet Structure
4 byte : OP code
4 byte : Total Message Size
4 byte : The number of inner messages
[If have inner messages]
{
4 byte : length of message
n byte : message body
} repeats

 * Outgoing Packet Structure
4 byte : OP code
4 byte : Result of function call (0 : OK, other : Error Code)
4 byte : Total Message Size
4 byte : The number of inner messages
[If have inner messages]
{
4 byte : length of message
n byte : message body
} repeats
 */
class protocol {
public:
    void process_getStat(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - stat\n");
        char *bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        struct stat statbuf;
        int returncode = syndicatefs_getattr(path, &statbuf);

        IPCStat stat;
        packetUtil::copyStatToIPCStat(&stat, &statbuf);

        int toWriteSize = 16;
        if (returncode == 0) {
            toWriteSize += 4 + SIZE_IPCSTAT;
        }

        *data_out = new char[toWriteSize];
        char *outBuffer = *data_out;
        char *bufferNext;

        writeHeader(outBuffer, OP_GET_STAT, returncode, 4 + SIZE_IPCSTAT, 1, &bufferNext);
        if (returncode == 0) {
            outBuffer = bufferNext;
            writeStat(outBuffer, &stat, &bufferNext);
        }

        *data_out_size = toWriteSize;
    }

    void process_delete(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - delete\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        int returncode = syndicatefs_unlink(path);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_DELETE, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_removeDir(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - remove directory\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        int returncode = syndicatefs_rmdir(path);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_REMOVE_DIRECTORY, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_rename(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - rename\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char path1[MAX_PATH_SIZE];
        char path2[MAX_PATH_SIZE];
        readPath(bytes_ptr1, path1, &bytes_ptr2);
        readPath(bytes_ptr2, path2, &bytes_ptr3);

        // call
        int returncode = syndicatefs_rename(path1, path2);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_RENAME, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_makeDir(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - make directory\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);
        mode_t mode = 509; // default

        // call
        int returncode = syndicatefs_mkdir(path, mode);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_MKDIR, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_readDir(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - read directory\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        struct IPCFileInfo fi;
        int returncode = syndicatefs_opendir(path, &fi);

        std::vector<char*> entryVector;
        if (returncode == 0) {
            returncode = syndicatefs_readdir(entryVector, &fi);
        }

        if (returncode == 0) {
            returncode = syndicatefs_releasedir(&fi);
        }

        int totalMessageSize = 0;
        int numOfEntries = entryVector.size();

        for (int i = 0; i < numOfEntries; i++) {
            totalMessageSize += strlen(entryVector[i]);
        }

        totalMessageSize += 4 * numOfEntries;

        int toWriteSize = 16 + totalMessageSize;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_READ_DIRECTORY, returncode, totalMessageSize, numOfEntries, &bufferNext);

        for (int j = 0; j < numOfEntries; j++) {
            outBuffer = bufferNext;
            writePath(outBuffer, entryVector[j], strlen(entryVector[j]), &bufferNext);
            delete entryVector[j];
        }

        entryVector.clear();

        *data_out_size = toWriteSize;
    }

    void process_getFileHandle(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - get file handle\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        IPCFileInfo fi;
        int returncode = syndicatefs_open(path, &fi);

        dbprintf("filehandle : %lld\n", fi.handle);

        int toWriteSize = 16;
        if (returncode == 0) {
            toWriteSize += 4 + SIZE_IPCFILEINFO;
        }

        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_GET_FILE_HANDLE, returncode, 4 + SIZE_IPCFILEINFO, 1, &bufferNext);
        if (returncode == 0) {
            outBuffer = bufferNext;
            writeFileInfo(outBuffer, &fi, &bufferNext);
        }

        *data_out_size = toWriteSize;
    }

    void process_createNewFile(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - create new file\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);
        mode_t mode = 33204; // default

        // call
        IPCFileInfo fi;
        int returncode = syndicatefs_create(path, mode, &fi);

        struct stat statbuf;
        if (returncode == 0) {
            returncode = syndicatefs_fgetattr(&statbuf, &fi);
        }

        if (returncode == 0) {
            returncode = syndicatefs_release(&fi);
        }

        IPCStat stat;
        packetUtil::copyStatToIPCStat(&stat, &statbuf);

        int toWriteSize = 16;
        if (returncode == 0) {
            toWriteSize += 4 + SIZE_IPCSTAT;
        }
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_CREATE_NEW_FILE, returncode, 4 + SIZE_IPCSTAT, 1, &bufferNext);
        if (returncode == 0) {
            outBuffer = bufferNext;
            writeStat(outBuffer, &stat, &bufferNext);
        }

        *data_out_size = toWriteSize;
    }

    void process_readFileData(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - read file data\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char* bytes_ptr4;
        IPCFileInfo fi;
        readFileInfo(bytes_ptr1, &fi, &bytes_ptr2);

        dbprintf("filehandle : %lld\n", fi.handle);

        long long int fileoffset;
        readLong(bytes_ptr2, &fileoffset, &bytes_ptr3);

        int size;
        readInt(bytes_ptr3, &size, &bytes_ptr4);

        dbprintf("offset : %lld, size : %d\n", fileoffset, size);

        // call
        char* buffer = new char[size];
        int returncode = syndicatefs_read(buffer, size, fileoffset, &fi);

        int toWriteSize = 16;
        if (returncode > 0) {
            toWriteSize += 4 + (int) returncode;
        }

        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_READ_FILEDATA, returncode, 4 + (int) returncode, 1, &bufferNext);
        if (returncode >= 0) {
            outBuffer = bufferNext;
            writeBytes(outBuffer, buffer, (int) returncode, &bufferNext);
        }

        delete buffer;

        *data_out_size = toWriteSize;
    }

    void process_writeFileData(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - write file data\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char* bytes_ptr4;
        IPCFileInfo fi;
        readFileInfo(bytes_ptr1, &fi, &bytes_ptr2);

        dbprintf("filehandle : %lld\n", fi.handle);

        long long int fileoffset;
        readLong(bytes_ptr2, &fileoffset, &bytes_ptr3);

        char* rawData;
        int rawDataSize = readBytes(bytes_ptr3, &rawData, &bytes_ptr4);

        // call
        int returncode = syndicatefs_write(rawData, rawDataSize, fileoffset, &fi);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_WRITE_FILE_DATA, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_flush(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - flush file data\n");
        char* bytes_ptr;
        IPCFileInfo fi;
        readFileInfo(message, &fi, &bytes_ptr);

        // call
        int returncode = syndicatefs_flush(&fi);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_FLUSH, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_closeFileHandle(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - close file handle\n");
        char* bytes_ptr;
        IPCFileInfo fi;
        readFileInfo(message, &fi, &bytes_ptr);

        // call
        int returncode = syndicatefs_release(&fi);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_CLOSE_FILE_HANDLE, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }
    
    void process_truncateFile(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - truncate file\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        IPCFileInfo fi;
        readFileInfo(bytes_ptr1, &fi, &bytes_ptr2);

        long long int fileoffset;
        readLong(bytes_ptr2, &fileoffset, &bytes_ptr3);
        
        // call
        int returncode = syndicatefs_ftruncate(fileoffset, &fi);

        int toWriteSize = 16;
        *data_out = new char[toWriteSize];
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_TRUNCATE_FILE, returncode, 0, 0, &bufferNext);
        
        *data_out_size = toWriteSize;
    }
    
    void process_getXAttr(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - getxattr\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char path[MAX_PATH_SIZE];
        readPath(bytes_ptr1, path, &bytes_ptr2);
        
        char name[MAX_XATTR_NAME_SIZE];
        readString(bytes_ptr2, name, &bytes_ptr3);
        
        char value[MAX_XATTR_VALUE_SIZE];

        // call
        int returncode = syndicatefs_getxattr(path, name, value, MAX_XATTR_VALUE_SIZE);
        
        int attrLen = strlen(value);

        int toWriteSize = 16;
        if (returncode >= 0) {
            toWriteSize += 4 + attrLen;
        }

        *data_out = new char[toWriteSize];
        char *outBuffer = *data_out;
        char *bufferNext;

        writeHeader(outBuffer, OP_GET_EXTENDED_ATTR, returncode, 4 + attrLen, 1, &bufferNext);
        if (returncode >= 0) {
            outBuffer = bufferNext;
            writeString(outBuffer, value, attrLen, &bufferNext);
        }

        *data_out_size = toWriteSize;
    }
    
    void process_listXAttr(const char *message, char **data_out, int *data_out_size) {
        dbprintf("%s", "process - listxattr\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char path[MAX_PATH_SIZE];
        readPath(bytes_ptr1, path, &bytes_ptr2);
        
        char* list = new char[MAX_XATTR_LIST_INITIAL_SIZE];
        
        // call
        int returncode = syndicatefs_listxattr(path, list, MAX_XATTR_LIST_INITIAL_SIZE);
        
        std::vector<char*> entryVector;
        char* listptr = list;
        for(int i=0;i<returncode;i++) {
            int entryLen = strlen(listptr);
            if(entryLen > 0) {
                entryVector.push_back(listptr);
            }
            listptr += entryLen + 1;
        }
        
        int totalMessageSize = 0;
        int numOfEntries = entryVector.size();

        for (int i = 0; i < numOfEntries; i++) {
            totalMessageSize += strlen(entryVector[i]);
        }

        totalMessageSize += 4 * numOfEntries;

        int toWriteSize = 16 + totalMessageSize;
        *data_out = new char[toWriteSize];
        char *outBuffer = *data_out;
        char *bufferNext;

        writeHeader(outBuffer, OP_LIST_EXTENDED_ATTR, returncode, totalMessageSize, numOfEntries, &bufferNext);
        
        for (int j = 0; j < numOfEntries; j++) {
            outBuffer = bufferNext;
            writeString(outBuffer, entryVector[j], strlen(entryVector[j]), &bufferNext);
        }
        
        entryVector.clear();
        delete list;

        *data_out_size = toWriteSize;
    }

private:
    int syndicatefs_getattr(const char *path, struct stat *statbuf) {
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_getattr( %s, %p )\n", path, statbuf);

        SYNDICATEFS_DATA->stats->enter(STAT_GETATTR);

        int rc = fs_entry_stat(SYNDICATEFS_DATA->core, path, statbuf, conf->owner, SYNDICATEFS_DATA->core->volume);
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_getattr rc = %d\n", rc);

        SYNDICATEFS_DATA->stats->leave(STAT_GETATTR, rc);

        return rc;
    }

    int syndicatefs_unlink(const char *path) {
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_unlink( %s )\n", path);

        SYNDICATEFS_DATA->stats->enter(STAT_UNLINK);

        int rc = fs_entry_versioned_unlink(SYNDICATEFS_DATA->core, path, 0, 0, -1, SYNDICATEFS_DATA->conf.owner, SYNDICATEFS_DATA->core->volume, false );

        SYNDICATEFS_DATA->stats->leave(STAT_UNLINK, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_unlink rc = %d\n", rc);
        return rc;
    }

    int syndicatefs_rmdir(const char *path) {
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_rmdir( %s )\n", path);

        SYNDICATEFS_DATA->stats->enter(STAT_RMDIR);

        int rc = fs_entry_rmdir(SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume);

        SYNDICATEFS_DATA->stats->leave(STAT_RMDIR, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_rmdir rc = %d\n", rc);
        return rc;
    }

    int syndicatefs_rename(const char *path, const char *newpath) {
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_rename( %s, %s )\n", path, newpath);

        SYNDICATEFS_DATA->stats->enter(STAT_RENAME);

        int rc = fs_entry_rename(SYNDICATEFS_DATA->core, path, newpath, conf->owner, SYNDICATEFS_DATA->core->volume);

        SYNDICATEFS_DATA->stats->leave(STAT_RENAME, rc);
        return rc;
    }

    int syndicatefs_mkdir(const char *path, mode_t mode) {
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_mkdir( %s, %o )\n", path, mode);

        SYNDICATEFS_DATA->stats->enter(STAT_MKDIR);

        int rc = fs_entry_mkdir(SYNDICATEFS_DATA->core, path, mode, conf->owner, SYNDICATEFS_DATA->core->volume);

        SYNDICATEFS_DATA->stats->leave(STAT_MKDIR, rc);
        
        logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_mkdir rc = %d\n", rc );
        return rc;
    }

    //int syndicatefs_opendir(const char *path, struct fuse_file_info *fi) {
    int syndicatefs_opendir(const char *path, struct IPCFileInfo *fi) {
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_opendir( %s )\n", path);

        SYNDICATEFS_DATA->stats->enter(STAT_OPENDIR);

        int rc = 0;
        struct fs_dir_handle* fdh = fs_entry_opendir(SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, &rc);

        if (rc == 0) {
            //fi->fh = (uint64_t) fdh;
            fi->handle = (long long int)fdh;
        }

        SYNDICATEFS_DATA->stats->leave(STAT_OPENDIR, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_opendir rc = %d\n", rc);

        return rc;
    }

    //int syndicatefs_releasedir(const char *path, struct fuse_file_info *fi) {
    int syndicatefs_releasedir(struct IPCFileInfo *fi) {
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_releasedir\n");

        SYNDICATEFS_DATA->stats->enter(STAT_RELEASEDIR);

        //struct fs_dir_handle* fdh = (struct fs_dir_handle*) fi->fh;
        struct fs_dir_handle* fdh = (struct fs_dir_handle*) fi->handle;

        int rc = fs_entry_closedir(SYNDICATEFS_DATA->core, fdh);

        free(fdh);

        SYNDICATEFS_DATA->stats->leave(STAT_RELEASEDIR, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_releasedir rc = %d\n", rc);
        return rc;
    }

    //int syndicatefs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    int syndicatefs_readdir(std::vector<char*> &entryVector, struct IPCFileInfo *fi) {
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_readdir\n");

        SYNDICATEFS_DATA->stats->enter(STAT_READDIR);

        //struct fs_dir_handle* fdh = (struct fs_dir_handle *) fi->fh; // get back our DIR instance
        struct fs_dir_handle* fdh = (struct fs_dir_handle*) fi->handle;

        int rc = 0;
        struct fs_dir_entry** dirents = fs_entry_readdir(SYNDICATEFS_DATA->core, fdh, &rc);

        if (rc == 0 && dirents) {

            // fill in the directory data
            int i = 0;
            while (dirents[i] != NULL) {
                //if (filler(buf, dirents[i]->data.path, NULL, 0) != 0) {
                //    logerr(SYNDICATEFS_DATA->logfile, "ERR: syndicatefs_readdir filler: buffer full\n");
                //    rc = -ENOMEM;
                //    break;
                //}
                int entryPathLen = strlen(dirents[i]->data.name);
                char* entryPath = new char[entryPathLen + 1];
                memcpy(entryPath, dirents[i]->data.name, entryPathLen);
                entryPath[entryPathLen] = 0;
                entryVector.push_back(entryPath);
                i++;
            }
        }

        fs_dir_entry_destroy_all(dirents);
        free(dirents);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_readdir rc = %d\n", rc);

        SYNDICATEFS_DATA->stats->leave(STAT_READDIR, rc);
        return rc;
    }

    int syndicatefs_open(const char *path, struct IPCFileInfo *fi) {
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_open( %s )\n", path);

        SYNDICATEFS_DATA->stats->enter(STAT_OPEN);

        int err = 0;
        //struct fs_file_handle* fh = fs_entry_open(SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, fi->flags, ~conf->usermask, &err);
        struct fs_file_handle* fh = fs_entry_open(SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, 0, ~conf->usermask, &err);
        
        // store the read handle
        //fi->fh = (uint64_t) fh;
        fi->handle = (long long int) fh;

        // force direct I/O
        //fi->direct_io = 1;

        SYNDICATEFS_DATA->stats->leave(STAT_OPEN, err);
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_open rc = %d\n", err);

        return err;
    }

    //int syndicatefs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    int syndicatefs_create(const char *path, mode_t mode, struct IPCFileInfo *fi) {
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_create( %s, %o )\n", path, mode);

        SYNDICATEFS_DATA->stats->enter(STAT_CREATE);

        int rc = 0;
        struct fs_file_handle* fh = fs_entry_create(SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, mode, &rc);

        if (rc == 0 && fh != NULL) {
            //fi->fh = (uint64_t) (fh);
            fi->handle = (long long int) fh;
        }

        SYNDICATEFS_DATA->stats->leave(STAT_CREATE, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_create rc = %d\n", rc);
        return rc;
    }

    //int syndicatefs_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi) {
    int syndicatefs_fgetattr(struct stat *statbuf, struct IPCFileInfo *fi) {

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_fgetattr\n");

        SYNDICATEFS_DATA->stats->enter(STAT_FGETATTR);

        //struct fs_file_handle* fh = (struct fs_file_handle*) (fi->fh);
        struct fs_file_handle* fh = (struct fs_file_handle*) (fi->handle);
        int rc = fs_entry_fstat(SYNDICATEFS_DATA->core, fh, statbuf);

        SYNDICATEFS_DATA->stats->leave(STAT_FGETATTR, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_fgetattr rc = %d\n", rc);

        return rc;
    }

    //int syndicatefs_release(const char *path, struct fuse_file_info *fi) {
    int syndicatefs_release(struct IPCFileInfo *fi) {

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_release\n");

        SYNDICATEFS_DATA->stats->enter(STAT_RELEASE);

        //struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
        struct fs_file_handle* fh = (struct fs_file_handle*) fi->handle;

        int rc = fs_entry_close(SYNDICATEFS_DATA->core, fh);
        if (rc != 0) {
            logerr(SYNDICATEFS_DATA->logfile, "syndicateipc_release: fs_entry_close rc = %d\n", rc);
        }

        free(fh);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_release rc = %d\n", rc);

        SYNDICATEFS_DATA->stats->leave(STAT_RELEASE, rc);
        return rc;
    }

    //int syndicatefs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int syndicatefs_read(char *buf, size_t size, off_t offset, struct IPCFileInfo *fi) {

        logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_read( %ld, %ld )\n", size, offset );
        
        SYNDICATEFS_DATA->stats->enter(STAT_READ);

        //struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
        struct fs_file_handle* fh = (struct fs_file_handle*) fi->handle;
        ssize_t rc = fs_entry_read(SYNDICATEFS_DATA->core, fh, buf, size, offset);

        if (rc < 0) {
            SYNDICATEFS_DATA->stats->leave(STAT_READ, -1);
            logerr(SYNDICATEFS_DATA->logfile, "syndicateipc_read rc = %ld\n", rc);
            return -1;
        }

        // fill the remainder of buf with 0's
        //if (rc < (signed)size) {
        //    memset(buf + rc, 0, size - rc);
        //}

        logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_read rc = %ld\n", rc );

        SYNDICATEFS_DATA->stats->leave(STAT_READ, (rc >= 0 ? 0 : rc));
        return rc;
    }

    //int syndicatefs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int syndicatefs_write(const char *buf, size_t size, off_t offset, struct IPCFileInfo *fi) {
        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_write( %ld, %ld )\n", size, offset);

        SYNDICATEFS_DATA->stats->enter(STAT_WRITE);

        //struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
        struct fs_file_handle* fh = (struct fs_file_handle*) fi->handle;
        ssize_t rc = fs_entry_write(SYNDICATEFS_DATA->core, fh, buf, size, offset);

        SYNDICATEFS_DATA->stats->leave(STAT_WRITE, (rc >= 0 ? 0 : rc));

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_write rc = %d\n", rc);
        return (int) rc;
    }

    //int syndicatefs_flush(const char *path, struct fuse_file_info *fi) {
    int syndicatefs_flush(struct IPCFileInfo *fi) {

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_flush( %p )\n", fi);

        SYNDICATEFS_DATA->stats->enter(STAT_FLUSH);

        //struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
        struct fs_file_handle* fh = (struct fs_file_handle*) fi->handle;

        int rc = fs_entry_fsync(SYNDICATEFS_DATA->core, fh);

        SYNDICATEFS_DATA->stats->leave(STAT_FLUSH, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_flush rc = %d\n", rc);
        return rc;
    }
    
    //int syndicatefs_ftruncate(const char *path, off_t length, struct fuse_file_info *fi) {
    int syndicatefs_ftruncate(off_t length, struct IPCFileInfo *fi) {
        
        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_ftruncate( %ld, %p )\n", length, fi);

        SYNDICATEFS_DATA->stats->enter(STAT_FTRUNCATE);

        //struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
        struct fs_file_handle* fh = (struct fs_file_handle*) fi->handle;
        int rc = fs_entry_ftruncate(SYNDICATEFS_DATA->core, fh, length, conf->owner, SYNDICATEFS_DATA->core->volume);
        if (rc != 0) {
            errorf("fs_entry_ftruncate rc = %d\n", rc);
        }

        SYNDICATEFS_DATA->stats->leave(STAT_FTRUNCATE, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_ftrunctate rc = %d\n", rc);

        return rc;
    }
    
    int syndicatefs_getxattr(const char *path, const char *name, char *value, size_t size) {

        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_getxattr( %s, %s, %p, %d )\n", path, name, value, size);

        SYNDICATEFS_DATA->stats->enter(STAT_GETXATTR);

        int rc = fs_entry_getxattr(SYNDICATEFS_DATA->core, path, name, value, size, conf->owner, SYNDICATEFS_DATA->core->volume);

        SYNDICATEFS_DATA->stats->leave(STAT_GETXATTR, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_getxattr rc = %d\n", rc);
        return rc;
    }
    
    int syndicatefs_listxattr(const char *path, char *list, size_t size) {

        struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_listxattr( %s, %p, %d )\n", path, list, size);

        SYNDICATEFS_DATA->stats->enter(STAT_LISTXATTR);

        int rc = fs_entry_listxattr(SYNDICATEFS_DATA->core, path, list, size, conf->owner, SYNDICATEFS_DATA->core->volume);

        SYNDICATEFS_DATA->stats->leave(STAT_LISTXATTR, rc);

        logmsg(SYNDICATEFS_DATA->logfile, "syndicateipc_listxattr rc = %d\n", rc);

        return rc;
    }
    
private:

    int writeHeader(const char* buffer, int opcode, int returncode, int totalMsgSize, int totalNumOfMsg, char** bufferNext) {
        char* bytes_ptr = (char*)buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, opcode);
        bytes_ptr += 4;

        packetUtil::writeIntToBuffer(bytes_ptr, returncode);
        bytes_ptr += 4;

        packetUtil::writeIntToBuffer(bytes_ptr, totalMsgSize);
        bytes_ptr += 4;

        packetUtil::writeIntToBuffer(bytes_ptr, totalNumOfMsg);
        bytes_ptr += 4;

        *bufferNext = bytes_ptr;
        return 16;
    }

    int readString(const char* msgFrom, char* outString, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = packetUtil::getIntFromBytes(bytes_ptr);
        bytes_ptr += 4;

        strncpy(outString, bytes_ptr, msgLen);
        outString[msgLen] = 0;
        bytes_ptr += msgLen;

        *msgNext = bytes_ptr;
        return msgLen;
    }
    
    int readPath(const char* msgFrom, char* outPath, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = packetUtil::getIntFromBytes(bytes_ptr);
        bytes_ptr += 4;

        strncpy(outPath, bytes_ptr, msgLen);
        outPath[msgLen] = 0;
        bytes_ptr += msgLen;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeString(char* buffer, const char* inString, int strLen, char** bufferNext) {
        char* bytes_ptr = buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, strLen);
        bytes_ptr += 4;

        memcpy(bytes_ptr, inString, strLen);
        bytes_ptr += strLen;

        *bufferNext = bytes_ptr;
        return strLen + 4;
    }
    
    int writePath(char* buffer, const char* path, int pathLen, char** bufferNext) {
        char* bytes_ptr = buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, pathLen);
        bytes_ptr += 4;

        memcpy(bytes_ptr, path, pathLen);
        bytes_ptr += pathLen;

        *bufferNext = bytes_ptr;
        return pathLen + 4;
    }

    int readFileInfo(const char* msgFrom, IPCFileInfo* outFileInfo, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = packetUtil::getIntFromBytes(bytes_ptr);
        bytes_ptr += 4;

        outFileInfo->handle = packetUtil::getLongFromBytes(bytes_ptr);
        bytes_ptr += SIZE_IPCFILEINFO;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeFileInfo(char* buffer, const IPCFileInfo* inFileInfo, char** bufferNext) {
        char* bytes_ptr = buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, 8);
        bytes_ptr += 4;

        packetUtil::writeLongToBuffer(bytes_ptr, inFileInfo->handle);
        bytes_ptr += SIZE_IPCFILEINFO;

        *bufferNext = bytes_ptr;
        return SIZE_IPCFILEINFO + 4;
    }

    int writeStat(char* buffer, const IPCStat* stat, char** bufferNext) {
        char* bytes_ptr = buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, SIZE_IPCSTAT);
        bytes_ptr += 4;

        packetUtil::writeIntToBuffer(bytes_ptr, stat->st_mode);
        bytes_ptr += 4;
        packetUtil::writeIntToBuffer(bytes_ptr, stat->st_uid);
        bytes_ptr += 4;
        packetUtil::writeIntToBuffer(bytes_ptr, stat->st_gid);
        bytes_ptr += 4;
        packetUtil::writeLongToBuffer(bytes_ptr, stat->st_size);
        bytes_ptr += 8;
        packetUtil::writeLongToBuffer(bytes_ptr, stat->st_blksize);
        bytes_ptr += 8;
        packetUtil::writeLongToBuffer(bytes_ptr, stat->st_blocks);
        bytes_ptr += 8;
        packetUtil::writeLongToBuffer(bytes_ptr, stat->st_atim);
        bytes_ptr += 8;
        packetUtil::writeLongToBuffer(bytes_ptr, stat->st_mtim);
        bytes_ptr += 8;

        *bufferNext = bytes_ptr;
        return 4 + SIZE_IPCSTAT;
    }

    int readLong(const char* msgFrom, long long int* outLong, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = packetUtil::getIntFromBytes(bytes_ptr);
        bytes_ptr += 4;

        *outLong = packetUtil::getLongFromBytes(bytes_ptr);
        bytes_ptr += 8;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeLong(char* buffer, long long int value, char** bufferNext) {
        char* bytes_ptr = buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, 8);
        bytes_ptr += 4;

        packetUtil::writeLongToBuffer(bytes_ptr, value);
        bytes_ptr += 8;

        *bufferNext = bytes_ptr;
        return 4 + 8;
    }

    int readInt(const char* msgFrom, int* outInt, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = packetUtil::getIntFromBytes(bytes_ptr);
        bytes_ptr += 4;

        *outInt = packetUtil::getIntFromBytes(bytes_ptr);
        bytes_ptr += 4;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeInt(char* buffer, int value, char** bufferNext) {
        char* bytes_ptr = buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, 4);
        bytes_ptr += 4;

        packetUtil::writeIntToBuffer(bytes_ptr, value);
        bytes_ptr += 4;

        *bufferNext = bytes_ptr;
        return 8;
    }

    int readBytes(const char* msgFrom, char** rawData, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = packetUtil::getIntFromBytes(bytes_ptr);
        bytes_ptr += 4;

        *rawData = bytes_ptr;
        bytes_ptr += msgLen;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeBytes(char* buffer, const char* bytes, int byteLen, char** bufferNext) {
        char* bytes_ptr = buffer;
        packetUtil::writeIntToBuffer(bytes_ptr, byteLen);
        bytes_ptr += 4;

        memcpy(bytes_ptr, bytes, byteLen);
        bytes_ptr += byteLen;

        *bufferNext = bytes_ptr;
        return byteLen + 4;
    }
};

/*
 * Class Definition & Implementation
 */
class session {
public:
    session(boost::asio::io_service& io_service)
    : socket_(io_service) {
    }

    tcp::socket& socket() {
        return socket_;
    }

    void start() {
        stage_ = STAGE_READ_HEADER;
        header_offset_ = 0;
        message_ = NULL;
        message_offset_ = 0;
        data_out_ = NULL;

        socket_.async_read_some(boost::asio::buffer(data_in_, MAX_IN_BUFFER_LENGTH),
                boost::bind(&session::handle_read, this,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));
    }

    void handle_read(const boost::system::error_code& error, size_t bytes_transferred) {
        if (!error) {
            int bytes_remain = (int) bytes_transferred;
            char* bytes_ptr = data_in_;
            while (bytes_remain > 0) {
                if (stage_ == STAGE_READ_HEADER) {
                    if (bytes_remain >= PACKET_HEADER_LENGTH - header_offset_) {
                        int readSize = PACKET_HEADER_LENGTH - header_offset_;
                        memcpy(header_ + header_offset_, bytes_ptr, readSize);
                        header_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;
                        // move stage
                        stage_ = STAGE_READ_DATA;
                        //dbprintf("stage -> read_data\n");
                        // parse header
                        op_code_ = packetUtil::getIntFromBytes(header_);
                        //dbprintf("hdr opcode : %d\n", op_code_);
                        total_msg_size_ = packetUtil::getIntFromBytes(header_ + 4);
                        //dbprintf("hdr msg_size : %d\n", total_msg_size_);
                        num_messages_ = packetUtil::getIntFromBytes(header_ + 8);
                        //dbprintf("hdr num_messages : %d\n", num_messages_);
                        // allocate data
                        message_ = new char[total_msg_size_];
                        message_offset_ = 0;
                    } else {
                        // chunked header
                        int readSize = bytes_remain;
                        memcpy(header_ + header_offset_, bytes_ptr, readSize);
                        header_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;
                    }
                } else if (stage_ == STAGE_READ_DATA) {
                    if (bytes_remain >= total_msg_size_ - message_offset_) {
                        int readSize = total_msg_size_ - message_offset_;
                        memcpy(message_ + message_offset_, bytes_ptr, readSize);
                        message_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;
                        // call processor
                        handle_protocol();
                        // move stage
                        stage_ = STAGE_READ_HEADER;
                        header_offset_ = 0;
                        if (message_ != NULL) {
                            delete message_;
                            message_ = NULL;
                        }
                    } else {
                        // chunked data
                        int readSize = bytes_remain;
                        memcpy(message_ + message_offset_, bytes_ptr, readSize);
                        message_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;
                    }
                }
            }

            // continue read
            socket_.async_read_some(boost::asio::buffer(data_in_, MAX_IN_BUFFER_LENGTH),
                    boost::bind(&session::handle_read, this,
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
        } else {
            errorf("%s", "error\n");
            delete this;
        }
    }

    void handle_write(const boost::system::error_code& error) {
        if (!error) {
            if (data_out_ != NULL) {
                delete data_out_;
                data_out_ = NULL;
            }
        } else {
            errorf("%s", "error\n");
            delete this;
        }
    }

private:
    void handle_protocol() {
        dbprintf("%s", "read done!\n");
        dbprintf("op-code : %d\n", op_code_);
        dbprintf("total message size : %d\n", total_msg_size_);
        dbprintf("number of messages : %d\n", num_messages_);

        protocol protocolHandler;
        int data_out_size = 0;

        switch (op_code_) {
            case OP_GET_STAT:
                protocolHandler.process_getStat(message_, &data_out_, &data_out_size);
                break;
            case OP_DELETE:
                protocolHandler.process_delete(message_, &data_out_, &data_out_size);
                break;
            case OP_REMOVE_DIRECTORY:
                protocolHandler.process_removeDir(message_, &data_out_, &data_out_size);
                break;
            case OP_RENAME:
                protocolHandler.process_rename(message_, &data_out_, &data_out_size);
                break;
            case OP_MKDIR:
                protocolHandler.process_makeDir(message_, &data_out_, &data_out_size);
                break;
            case OP_READ_DIRECTORY:
                protocolHandler.process_readDir(message_, &data_out_, &data_out_size);
                break;
            case OP_GET_FILE_HANDLE:
                protocolHandler.process_getFileHandle(message_, &data_out_, &data_out_size);
                break;
            case OP_CREATE_NEW_FILE:
                protocolHandler.process_createNewFile(message_, &data_out_, &data_out_size);
                break;
            case OP_READ_FILEDATA:
                protocolHandler.process_readFileData(message_, &data_out_, &data_out_size);
                break;
            case OP_WRITE_FILE_DATA:
                protocolHandler.process_writeFileData(message_, &data_out_, &data_out_size);
                break;
            case OP_FLUSH:
                protocolHandler.process_flush(message_, &data_out_, &data_out_size);
                break;
            case OP_CLOSE_FILE_HANDLE:
                protocolHandler.process_closeFileHandle(message_, &data_out_, &data_out_size);
                break;
            case OP_TRUNCATE_FILE:
                protocolHandler.process_truncateFile(message_, &data_out_, &data_out_size);
                break;
            case OP_GET_EXTENDED_ATTR:
                protocolHandler.process_getXAttr(message_, &data_out_, &data_out_size);
                break;
            case OP_LIST_EXTENDED_ATTR:
                protocolHandler.process_listXAttr(message_, &data_out_, &data_out_size);
                break;
        }

        if(data_out_size > 0) {
            boost::asio::async_write(socket_,
                boost::asio::buffer(data_out_, data_out_size),
                boost::bind(&session::handle_write, this,
                boost::asio::placeholders::error));
        }
    }

private:
    tcp::socket socket_;

    enum {
        MAX_IN_BUFFER_LENGTH = 4096,
    };
    char data_in_[MAX_IN_BUFFER_LENGTH];
    char* data_out_;

    int op_code_;
    int total_msg_size_;
    int num_messages_;

    enum {
        // int * 3
        PACKET_HEADER_LENGTH = 12,
    };
    char header_[PACKET_HEADER_LENGTH];
    int header_offset_;

    char* message_;
    int message_offset_;

    enum {
        STAGE_READ_HEADER = 0,
        STAGE_READ_DATA = 1,
    };
    int stage_;
};

class server {
public:

    server(boost::asio::io_service& io_service, short port)
    : io_service_(io_service),
    acceptor_(io_service, tcp::endpoint(tcp::v4(), port)) {
        session* new_session = new session(io_service_);
        acceptor_.async_accept(new_session->socket(),
                boost::bind(&server::handle_accept, this, new_session,
                boost::asio::placeholders::error));
    }

    void handle_accept(session* new_session, const boost::system::error_code& error) {
        if (!error) {
            new_session->start();
            new_session = new session(io_service_);
            acceptor_.async_accept(new_session->socket(),
                    boost::bind(&server::handle_accept, this, new_session,
                    boost::asio::placeholders::error));
        } else {
            delete new_session;
        }
    }

private:
    boost::asio::io_service& io_service_;
    tcp::acceptor acceptor_;
};

int main(int argc, char* argv[]) {

    curl_global_init(CURL_GLOBAL_ALL);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int rc = 0;

    // prevent root from mounting this, since we don't really do much
    // in the way of checking access.
    if (getuid() == 0 || geteuid() == 0) {
        perror("Running SyndicateIPC as root opens unnacceptable security holes\n");
        return 1;
    }

    char* config_file = (char*) CLIENT_DEFAULT_CONFIG;
    char* username = NULL;
    char* password = NULL;
    char* volume_name = NULL;
    char* ms_url = NULL;
    char* gateway_name = NULL;
    int portnum = -1;
    char* volume_pubkey_path = NULL;
    char* gateway_pkey_path = NULL;
    char* tls_pkey_path = NULL;
    char* tls_cert_path = NULL;
    int ipcportnum = -1;

    static struct option syndicate_options[] = {
        {"config-file", required_argument, 0, 'c'},
        {"volume-name", required_argument, 0, 'v'},
        {"username", required_argument, 0, 'u'},
        {"password", required_argument, 0, 'p'},
        {"gateway", required_argument, 0, 'g'},
        {"port", required_argument, 0, 'P'},
        {"ipcport", required_argument, 0, 'O'},
        {"MS", required_argument, 0, 'm'},
        {"volume-pubkey", required_argument, 0, 'V'},
        {"gateway-pkey", required_argument, 0, 'G'},
        {"tls-pkey", required_argument, 0, 'S'},
        {"tls-cert", required_argument, 0, 'C'},
        {0, 0, 0, 0}
    };

    int opt_index = 0;
    int c = 0;
    while ((c = getopt_long(argc, argv, "c:v:u:p:P:O:m:g:V:G:S:C:", syndicate_options, &opt_index)) != -1) {
        switch (c) {
            case 'v':
            {
                volume_name = optarg;
                break;
            }
            case 'c':
            {
                config_file = optarg;
                break;
            }
            case 'u':
            {
                username = optarg;
                break;
            }
            case 'p':
            {
                password = optarg;
                break;
            }
            case 'P':
            {
                portnum = strtol(optarg, NULL, 10);
                break;
            }
            case 'O':
            {
                ipcportnum = strtol(optarg, NULL, 10);
                break;
            }
            case 'm':
            {
                ms_url = optarg;
                break;
            }
            case 'g':
            {
                gateway_name = optarg;
                break;
            }
            case 'V':
            {
                volume_pubkey_path = optarg;
                break;
            }
            case 'G':
            {
                gateway_pkey_path = optarg;
                break;
            }
            case 'S':
            {
                tls_pkey_path = optarg;
                break;
            }
            case 'C':
            {
                tls_cert_path = optarg;
                break;
            }

            default:
            {
                break;
            }
        }
    }

    // we need a mountpoint, and possibly other options
    if (argv[argc - 1][0] == '-') {
        errorf("Usage: %s [-n] [-c CONF_FILE] [-m MS_URL] [-u USERNAME] [-p PASSWORD] [-v VOLUME] [-g GATEWAY_NAME] [-P PORTNUM] [-O IPC_PORTNUM] [-G GATEWAY_PKEY] [-V VOLUME_PUBKEY] [-S TLS_PKEY] [-C TLS_CERT]\n", argv[0]);
        exit(1);
    }

    struct md_HTTP syndicate_http;

    rc = syndicate_init(config_file, &syndicate_http, portnum, ms_url, volume_name, gateway_name, username, password, volume_pubkey_path, gateway_pkey_path, tls_pkey_path, tls_cert_path);
    if (rc != 0)
        exit(1);

    syndicateipc_get_context()->syndicate_state_data = syndicate_get_state();
    syndicateipc_get_context()->syndicate_http = syndicate_http;

    printf("\n\nSyndicateIPC starting up\n\n");

    try {
        boost::asio::io_service io_service;
        server s(io_service, ipcportnum);

        io_service.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    printf("\n\nSyndicateIPC shutting down\n\n");

    dbprintf("%s", "HTTP server shutdown\n");

    md_stop_HTTP(&syndicate_http);
    md_free_HTTP(&syndicate_http);
    syndicate_destroy();

    curl_global_cleanup();
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}
