// Implementation of the JSyndicateFS methods.

#include "JSyndicateFS.h"

/*
 * Init JSyndicateFS
 */
int jsyndicatefs_init(const JSyndicateFS_Config *cfg) {
    curl_global_init(CURL_GLOBAL_ALL);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int rc = 0;

    char* config_file = (char*) CLIENT_DEFAULT_CONFIG;
    char* ug_name = NULL;
    char* ug_password = NULL;
    char* volume_name = NULL;
    char* volume_secret = NULL;
    char* ms_url = NULL;
    int portnum = -1;

    if(cfg != NULL) {
        config_file = cfg->config_file;
        ms_url = cfg->ms_url;
        ug_name = cfg->ug_name;
        ug_password = cfg->ug_password;
        volume_name = cfg->volume_name;
        volume_secret = cfg->volume_secret;
        portnum = cfg->portnum;
    }

    JSyndicateFS_Context* syndicate_context = jsyndicatefs_get_context();
    
    struct md_HTTP* syndicate_http = &syndicate_context->syndicate_http;

    rc = syndicate_init(config_file, syndicate_http, portnum, ms_url, volume_name, volume_secret, ug_name, ug_password);
    if (rc != 0) {
        return rc;
    }

    printf("\n\nJSyndicateFS starting up\n\n");

    struct syndicate_state* syndicate_st = syndicate_get_state();
    
    syndicate_context->syndicate_state_data = syndicate_st;
    
    return 0;
}

/*
 * Release JSyndicateFS
 */
int jsyndicatefs_destroy() {
    printf("\n\nJSyndicateFS shutting down\n\n");

    dbprintf("%s", "HTTP server shutdown\n");
    
    JSyndicateFS_Context* syndicate_context = jsyndicatefs_get_context();
    struct md_HTTP* syndicate_http = &syndicate_context->syndicate_http;

    md_stop_HTTP(syndicate_http);
    md_free_HTTP(syndicate_http);
    syndicate_destroy();

    curl_global_cleanup();
    google::protobuf::ShutdownProtobufLibrary();
    
    return 0;
}

/*
 * Get file attributes (lstat)
 */
int jsyndicatefs_getattr(const char *path, struct stat *statbuf) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_getattr( %s, %p )\n", path, statbuf);

    SYNDICATEFS_DATA->stats->enter(STAT_GETATTR);

    int rc = fs_entry_stat(SYNDICATEFS_DATA->core, path, statbuf, conf->owner, conf->volume);
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_getattr rc = %d\n", rc);

    SYNDICATEFS_DATA->stats->leave(STAT_GETATTR, rc);

    return rc;
}

/*
 * Create a file node with open(), mkfifo(), or mknod(), depending on the mode.
 * Right now, only normal files are supported.
 */
int jsyndicatefs_mknod(const char *path, mode_t mode, dev_t dev) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_mknod( %s, %o, %d )\n", path, mode, dev);

    SYNDICATEFS_DATA->stats->enter(STAT_MKNOD);

    int rc = fs_entry_mknod(SYNDICATEFS_DATA->core, path, mode, dev, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_MKNOD, rc);
    return rc;
}

/** Create a directory (mkdir) */
int jsyndicatefs_mkdir(const char *path, mode_t mode) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_mkdir( %s, %o )\n", path, mode);

    SYNDICATEFS_DATA->stats->enter(STAT_MKDIR);

    int rc = fs_entry_mkdir(SYNDICATEFS_DATA->core, path, mode, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_MKDIR, rc);
    return rc;
}

/** Remove a file (unlink) */
int jsyndicatefs_unlink(const char* path) {
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_unlink( %s )\n", path);

    SYNDICATEFS_DATA->stats->enter(STAT_UNLINK);

    int rc = fs_entry_versioned_unlink(SYNDICATEFS_DATA->core, path, -1, SYNDICATEFS_DATA->conf.owner, SYNDICATEFS_DATA->conf.volume);

    SYNDICATEFS_DATA->stats->leave(STAT_UNLINK, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_unlink rc = %d\n", rc);
    return rc;
}

/** Remove a directory (rmdir) */
int jsyndicatefs_rmdir(const char *path) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_rmdir( %s )\n", path);

    SYNDICATEFS_DATA->stats->enter(STAT_RMDIR);

    int rc = fs_entry_rmdir(SYNDICATEFS_DATA->core, path, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_RMDIR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_rmdir rc = %d\n", rc);
    return rc;
}

/** Rename a file.  Paths are FS-relative! (rename) */
int jsyndicatefs_rename(const char *path, const char *newpath) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_rename( %s, %s )\n", path, newpath);

    SYNDICATEFS_DATA->stats->enter(STAT_RENAME);

    int rc = fs_entry_rename(SYNDICATEFS_DATA->core, path, newpath, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_RENAME, rc);
    return rc;
}

/** Change the permission bits of a file (chmod) */
int jsyndicatefs_chmod(const char *path, mode_t mode) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_chmod( %s, %o )\n", path, mode);

    SYNDICATEFS_DATA->stats->enter(STAT_CHMOD);

    int rc = fs_entry_chmod(SYNDICATEFS_DATA->core, path, conf->owner, conf->volume, mode);
    if (rc == 0) {
        // TODO: update the modtime and metadata of this file
    }

    SYNDICATEFS_DATA->stats->leave(STAT_CHMOD, rc);
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_chmod rc = %d\n");
    return rc;
}


/** Change the size of a file (truncate) */

/* only works on local files */
int jsyndicatefs_truncate(const char *path, off_t newsize) {
    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_truncate( %s, %ld )\n", path, newsize);

    SYNDICATEFS_DATA->stats->enter(STAT_TRUNCATE);

    int rc = fs_entry_versioned_truncate(SYNDICATEFS_DATA->core, path, newsize, -1, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_TRUNCATE, rc);
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_truncate rc = %d\n", rc);
    return rc;
}

/** Change the access and/or modification times of a file (utime) */
int jsyndicatefs_utime(const char *path, struct utimbuf *ubuf) {
    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_utime( %s, {%d, %d} )\n", path, ubuf->actime, ubuf->modtime);

    SYNDICATEFS_DATA->stats->enter(STAT_UTIME);

    int rc = fs_entry_utime(SYNDICATEFS_DATA->core, path, ubuf, conf->owner, conf->volume);
    if (rc == 0) {
        // TODO: update the modtime of this file
    }

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_utime rc = %d\n", rc);
    SYNDICATEFS_DATA->stats->leave(STAT_UTIME, rc);
    return rc;
}

/** File open operation (O_CREAT and O_EXCL will *not* be passed to this method, according to the documentation) */
int jsyndicatefs_open(const char *path, struct JSyndicateFS_FileInfo *fi) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_open( %s, %p (flags = %o) )\n", path, fi, fi->flags);

    SYNDICATEFS_DATA->stats->enter(STAT_OPEN);

    int err = 0;
    struct fs_file_handle* fh = fs_entry_open(SYNDICATEFS_DATA->core, path, NULL, conf->owner, conf->volume, fi->flags, ~conf->usermask, &err);

    // store the read handle
    //fi->fh = (uint64_t) fh;
    fi->fh = (void*) fh;

    // force direct I/O
    fi->direct_io = 1;

    SYNDICATEFS_DATA->stats->leave(STAT_OPEN, err);
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_open rc = %d\n", err);

    return err;
}

/** Read data from an open file.  Return number of bytes read. */
int jsyndicatefs_read(const char *path, char *buf, size_t size, off_t offset, struct JSyndicateFS_FileInfo *fi) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    if (conf->debug_read)
        logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_read( %s, %p, %ld, %ld, %p )\n", path, buf, size, offset, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_READ);

    struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
    ssize_t rc = fs_entry_read(SYNDICATEFS_DATA->core, fh, buf, size, offset);

    if (rc < 0) {
        SYNDICATEFS_DATA->stats->leave(STAT_READ, -1);
        logerr(SYNDICATEFS_DATA->logfile, "jsyndicatefs_read rc = %ld\n", rc);
        return -1;
    }

    // fill the remainder of buf with 0's
    if (rc < (signed)size) {
        memset(buf + rc, 0, size - rc);
    }

    if (conf->debug_read)
        logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_read rc = %ld\n", rc);

    SYNDICATEFS_DATA->stats->leave(STAT_READ, (rc >= 0 ? 0 : rc));
    return rc;
}

/** Write data to an open file (pwrite) */
int jsyndicatefs_write(const char *path, const char *buf, size_t size, off_t offset, struct JSyndicateFS_FileInfo *fi) {


    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_write( %s, %p, %ld, %ld, %p )\n", path, buf, size, offset, fi->fh);

    SYNDICATEFS_DATA->stats->enter(STAT_WRITE);

    struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
    ssize_t rc = fs_entry_write(SYNDICATEFS_DATA->core, fh, buf, size, offset);

    SYNDICATEFS_DATA->stats->leave(STAT_WRITE, (rc >= 0 ? 0 : rc));

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_write rc = %d\n", rc);
    return (int) rc;
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 */
int jsyndicatefs_statfs(const char *path, struct statvfs *statv) {
    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_statfs( %s, %p )\n", path, statv);

    SYNDICATEFS_DATA->stats->enter(STAT_STATFS);

    int rc = fs_entry_statfs(SYNDICATEFS_DATA->core, path, statv, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_STATFS, rc);
    return rc;
}

/** Possibly flush cached data (No-op) */
int jsyndicatefs_flush(const char *path, struct JSyndicateFS_FileInfo *fi) {

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_flush( %s, %p )\n", path, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_FLUSH);

    struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;

    int rc = fs_entry_fsync(SYNDICATEFS_DATA->core, fh);

    SYNDICATEFS_DATA->stats->leave(STAT_FLUSH, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_flush rc = %d\n", rc);
    return rc;
}

/** Release an open file (close) */
int jsyndicatefs_release(const char *path, struct JSyndicateFS_FileInfo *fi) {

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_release( %s, %p )\n", path, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_RELEASE);

    struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;

    int rc = fs_entry_close(SYNDICATEFS_DATA->core, fh);
    if (rc != 0) {
        logerr(SYNDICATEFS_DATA->logfile, "jsyndicatefs_release: fs_entry_close rc = %d\n", rc);
    }

    free(fh);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_release rc = %d\n", rc);

    SYNDICATEFS_DATA->stats->leave(STAT_RELEASE, rc);
    return rc;
}

/** Synchronize file contents (fdatasync, fsync)
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 */
int jsyndicatefs_fsync(const char *path, int datasync, struct JSyndicateFS_FileInfo *fi) {

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_fsync( %s, %d, %p )\n", path, datasync, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_FSYNC);

    struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
    int rc = 0;
    if (datasync == 0)
        rc = fs_entry_fdatasync(SYNDICATEFS_DATA->core, fh);

    if (rc == 0)
        fs_entry_fsync(SYNDICATEFS_DATA->core, fh);

    SYNDICATEFS_DATA->stats->leave(STAT_FSYNC, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_fsync rc = %d\n", rc);
    return rc;
}

/** Set extended attributes (lsetxattr) */
int jsyndicatefs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    char* safe_value = (char*) calloc(size + 1, 1);
    strncpy(safe_value, value, size);
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_setxattr( %s, %s, %s, %d, %x )\n", path, name, safe_value, size, flags);
    free(safe_value);

    SYNDICATEFS_DATA->stats->enter(STAT_SETXATTR);

    int rc = fs_entry_setxattr(SYNDICATEFS_DATA->core, path, name, value, size, flags, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_SETXATTR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_setxattr rc = %d\n", rc);
    return rc;
}

/** Get extended attributes (lgetxattr) */
int jsyndicatefs_getxattr(const char *path, const char *name, char *value, size_t size) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_getxattr( %s, %s, %p, %d )\n", path, name, value, size);

    SYNDICATEFS_DATA->stats->enter(STAT_GETXATTR);

    int rc = fs_entry_getxattr(SYNDICATEFS_DATA->core, path, name, value, size, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_GETXATTR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_getxattr rc = %d\n", rc);
    return rc;
}

/** List extended attributes (llistxattr) */
int jsyndicatefs_listxattr(const char *path, char *list, size_t size) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_listxattr( %s, %p, %d )\n", path, list, size);

    SYNDICATEFS_DATA->stats->enter(STAT_LISTXATTR);

    int rc = fs_entry_listxattr(SYNDICATEFS_DATA->core, path, list, size, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_LISTXATTR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_listxattr rc = %d\n", rc);

    return rc;
}

/** Remove extended attributes (lremovexattr) */
int jsyndicatefs_removexattr(const char *path, const char *name) {
    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_removexattr( %s, %s )\n", path, name);

    SYNDICATEFS_DATA->stats->enter(STAT_REMOVEXATTR);

    int rc = fs_entry_removexattr(SYNDICATEFS_DATA->core, path, name, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_REMOVEXATTR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_removexattr rc = %d\n", rc);
    return rc;
}

/** Open directory (opendir) */
int jsyndicatefs_opendir(const char *path, struct JSyndicateFS_FileInfo *fi) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_opendir( %s, %p )\n", path, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_OPENDIR);

    int rc = 0;
    struct fs_dir_handle* fdh = fs_entry_opendir(SYNDICATEFS_DATA->core, path, conf->owner, conf->volume, &rc);

    if (rc == 0) {
        //fi->fh = (uint64_t) fdh;
        fi->fh = (void*) fdh;
    }

    SYNDICATEFS_DATA->stats->leave(STAT_OPENDIR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_opendir rc = %d\n", rc);

    return rc;
}

/** Read directory (readdir)
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 */
int jsyndicatefs_readdir(void *pjenv, void *pjobj, const char *path, JSyndicateFS_Fill_Dir_t filler, off_t offset, struct JSyndicateFS_FileInfo *fi) {


    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_readdir( %s, %p, %ld, %p )\n", path, filler, offset, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_READDIR);

    struct fs_dir_handle* fdh = (struct fs_dir_handle *) fi->fh; // get back our DIR instance

    int rc = 0;
    struct fs_dir_entry** dirents = fs_entry_readdir(SYNDICATEFS_DATA->core, fdh, &rc);

    if (rc == 0 && dirents) {

        // fill in the directory data
        int i = 0;
        while (dirents[i] != NULL) {
            if (filler(pjenv, pjobj, dirents[i]->data.path, NULL, 0) != 0) {
                logerr(SYNDICATEFS_DATA->logfile, "ERR: jsyndicatefs_readdir filler: buffer full\n");
                rc = -ENOMEM;
                break;
            }
            i++;
        }
    }

    fs_dir_entry_destroy_all(dirents);
    free(dirents);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_readdir rc = %d\n", rc);

    SYNDICATEFS_DATA->stats->leave(STAT_READDIR, rc);
    return rc;
}

/** Release directory (closedir) */
int jsyndicatefs_releasedir(const char *path, struct JSyndicateFS_FileInfo *fi) {

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_releasedir( %s, %p )\n", path, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_RELEASEDIR);

    struct fs_dir_handle* fdh = (struct fs_dir_handle*) fi->fh;

    int rc = fs_entry_closedir(SYNDICATEFS_DATA->core, fdh);

    free(fdh);

    SYNDICATEFS_DATA->stats->leave(STAT_RELEASEDIR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_releasedir rc = %d\n", rc);
    return rc;
}

/** Synchronize directory contents (no-op) */
int jsyndicatefs_fsyncdir(const char *path, int datasync, struct JSyndicateFS_FileInfo *fi) {
    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_fsyncdir( %s, %d, %p )\n", path, datasync, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_FSYNCDIR);

    SYNDICATEFS_DATA->stats->leave(STAT_FSYNCDIR, 0);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_fsyncdir rc = %d\n", 0);
    return 0;
}

/**
 * Check file access permissions (access)
 */
int jsyndicatefs_access(const char *path, int mask) {
    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_access( %s, %x )\n", path, mask);

    SYNDICATEFS_DATA->stats->enter(STAT_ACCESS);

    int rc = fs_entry_access(SYNDICATEFS_DATA->core, path, mask, conf->owner, conf->volume);

    SYNDICATEFS_DATA->stats->leave(STAT_ACCESS, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_access rc = %d\n", rc);
    return rc;
}

/**
 * Create and open a file (creat)
 */
int jsyndicatefs_create(const char *path, mode_t mode, struct JSyndicateFS_FileInfo *fi) {
    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_create( %s, %o, %p )\n", path, mode, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_CREATE);

    int rc = 0;
    struct fs_file_handle* fh = fs_entry_create(SYNDICATEFS_DATA->core, path, NULL, conf->owner, conf->volume, mode, &rc);

    if (rc == 0 && fh != NULL) {
        //fi->fh = (uint64_t) (fh);
        fi->fh = (void*) fh;
    }

    SYNDICATEFS_DATA->stats->leave(STAT_CREATE, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_create rc = %d\n", rc);
    return rc;
}

/**
 * Change the size of an file (ftruncate)
 */
int jsyndicatefs_ftruncate(const char *path, off_t length, struct JSyndicateFS_FileInfo *fi) {

    struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_ftruncate( %s, %ld, %p )\n", path, length, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_FTRUNCATE);

    struct fs_file_handle* fh = (struct fs_file_handle*) fi->fh;
    int rc = fs_entry_ftruncate(SYNDICATEFS_DATA->core, fh, length, conf->owner, conf->volume);
    if (rc != 0) {
        errorf("fs_entry_ftruncate rc = %d\n", rc);
    }

    SYNDICATEFS_DATA->stats->leave(STAT_FTRUNCATE, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_ftrunctate rc = %d\n", rc);

    return rc;
}

/**
 * Get attributes from an open file (fstat)
 */
int jsyndicatefs_fgetattr(const char *path, struct stat *statbuf, struct JSyndicateFS_FileInfo *fi) {

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_fgetattr( %s, %p, %p )\n", path, statbuf, fi);

    SYNDICATEFS_DATA->stats->enter(STAT_FGETATTR);

    struct fs_file_handle* fh = (struct fs_file_handle*) (fi->fh);
    int rc = fs_entry_fstat(SYNDICATEFS_DATA->core, fh, statbuf);

    SYNDICATEFS_DATA->stats->leave(STAT_FGETATTR, rc);

    logmsg(SYNDICATEFS_DATA->logfile, "jsyndicatefs_fgetattr rc = %d\n", rc);

    return rc;
}
