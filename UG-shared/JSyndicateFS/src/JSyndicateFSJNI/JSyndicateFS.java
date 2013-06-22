package JSyndicateFSJNI;

// this is the Java interface to Syndicate

import JSyndicateFSJNI.struct.JSFSConfig;
import JSyndicateFSJNI.struct.JSFSFileInfo;
import JSyndicateFSJNI.struct.JSFSFillDir;
import JSyndicateFSJNI.struct.JSFSStat;
import JSyndicateFSJNI.struct.JSFSStatvfs;
import JSyndicateFSJNI.struct.JSFSUtimbuf;
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class JSyndicateFS {

    public static final Log LOG = LogFactory.getLog(JSyndicateFS.class);
    
    public static final String LIBRARY_FILE_NAME = "libjsyndicatefs.so";
    public static final String LIBRARY_FILE_PATH_KEY = "JSyndicateFS.JSyndicateFSJNI.LibraryPath";
    private static boolean isLibraryLoaded = false;
    
    static { loadLibrary(); }
    
    protected static void loadLibrary() {
        // check library is already loaded
        if(isLibraryLoaded) return;
        
        String libraryFilename = System.getProperty(LIBRARY_FILE_PATH_KEY, null);
        
        if((libraryFilename != null) && (!libraryFilename.isEmpty())) {
            File jsfsDLL = new File(libraryFilename);
            
            LOG.info("JSFS Library Load : " + jsfsDLL.getAbsolutePath());
            
            if(jsfsDLL.exists() && jsfsDLL.canRead() && jsfsDLL.isFile()) {
                try {
                    System.load(jsfsDLL.getAbsolutePath());
                    isLibraryLoaded = true;
                } catch (Exception ex) {
                    isLibraryLoaded = false;
                    throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + libraryFilename);
                }
            } else {
                isLibraryLoaded = false;
                throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + libraryFilename);
            }
        } else {
            isLibraryLoaded = false;
            throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : Empty Path");
        }
    }
    
    protected static void checkLibraryLoaded() {
        if(!isLibraryLoaded) {
            throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library");
        }
    }
    
    public static int jsyndicatefs_init(JSFSConfig cfg) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_init");
        return JSyndicateFSJNI.jsyndicatefs_init(cfg);
    }

    public static int jsyndicatefs_destroy() {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_destroy");
        return JSyndicateFSJNI.jsyndicatefs_destroy();
    }

    public static int jsyndicatefs_getattr(String path, JSFSStat statbuf) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_getattr");
        return JSyndicateFSJNI.jsyndicatefs_getattr(path, statbuf);
    }

    public static int jsyndicatefs_mknod(String path, int mode, long dev) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_mknod");
        return JSyndicateFSJNI.jsyndicatefs_mknod(path, mode, dev);
    }

    public static int jsyndicatefs_mkdir(String path, int mode) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_mkdir");
        return JSyndicateFSJNI.jsyndicatefs_mkdir(path, mode);
    }

    public static int jsyndicatefs_unlink(String path) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_unlink");
        return JSyndicateFSJNI.jsyndicatefs_unlink(path);
    }

    public static int jsyndicatefs_rmdir(String path) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_rmdir");
        return JSyndicateFSJNI.jsyndicatefs_rmdir(path);
    }

    public static int jsyndicatefs_rename(String path, String newpath) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_rename");
        return JSyndicateFSJNI.jsyndicatefs_rename(path, newpath);
    }

    public static int jsyndicatefs_chmod(String path, int mode) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_chmod");
        return JSyndicateFSJNI.jsyndicatefs_chmod(path, mode);
    }

    public static int jsyndicatefs_truncate(String path, long newsize) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_truncate");
        return JSyndicateFSJNI.jsyndicatefs_truncate(path, newsize);
    }

    public static int jsyndicatefs_utime(String path, JSFSUtimbuf ubuf) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_utime");
        return JSyndicateFSJNI.jsyndicatefs_utime(path, ubuf);
    }

    public static int jsyndicatefs_open(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_open");
        return JSyndicateFSJNI.jsyndicatefs_open(path, fi);
    }

    public static int jsyndicatefs_read(String path, byte[] buf, long size, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_read");
        return JSyndicateFSJNI.jsyndicatefs_read(path, buf, size, offset, fi);
    }

    public static int jsyndicatefs_write(String path, byte[] buf, long size, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_write");
        return JSyndicateFSJNI.jsyndicatefs_write(path, buf, size, offset, fi);
    }

    public static int jsyndicatefs_statfs(String path, JSFSStatvfs statv) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_statfs");
        return JSyndicateFSJNI.jsyndicatefs_statfs(path, statv);
    }

    public static int jsyndicatefs_flush(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_flush");
        return JSyndicateFSJNI.jsyndicatefs_flush(path, fi);
    }

    public static int jsyndicatefs_release(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_release");
        return JSyndicateFSJNI.jsyndicatefs_release(path, fi);
    }

    public static int jsyndicatefs_fsync(String path, int datasync, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_fsync");
        return JSyndicateFSJNI.jsyndicatefs_fsync(path, datasync, fi);
    }

    public static int jsyndicatefs_setxattr(String path, String name, byte[] value, long size, int flags) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_setxattr");
        return JSyndicateFSJNI.jsyndicatefs_setxattr(path, name, value, size, flags);
    }

    public static int jsyndicatefs_getxattr(String path, String name, byte[] value, long size) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_getxattr");
        return JSyndicateFSJNI.jsyndicatefs_getxattr(path, name, value, size);
    }

    public static int jsyndicatefs_listxattr(String path, byte[] list, long size) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_listxattr");
        return JSyndicateFSJNI.jsyndicatefs_listxattr(path, list, size);
    }

    public static int jsyndicatefs_removexattr(String path, String name) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_removexattr");
        return JSyndicateFSJNI.jsyndicatefs_removexattr(path, name);
    }

    public static int jsyndicatefs_opendir(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_opendir");
        return JSyndicateFSJNI.jsyndicatefs_opendir(path, fi);
    }

    public static int jsyndicatefs_readdir(String path, JSFSFillDir filler, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_readdir");
        return JSyndicateFSJNI.jsyndicatefs_readdir(path, filler, offset, fi);
    }

    public static int jsyndicatefs_releasedir(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_releasedir");
        return JSyndicateFSJNI.jsyndicatefs_releasedir(path, fi);
    }

    public static int jsyndicatefs_fsyncdir(String path, int datasync, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_fsyncdir");
        return JSyndicateFSJNI.jsyndicatefs_fsyncdir(path, datasync, fi);
    }

    public static int jsyndicatefs_access(String path, int mask) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_access");
        return JSyndicateFSJNI.jsyndicatefs_access(path, mask);
    }

    public static int jsyndicatefs_create(String path, int mode, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_create");
        return JSyndicateFSJNI.jsyndicatefs_create(path, mode, fi);
    }

    public static int jsyndicatefs_ftruncate(String path, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_ftruncate");
        return JSyndicateFSJNI.jsyndicatefs_ftruncate(path, offset, fi);
    }

    public static int jsyndicatefs_fgetattr(String path, JSFSStat statbuf, JSFSFileInfo fi) {
        checkLibraryLoaded();
        LOG.info("jsyndicatefs_fgetattr");
        return JSyndicateFSJNI.jsyndicatefs_fgetattr(path, statbuf, fi);
    }
}
