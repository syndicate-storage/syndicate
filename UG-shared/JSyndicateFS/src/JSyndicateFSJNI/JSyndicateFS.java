package JSyndicateFSJNI;

// this is the Java interface to Syndicate

import JSyndicateFSJNI.struct.JSFSConfig;
import JSyndicateFSJNI.struct.JSFSFileInfo;
import JSyndicateFSJNI.struct.JSFSFillDir;
import JSyndicateFSJNI.struct.JSFSStat;
import JSyndicateFSJNI.struct.JSFSStatvfs;
import JSyndicateFSJNI.struct.JSFSUtimbuf;
import java.io.File;


public class JSyndicateFS {

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
            
            System.out.println("JSFS Library Absolute Path : " + jsfsDLL.getAbsolutePath());
            
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
        return JSyndicateFSJNI.jsyndicatefs_init(cfg);
    }

    public static int jsyndicatefs_destroy() {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_destroy();
    }

    public static int jsyndicatefs_getattr(String path, JSFSStat statbuf) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_getattr(path, statbuf);
    }

    public static int jsyndicatefs_mknod(String path, int mode, long dev) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_mknod(path, mode, dev);
    }

    public static int jsyndicatefs_mkdir(String path, int mode) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_mkdir(path, mode);
    }

    public static int jsyndicatefs_unlink(String path) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_unlink(path);
    }

    public static int jsyndicatefs_rmdir(String path) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_rmdir(path);
    }

    public static int jsyndicatefs_rename(String path, String newpath) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_rename(path, newpath);
    }

    public static int jsyndicatefs_chmod(String path, int mode) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_chmod(path, mode);
    }

    public static int jsyndicatefs_truncate(String path, long newsize) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_truncate(path, newsize);
    }

    public static int jsyndicatefs_utime(String path, JSFSUtimbuf ubuf) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_utime(path, ubuf);
    }

    public static int jsyndicatefs_open(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_open(path, fi);
    }

    public static int jsyndicatefs_read(String path, byte[] buf, long size, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_read(path, buf, size, offset, fi);
    }

    public static int jsyndicatefs_write(String path, byte[] buf, long size, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_write(path, buf, size, offset, fi);
    }

    public static int jsyndicatefs_statfs(String path, JSFSStatvfs statv) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_statfs(path, statv);
    }

    public static int jsyndicatefs_flush(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_flush(path, fi);
    }

    public static int jsyndicatefs_release(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_release(path, fi);
    }

    public static int jsyndicatefs_fsync(String path, int datasync, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_fsync(path, datasync, fi);
    }

    public static int jsyndicatefs_setxattr(String path, String name, byte[] value, long size, int flags) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_setxattr(path, name, value, size, flags);
    }

    public static int jsyndicatefs_getxattr(String path, String name, byte[] value, long size) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_getxattr(path, name, value, size);
    }

    public static int jsyndicatefs_listxattr(String path, byte[] list, long size) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_listxattr(path, list, size);
    }

    public static int jsyndicatefs_removexattr(String path, String name) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_removexattr(path, name);
    }

    public static int jsyndicatefs_opendir(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_opendir(path, fi);
    }

    public static int jsyndicatefs_readdir(String path, JSFSFillDir filler, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_readdir(path, filler, offset, fi);
    }

    public static int jsyndicatefs_releasedir(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_releasedir(path, fi);
    }

    public static int jsyndicatefs_fsyncdir(String path, int datasync, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_fsyncdir(path, datasync, fi);
    }

    public static int jsyndicatefs_access(String path, int mask) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_access(path, mask);
    }

    public static int jsyndicatefs_create(String path, int mode, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_create(path, mode, fi);
    }

    public static int jsyndicatefs_ftruncate(String path, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_ftruncate(path, offset, fi);
    }

    public static int jsyndicatefs_fgetattr(String path, JSFSStat statbuf, JSFSFileInfo fi) {
        checkLibraryLoaded();
        return JSyndicateFSJNI.jsyndicatefs_fgetattr(path, statbuf, fi);
    }
}
