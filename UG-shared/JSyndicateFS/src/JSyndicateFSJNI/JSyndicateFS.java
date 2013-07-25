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
    
    public static final String LIBRARY_FILE_NAME = "libjsyndicatefs";
    public static final String LIBRARY_FILE_PATH_KEY = "JSyndicateFS.JSyndicateFSJNI.LibraryPath";
    private static boolean isLibraryLoaded = false;
    private static boolean isSyndicateInitialized = false;
    private static String userLibraryPath;
    
    protected static void loadLibrary() {
        // check library is already loaded
        if(isLibraryLoaded) return;
        
        if(userLibraryPath != null && !userLibraryPath.isEmpty()) {
            LOG.info("JSFS Library Load in user given path : " + userLibraryPath);

            File jsfsDLL = new File(userLibraryPath);
            if (jsfsDLL.exists() && jsfsDLL.canRead() && jsfsDLL.isFile()) {
                try {
                    System.load(userLibraryPath);
                    isLibraryLoaded = true;
                } catch (Exception ex) {
                    isLibraryLoaded = false;

                    LOG.error("Library loading failed : " + ex.toString());
                    throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + userLibraryPath);
                } catch (UnsatisfiedLinkError ex) {
                    isLibraryLoaded = false;

                    LOG.error("Library loading failed : " + ex.toString());
                    throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + userLibraryPath);
                }
            } else {
                isLibraryLoaded = false;
                throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : Empty Path");
            }
        } else {
            String libraryFilename = System.getProperty(LIBRARY_FILE_PATH_KEY, LIBRARY_FILE_NAME);

            if (libraryFilename.equals(LIBRARY_FILE_NAME)) {
                // default case -- find in class path
                LOG.info("JSFS Library Load in ClassPath : " + LIBRARY_FILE_NAME);

                try {
                    System.loadLibrary(libraryFilename);
                    isLibraryLoaded = true;
                } catch (Exception ex) {
                    isLibraryLoaded = false;

                    LOG.error("Library loading failed : " + ex.toString());
                    LOG.debug("Classpath used : " + System.getProperty("java.library.path"));

                    throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + libraryFilename);
                } catch (UnsatisfiedLinkError ex) {
                    isLibraryLoaded = false;

                    LOG.error("Library loading failed : " + ex.toString());
                    LOG.debug("Classpath used : " + System.getProperty("java.library.path"));

                    throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + libraryFilename);
                }
            } else {
                // path was given
                if ((libraryFilename != null) && (!libraryFilename.isEmpty())) {
                    File jsfsDLL = new File(libraryFilename);
                    LOG.info("JSFS Library Load : " + jsfsDLL.getAbsolutePath());

                    if (jsfsDLL.exists() && jsfsDLL.canRead() && jsfsDLL.isFile()) {
                        try {
                            System.load(jsfsDLL.getAbsolutePath());
                            isLibraryLoaded = true;
                        } catch (Exception ex) {
                            isLibraryLoaded = false;

                            LOG.error("Library loading failed : " + ex.toString());

                            throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + libraryFilename);
                        } catch (UnsatisfiedLinkError ex) {
                            isLibraryLoaded = false;

                            LOG.error("Library loading failed : " + ex.toString());

                            throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + libraryFilename);
                        }
                    } else {
                        isLibraryLoaded = false;

                        LOG.error("Library loading failed (path error) : " + libraryFilename);

                        throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : " + libraryFilename);
                    }
                } else {
                    isLibraryLoaded = false;
                    throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library : Empty Path");
                }
            }
        }
    }
    
    protected static void checkLibraryLoaded() {
        if(!isLibraryLoaded) {
            throw new UnsatisfiedLinkError("Invalid JSyndicateFSNative Library");
        }
    }
    
    protected static void checkSyndicateInit() {
        if(!isSyndicateInitialized) {
            throw new IllegalStateException("Syndicate is not initialized");
        }
    }
    
    public static void setLibraryPath(String path) {
        userLibraryPath = path;
    }
    
    public static int jsyndicatefs_init(JSFSConfig cfg) {
        loadLibrary();
        checkLibraryLoaded();
        if(isSyndicateInitialized) {
            throw new IllegalStateException("Syndicate is already initialized");
        }
        
        LOG.info("jsyndicatefs_init");
        LOG.info("Config - UG_name : " + cfg.getUGName());
        
        int ret = JSyndicateFSJNI.jsyndicatefs_init(cfg);
        if(ret == 0) {
            isSyndicateInitialized = true;
            LOG.info("jsyndicatefs_init initialized");
        } else {
            isSyndicateInitialized = false;
            LOG.error("jsyndicatefs_init failed");
        }
        
        return ret;
    }

    public static int jsyndicatefs_destroy() {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_destroy");
        
        return JSyndicateFSJNI.jsyndicatefs_destroy();
    }

    public static int jsyndicatefs_getattr(String path, JSFSStat statbuf) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_getattr");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_getattr(path, statbuf);
    }

    public static int jsyndicatefs_mknod(String path, int mode, long dev) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_mknod");
        LOG.debug("path : " + path);
        LOG.debug("mode : " + mode);
        LOG.debug("dev : " + dev);
        
        return JSyndicateFSJNI.jsyndicatefs_mknod(path, mode, dev);
    }

    public static int jsyndicatefs_mkdir(String path, int mode) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_mkdir");
        LOG.debug("path : " + path);
        LOG.debug("mode : " + mode);
        
        return JSyndicateFSJNI.jsyndicatefs_mkdir(path, mode);
    }

    public static int jsyndicatefs_unlink(String path) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_unlink");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_unlink(path);
    }

    public static int jsyndicatefs_rmdir(String path) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_rmdir");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_rmdir(path);
    }

    public static int jsyndicatefs_rename(String path, String newpath) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_rename");
        LOG.debug("path : " + path);
        LOG.debug("newpath : " + newpath);
        
        return JSyndicateFSJNI.jsyndicatefs_rename(path, newpath);
    }

    public static int jsyndicatefs_chmod(String path, int mode) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_chmod");
        LOG.debug("path : " + path);
        LOG.debug("mode : " + mode);
        
        return JSyndicateFSJNI.jsyndicatefs_chmod(path, mode);
    }

    public static int jsyndicatefs_truncate(String path, long newsize) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_truncate");
        LOG.debug("path : " + path);
        LOG.debug("newsize : " + newsize);
        
        return JSyndicateFSJNI.jsyndicatefs_truncate(path, newsize);
    }

    public static int jsyndicatefs_utime(String path, JSFSUtimbuf ubuf) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_utime");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_utime(path, ubuf);
    }

    public static int jsyndicatefs_open(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_open");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_open(path, fi);
    }

    public static int jsyndicatefs_read(String path, byte[] buf, long size, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_read");
        LOG.debug("path : " + path);
        LOG.debug("size : " + size);
        LOG.debug("offset : " + offset);
        
        return JSyndicateFSJNI.jsyndicatefs_read(path, buf, size, offset, fi);
    }

    public static int jsyndicatefs_write(String path, byte[] buf, long size, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_write");
        LOG.debug("path : " + path);
        LOG.debug("size : " + size);
        LOG.debug("offset : " + offset);
        
        return JSyndicateFSJNI.jsyndicatefs_write(path, buf, size, offset, fi);
    }

    public static int jsyndicatefs_statfs(String path, JSFSStatvfs statv) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_statfs");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_statfs(path, statv);
    }

    public static int jsyndicatefs_flush(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_flush");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_flush(path, fi);
    }

    public static int jsyndicatefs_release(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_release");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_release(path, fi);
    }

    public static int jsyndicatefs_fsync(String path, int datasync, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_fsync");
        LOG.debug("path : " + path);
        LOG.debug("datasync : " + datasync);
        
        return JSyndicateFSJNI.jsyndicatefs_fsync(path, datasync, fi);
    }

    public static int jsyndicatefs_setxattr(String path, String name, byte[] value, long size, int flags) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_setxattr");
        LOG.debug("path : " + path);
        LOG.debug("name : " + name);
        
        return JSyndicateFSJNI.jsyndicatefs_setxattr(path, name, value, size, flags);
    }

    public static int jsyndicatefs_getxattr(String path, String name, byte[] value, long size) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_getxattr");
        LOG.debug("path : " + path);
        LOG.debug("name : " + name);
        
        return JSyndicateFSJNI.jsyndicatefs_getxattr(path, name, value, size);
    }

    public static int jsyndicatefs_listxattr(String path, byte[] list, long size) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_listxattr");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_listxattr(path, list, size);
    }

    public static int jsyndicatefs_removexattr(String path, String name) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_removexattr");
        LOG.debug("path : " + path);
        LOG.debug("name : " + name);
        
        return JSyndicateFSJNI.jsyndicatefs_removexattr(path, name);
    }

    public static int jsyndicatefs_opendir(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_opendir");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_opendir(path, fi);
    }

    public static int jsyndicatefs_readdir(String path, JSFSFillDir filler, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_readdir");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_readdir(path, filler, offset, fi);
    }

    public static int jsyndicatefs_releasedir(String path, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_releasedir");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_releasedir(path, fi);
    }

    public static int jsyndicatefs_fsyncdir(String path, int datasync, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_fsyncdir");
        LOG.debug("path : " + path);
        LOG.debug("datasync : " + datasync);
        
        return JSyndicateFSJNI.jsyndicatefs_fsyncdir(path, datasync, fi);
    }

    public static int jsyndicatefs_access(String path, int mask) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_access");
        LOG.debug("path : " + path);
        LOG.debug("mask : " + mask);
        
        return JSyndicateFSJNI.jsyndicatefs_access(path, mask);
    }

    public static int jsyndicatefs_create(String path, int mode, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_create");
        LOG.debug("path : " + path);
        LOG.debug("mode : " + mode);
        
        return JSyndicateFSJNI.jsyndicatefs_create(path, mode, fi);
    }

    public static int jsyndicatefs_ftruncate(String path, long offset, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_ftruncate");
        LOG.debug("path : " + path);
        LOG.debug("offset : " + offset);
        
        return JSyndicateFSJNI.jsyndicatefs_ftruncate(path, offset, fi);
    }

    public static int jsyndicatefs_fgetattr(String path, JSFSStat statbuf, JSFSFileInfo fi) {
        checkLibraryLoaded();
        checkSyndicateInit();
        
        LOG.info("jsyndicatefs_fgetattr");
        LOG.debug("path : " + path);
        
        return JSyndicateFSJNI.jsyndicatefs_fgetattr(path, statbuf, fi);
    }
}
