package JSyndicateFSJNI;

// this is the native interface to Syndicate

import JSyndicateFSJNI.struct.JSFSConfig;
import JSyndicateFSJNI.struct.JSFSFileInfo;
import JSyndicateFSJNI.struct.JSFSFillDir;
import JSyndicateFSJNI.struct.JSFSStat;
import JSyndicateFSJNI.struct.JSFSStatvfs;
import JSyndicateFSJNI.struct.JSFSUtimbuf;


public class JSyndicateFSJNI {

    public final static native int jsyndicatefs_init(JSFSConfig jarg1);

    public final static native int jsyndicatefs_destroy();

    public final static native int jsyndicatefs_getattr(String jarg1, JSFSStat jarg2);

    public final static native int jsyndicatefs_mknod(String jarg1, int jarg2, long jarg3);

    public final static native int jsyndicatefs_mkdir(String jarg1, int jarg2);

    public final static native int jsyndicatefs_unlink(String jarg1);

    public final static native int jsyndicatefs_rmdir(String jarg1);

    public final static native int jsyndicatefs_rename(String jarg1, String jarg2);

    public final static native int jsyndicatefs_chmod(String jarg1, int jarg2);

    public final static native int jsyndicatefs_truncate(String jarg1, long jarg2);

    public final static native int jsyndicatefs_utime(String jarg1, JSFSUtimbuf jarg2);

    public final static native int jsyndicatefs_open(String jarg1, JSFSFileInfo jarg2);

    public final static native int jsyndicatefs_read(String jarg1, byte[] jarg2, long jarg3, long jarg4, JSFSFileInfo jarg5);

    public final static native int jsyndicatefs_write(String jarg1, byte[] jarg2, long jarg3, long jarg4, JSFSFileInfo jarg5);

    public final static native int jsyndicatefs_statfs(String jarg1, JSFSStatvfs jarg2);

    public final static native int jsyndicatefs_flush(String jarg1, JSFSFileInfo jarg2);

    public final static native int jsyndicatefs_release(String jarg1, JSFSFileInfo jarg2);

    public final static native int jsyndicatefs_fsync(String jarg1, int jarg2, JSFSFileInfo jarg3);

    public final static native int jsyndicatefs_setxattr(String jarg1, String jarg2, byte[] jarg3, long jarg4, int jarg5);

    public final static native int jsyndicatefs_getxattr(String jarg1, String jarg2, byte[] jarg3, long jarg4);

    public final static native int jsyndicatefs_listxattr(String jarg1, byte[] jarg2, long jarg3);

    public final static native int jsyndicatefs_removexattr(String jarg1, String jarg2);

    public final static native int jsyndicatefs_opendir(String jarg1, JSFSFileInfo jarg2);

    public final static native int jsyndicatefs_readdir(String jarg1, JSFSFillDir jarg2, long jarg3, JSFSFileInfo jarg4);

    public final static native int jsyndicatefs_releasedir(String jarg1, JSFSFileInfo jarg2);

    public final static native int jsyndicatefs_fsyncdir(String jarg1, int jarg2, JSFSFileInfo jarg3);

    public final static native int jsyndicatefs_access(String jarg1, int jarg2);

    public final static native int jsyndicatefs_create(String jarg1, int jarg2, JSFSFileInfo jarg3);

    public final static native int jsyndicatefs_ftruncate(String jarg1, long jarg2, JSFSFileInfo jarg3);

    public final static native int jsyndicatefs_fgetattr(String jarg1, JSFSStat jarg2, JSFSFileInfo jarg3);
}
