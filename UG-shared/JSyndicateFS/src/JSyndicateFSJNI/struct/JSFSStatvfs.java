/*
 * Statvfs class for JSyndicateFS
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public class JSFSStatvfs {
    
    private long f_bsize;
    private long f_frsize;
    private long f_blocks;
    private long f_bfree;
    private long f_bavail;
    private long f_files;
    private long f_ffree;
    private long f_favail;
    private long f_fsid;
    private long f_flag;
    private long f_namemax;
    
    public JSFSStatvfs() {
        this.f_bsize = -1;
        this.f_frsize = -1;
        this.f_blocks = -1;
        this.f_bfree = -1;
        this.f_bavail = -1;
        this.f_files = -1;
        this.f_ffree = -1;
        this.f_favail = -1;
        this.f_fsid = -1;
        this.f_flag = -1;
        this.f_namemax = -1;
    }
    
    public JSFSStatvfs(long f_bsize, long f_frsize, long f_blocks, long f_bfree, long f_bavail, long f_files, long f_ffree, long f_favail, long f_fsid, long f_flag, long f_namemax) {
        this.f_bsize = f_bsize;
        this.f_frsize = f_frsize;
        this.f_blocks = f_blocks;
        this.f_bfree = f_bfree;
        this.f_bavail = f_bavail;
        this.f_files = f_files;
        this.f_ffree = f_ffree;
        this.f_favail = f_favail;
        this.f_fsid = f_fsid;
        this.f_flag = f_flag;
        this.f_namemax = f_namemax;
    }

    /**
     * @return the f_bsize
     */
    public long getF_bsize() {
        return f_bsize;
    }

    /**
     * @param f_bsize the f_bsize to set
     */
    public void setF_bsize(long f_bsize) {
        this.f_bsize = f_bsize;
    }

    /**
     * @return the f_frsize
     */
    public long getF_frsize() {
        return f_frsize;
    }

    /**
     * @param f_frsize the f_frsize to set
     */
    public void setF_frsize(long f_frsize) {
        this.f_frsize = f_frsize;
    }

    /**
     * @return the f_blocks
     */
    public long getF_blocks() {
        return f_blocks;
    }

    /**
     * @param f_blocks the f_blocks to set
     */
    public void setF_blocks(long f_blocks) {
        this.f_blocks = f_blocks;
    }

    /**
     * @return the f_bfree
     */
    public long getF_bfree() {
        return f_bfree;
    }

    /**
     * @param f_bfree the f_bfree to set
     */
    public void setF_bfree(long f_bfree) {
        this.f_bfree = f_bfree;
    }

    /**
     * @return the f_bavail
     */
    public long getF_bavail() {
        return f_bavail;
    }

    /**
     * @param f_bavail the f_bavail to set
     */
    public void setF_bavail(long f_bavail) {
        this.f_bavail = f_bavail;
    }

    /**
     * @return the f_files
     */
    public long getF_files() {
        return f_files;
    }

    /**
     * @param f_files the f_files to set
     */
    public void setF_files(long f_files) {
        this.f_files = f_files;
    }

    /**
     * @return the f_ffree
     */
    public long getF_ffree() {
        return f_ffree;
    }

    /**
     * @param f_ffree the f_ffree to set
     */
    public void setF_ffree(long f_ffree) {
        this.f_ffree = f_ffree;
    }

    /**
     * @return the f_favail
     */
    public long getF_favail() {
        return f_favail;
    }

    /**
     * @param f_favail the f_favail to set
     */
    public void setF_favail(long f_favail) {
        this.f_favail = f_favail;
    }

    /**
     * @return the f_fsid
     */
    public long getF_fsid() {
        return f_fsid;
    }

    /**
     * @param f_fsid the f_fsid to set
     */
    public void setF_fsid(long f_fsid) {
        this.f_fsid = f_fsid;
    }

    /**
     * @return the f_flag
     */
    public long getF_flag() {
        return f_flag;
    }

    /**
     * @param f_flag the f_flag to set
     */
    public void setF_flag(long f_flag) {
        this.f_flag = f_flag;
    }

    /**
     * @return the f_namemax
     */
    public long getF_namemax() {
        return f_namemax;
    }

    /**
     * @param f_namemax the f_namemax to set
     */
    public void setF_namemax(long f_namemax) {
        this.f_namemax = f_namemax;
    }
}
