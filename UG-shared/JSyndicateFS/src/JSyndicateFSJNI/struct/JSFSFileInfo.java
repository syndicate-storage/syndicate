/*
 * FileInfo class for JSyndicateFS
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public class JSFSFileInfo {
    private int flags;
    //private long fh_old;
    //private int writepage;
    private int direct_io;
    //private int keep_cache;
    //private int flush;
    //private int nonseekable;
    private /* uint64_t */ long fh;
    //private /* uint64_t */ long lock_owner;

    public JSFSFileInfo() {
        this.flags = 0;
        this.direct_io = 0;
        this.fh = 0;
    }
    
    /**
     * @return the flags
     */
    public int getFlags() {
        return flags;
    }

    /**
     * @param flags the flags to set
     */
    public void setFlags(int flags) {
        this.flags = flags;
    }

    /**
     * @return the direct_io
     */
    public int getDirect_io() {
        return direct_io;
    }

    /**
     * @param direct_io the direct_io to set
     */
    public void setDirect_io(int direct_io) {
        this.direct_io = direct_io;
    }

    /**
     * @return the fh
     */
    public long getFh() {
        return fh;
    }

    /**
     * @param fh the fh to set
     */
    public void setFh(long fh) {
        this.fh = fh;
    }
}
