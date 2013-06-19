/*
 * Utimbuf class for JSyndicateFS
 * - This class is used between JNI layers
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public class JSFSUtimbuf {
    
    private /* __time_t */ long actime;		/* Access time.  */
    private /* __time_t */ long modtime;	/* Modification time.  */
    
    public JSFSUtimbuf() {
        this.actime = -1;
        this.modtime = -1;
    }
    
    public JSFSUtimbuf(long actime, long modtime) {
        this.actime = actime;
        this.modtime = modtime;
    }

    /**
     * @return the actime
     */
    public long getActime() {
        return actime;
    }

    /**
     * @param actime the actime to set
     */
    public void setActime(long actime) {
        this.actime = actime;
    }

    /**
     * @return the modtime
     */
    public long getModtime() {
        return modtime;
    }

    /**
     * @param modtime the modtime to set
     */
    public void setModtime(long modtime) {
        this.modtime = modtime;
    }
}
