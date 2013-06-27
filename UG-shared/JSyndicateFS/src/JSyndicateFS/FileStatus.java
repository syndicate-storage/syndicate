/*
 * FileStatus class for JSyndicateFS
 */
package JSyndicateFS;

import JSyndicateFSJNI.struct.JSFSStat;

/**
 *
 * @author iychoi
 */
public class FileStatus {

    private Path path;
    private JSFSStat stat;
    private boolean dirty;
    private boolean sizeModified;
    private long localFileSize;
    
    /*
     * Construct FileStatus
     */
    FileStatus(Path path, JSFSStat statbuf) {
        if(path == null)
            throw new IllegalArgumentException("Can not create FileStatus from null Path");
        if(statbuf == null)
            throw new IllegalArgumentException("Can not create FileStatus from null JSFSStat");
            
        this.path = path;
        this.stat = statbuf;
        this.dirty = false;
        this.sizeModified = false;
        this.localFileSize = 0;
    }
    
    public Path getPath() {
        return this.path;
    }
    
    /*
     * Return True if this file is Directory
     */
    public boolean isDirectory() {
        if((this.stat.getSt_mode() & JSFSStat.S_IFDIR) == JSFSStat.S_IFDIR)
            return true;
        return false;   
    }

    /*
     * Return True if this file is File
     */
    public boolean isFile() {
        if((this.stat.getSt_mode() & JSFSStat.S_IFREG) == JSFSStat.S_IFREG)
            return true;
        return false;
    }
    
    /*
     * Return the size in byte of this file
     */
    public long getSize() {
        if(this.sizeModified)
            return this.localFileSize;
        else
            return this.stat.getSt_size();
    }
    
    void setSize(long size) {
        this.localFileSize = size;
        this.sizeModified = true;
    }
    
    /*
     * Return the last access time
     */
    public long getLastAccess() {
        return this.stat.getSt_atim();
    }
    
    /*
     * Return the last modification time
     */
    public long getLastModification() {
        return this.stat.getSt_mtim();
    }
    
    /*
     * Return True if data is modified after loaded
     */
    public boolean isDirty() {
        return this.dirty;
    }
    
    /*
     * Set this status is invalid
     */
    public void setDirty() {
        this.dirty = true;
    }
}
