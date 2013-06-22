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
    
    /*
     * Construct FileStatus
     */
    public FileStatus(Path path, JSFSStat statbuf) {
        if(path == null)
            throw new IllegalArgumentException("Can not create FileStatus from null Path");
        if(statbuf == null)
            throw new IllegalArgumentException("Can not create FileStatus from null JSFSStat");
            
        this.path = path;
        this.stat = statbuf;
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
        return this.stat.getSt_size();
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
}
