/*
 * File Handle class for JSyndicateFS
 */
package JSyndicateFS;

import JSyndicateFSJNI.struct.JSFSFileInfo;
import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FileHandle implements Closeable {

    public static final Log LOG = LogFactory.getLog(FileHandle.class);
    
    private static long currentIdGenerated = 1;
    
    private FileSystem filesystem;
    private FileStatus status;
    private boolean closed = true;
    private JSFSFileInfo fileinfo;
    private long id;
    
    private static long generateNewID() {
        currentIdGenerated++;
        return currentIdGenerated;
    }
    
    /*
     * Construct FileHandle from FileSystem and FileStatus
     */
    public FileHandle(FileSystem fs, FileStatus status, JSFSFileInfo fileinfo) throws IOException {
        if(fs == null)
            throw new IllegalArgumentException("Can not create FileHandle from null filesystem");
        if(status == null)
            throw new IllegalArgumentException("Can not create FileHandle from null status");
        if(fileinfo == null)
            throw new IllegalArgumentException("Can not create FileHandle from null fileinfo");
        
        this.filesystem = fs;
        this.status = status;
        this.fileinfo = fileinfo;
        this.id = generateNewID();
        
        this.closed = false;
    }
    
    /*
     * Return FileInfo
     */
    public JSFSFileInfo getFileInfo() {
        return this.fileinfo;
    }
    
    /*
     * Return handleID
     */
    public long getHandleID() {
        return this.id;
    }
    
    /*
     * Return Path of the file
     */
    public Path getPath() {
        return this.status.getPath();
    }
    
    /*
     * Return FileStatus of the file
     */
    public FileStatus getStatus() {
        return this.status;
    }
    
    /*
     * True if the file is open
     */
    public boolean isOpen() {
        if(this.status == null)
            return false;
        if(this.fileinfo == null)
            return false;
        
        return !this.closed;
    }
    
    @Override
    public void close() throws IOException {
        this.filesystem.closeFileHandle(this);
        
        this.fileinfo = null;
        this.closed = true;
    }
}
