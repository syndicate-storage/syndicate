/*
 * File class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class File {
    
    public static final Log LOG = LogFactory.getLog(File.class);
    
    private FileSystem filesystem;
    private FileStatus status;
    private Path path;
    private boolean loadStatus = false;
    
    /*
     * Construct File from FileSystem and Path
     */
    public File(FileSystem fs, String path) {
        this(fs, new Path(path));
    }
    
    public File(FileSystem fs, Path path) {
        initialize(fs, path, null);
    }
    
    public File(FileSystem fs, FileStatus status) {
        initialize(fs, status.getPath(), status);
    }
    
    private void initialize(FileSystem fs, Path path, FileStatus status) {
        if(fs == null)
            throw new IllegalArgumentException("Can not create File from null filesystem");
        if(path == null)
            throw new IllegalArgumentException("Can not create File from null path");
        
        // status can be empty
        
        this.filesystem = fs;
        this.path = fs.getAbsolutePath(path);
        this.status = status;
        
        if(status != null) {
            this.loadStatus = true;
        }
    }
    
    private void loadStatus() {
        if(!this.loadStatus) {
            try {
                this.status = this.filesystem.getFileStatus(this.path);
            } catch (FileNotFoundException ex) {
                LOG.debug("Fail loading FileStatus : " + this.path.toString());
            } catch (IOException ex) {
                LOG.debug("Fail loading FileStatus : " + this.path.toString());
            }
            this.loadStatus = true;
        }
    }
    
    public Path getPath() {
        return this.path;
    }
    
    /*
     * True if the file exist
     */
    public boolean exist() {
        loadStatus();
        
        if(this.status == null)
            return false;
        
        return true;
    }
    
    /*
     * True if the file is a regular file
     */
    public boolean isFile() {
        loadStatus();
        
        if(this.status == null)
            return false;
        
        return this.status.isFile();
    }
    
    /*
     * True if the file is a directory
     */
    public boolean isDirectory() {
        loadStatus();
        
        if(this.status == null)
            return false;
        
        return this.status.isDirectory();
    }
    
    /*
     * Return the size in byte of this file
     */
    public long getSize() {
       loadStatus();
       
       if(this.status == null)
           return 0;
       
       return this.status.getSize();
    }
    
    /*
     * Return the last access time
     */
    public long getLastAccess() {
       loadStatus();
       
       if(this.status == null)
           return 0;
       
        return this.status.getLastAccess();
    }
    
    /*
     * Return the last modification time
     */
    public long getLastModification() {
       loadStatus();
       
       if(this.status == null)
           return 0;
       
        return this.status.getLastModification();
    }
    
    /*
     * Return the input stream of the file
     */
    public DataInputStream getInputStream() throws IOException {
        loadStatus();
        
        if(this.status == null)
           throw new IOException("Can not get input stream from null status");
        
        return this.filesystem.getFileInputStream(this.status);
    }
}
