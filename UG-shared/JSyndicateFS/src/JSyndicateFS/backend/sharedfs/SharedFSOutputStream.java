/*
 * OutputStream class for JSyndicateFS with Shared FileSystem backend
 */
package JSyndicateFS.backend.sharedfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 *
 * @author iychoi
 */
public class SharedFSOutputStream extends FileOutputStream{
    
    private SharedFSFileSystem filesystem;
    
    public SharedFSOutputStream(SharedFSFileSystem fs, String name) throws FileNotFoundException {
        super(name);
        
        this.filesystem = fs;
    }
    
    public SharedFSOutputStream(SharedFSFileSystem fs, String name, boolean append) throws FileNotFoundException {
        super(name, append);
        
        this.filesystem = fs;
    }
    
    public SharedFSOutputStream(SharedFSFileSystem fs, File file) throws FileNotFoundException {
        super(file);
        
        this.filesystem = fs;
    }
    
    public SharedFSOutputStream(SharedFSFileSystem fs, File file, boolean append) throws FileNotFoundException {
        super(file, append);
        
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        this.filesystem.notifyClosed(this);
    }
}
