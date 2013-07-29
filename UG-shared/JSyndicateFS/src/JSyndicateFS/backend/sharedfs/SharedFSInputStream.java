/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS.backend.sharedfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author iychoi
 */
public class SharedFSInputStream extends FileInputStream {
    
    private SharedFSFileSystem filesystem;
    
    public SharedFSInputStream(SharedFSFileSystem fs, String name) throws FileNotFoundException {
        super(name);
        
        this.filesystem = fs;
    }
    
    public SharedFSInputStream(SharedFSFileSystem fs, File file) throws FileNotFoundException {
        super(file);
        
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        this.filesystem.notifyClosed(this);
    }
}
