/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS.backend.sharedfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author iychoi
 */
public class SharedFSRandomAccessFile extends RandomAccessFile {
    
    private SharedFSFileSystem filesystem;
    
    public SharedFSRandomAccessFile(SharedFSFileSystem fs, String name, String mode) throws FileNotFoundException {
        super(name, mode);
        
        this.filesystem = fs;
    }
    
    public SharedFSRandomAccessFile(SharedFSFileSystem fs, File file, String mode) throws FileNotFoundException {
        super(file, mode);
        
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        this.filesystem.notifyClosed(this);
    }
}
