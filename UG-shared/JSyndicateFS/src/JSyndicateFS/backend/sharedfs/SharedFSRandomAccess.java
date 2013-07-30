/*
 * RandomAccessFile class for JSyndicateFS with Shared FileSystem backend
 */
package JSyndicateFS.backend.sharedfs;

import JSyndicateFS.JSFSRandomAccess;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author iychoi
 */
public class SharedFSRandomAccess implements JSFSRandomAccess {
    
    private RandomAccessFile raf;
    private SharedFSFileSystem filesystem;
    
    public SharedFSRandomAccess(SharedFSFileSystem fs, String name, String mode) throws FileNotFoundException {
        this.filesystem = fs;
        this.raf = new RandomAccessFile(name, mode);
    }
    
    public SharedFSRandomAccess(SharedFSFileSystem fs, File file, String mode) throws FileNotFoundException {
        this.raf = new RandomAccessFile(file, mode);
        
        this.filesystem = fs;
    }
    
    @Override
    public void close() throws IOException {
        this.raf.close();
        
        this.filesystem.notifyClosed(this);
    }

    @Override
    public int read() throws IOException {
        return this.raf.read();
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return this.raf.read(bytes);
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return this.raf.read(bytes, off, len);
    }

    @Override
    public int skip(int n) throws IOException {
        return this.raf.skipBytes(n);
    }

    @Override
    public long getFilePointer() throws IOException {
        return this.raf.getFilePointer();
    }

    @Override
    public long length() throws IOException {
        return this.raf.length();
    }

    @Override
    public void seek(long l) throws IOException {
        this.raf.seek(l);
    }
}
