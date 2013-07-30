/*
 * InputStream class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.JSFSPath;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author iychoi
 */
public class IPCInputStream extends InputStream {

    private IPCFileSystem filesystem;
    private String filename;
    
    public IPCInputStream(IPCFileSystem fs, String name) throws FileNotFoundException {
        this.filesystem = fs;
        this.filename = name;
    }
    
    public IPCInputStream(IPCFileSystem fs, JSFSPath path) throws FileNotFoundException {
        this.filesystem = fs;
        this.filename = path.getPath();
    }
    
    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public int read(byte[] bytes) throws IOException {
        return 0;
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return 0;
    }
    
    @Override
    public long skip(long n) throws IOException {
        return 0;
    }
    
    @Override
    public int available() throws IOException {
        return 0;
    }
    
    @Override
    public void close() throws IOException {
        this.filesystem.notifyClosed(this);
    }
    
    @Override
    public synchronized void mark(int readlimit) {
    }
    
    @Override
    public synchronized void reset() throws IOException {
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }
}
