/*
 * OutputStream class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.JSFSPath;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author iychoi
 */
public class IPCOutputStream extends OutputStream {

    private IPCFileSystem filesystem;
    private String filename;
    
    public IPCOutputStream(IPCFileSystem fs, String name) throws FileNotFoundException {
        this.filesystem = fs;
        this.filename = name;
    }
    
    public IPCOutputStream(IPCFileSystem fs, JSFSPath path) throws FileNotFoundException {
        this.filesystem = fs;
        this.filename = path.getPath();
    }
    
    @Override
    public void write(int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void write(byte[] bytes) throws IOException {
        
    }
    
    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        
    }
    
    @Override
    public void flush() throws IOException {
        
    }
    
    @Override
    public void close() throws IOException {
        this.filesystem.notifyClosed(this);        
    }
}
