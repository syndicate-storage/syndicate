/*
 * JSFSRandomAccess interface for JSyndicateFS
 */
package JSyndicateFS;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author iychoi
 */
public interface JSFSRandomAccess extends Closeable {
    
    public int read() throws IOException;
    
    public int read(byte[] bytes) throws IOException;
    
    public int read(byte[] bytes, int off, int len) throws IOException;
    
    public int skip(int n) throws IOException;
    
    public long getFilePointer() throws IOException;
    
    public long length() throws IOException;
    
    public void setLength(long l) throws IOException;
    
    public void write(int i) throws IOException;
    
    public void write(byte[] bytes) throws IOException;
    
    public void write(byte[] bytes, int i, int i1) throws IOException;
    
    public void flush() throws IOException;
    
    public void seek(long l) throws IOException;
    
    @Override
    public void close() throws IOException;
}
