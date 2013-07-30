/*
 * OutputStream class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.backend.ipc.struct.IPCFileHandle;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPCOutputStream extends OutputStream {

    public static final Log LOG = LogFactory.getLog(IPCOutputStream.class);
    
    private IPCFileSystem filesystem;
    private IPCFileHandle handle;
    private long offset;
    
    public IPCOutputStream(IPCFileSystem fs, IPCFileHandle handle) {
        this.filesystem = fs;
        this.handle = handle;
        
        this.offset = 0;
    }
    
    @Override
    public void write(int i) throws IOException {
        byte[] buffer = new byte[1];
        buffer[0] = (byte)i;
        
        this.handle.writeFileData(buffer, 1, 0, this.offset);
        this.offset++;
    }
    
    @Override
    public void write(byte[] bytes) throws IOException {
        this.handle.writeFileData(bytes, bytes.length, 0, this.offset);
        this.offset += bytes.length;
    }
    
    @Override
    public void write(byte[] bytes, int offset, int len) throws IOException {
        this.handle.writeFileData(bytes, len, offset, this.offset);
        this.offset += len;
    }
    
    @Override
    public void flush() throws IOException {
        this.handle.flush();
    }
    
    @Override
    public void close() throws IOException {
        this.handle.close();
        this.filesystem.notifyClosed(this);        
    }
}
