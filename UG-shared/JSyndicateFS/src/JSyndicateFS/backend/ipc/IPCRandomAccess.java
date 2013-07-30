/*
 * RandomAccessFile class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.JSFSRandomAccess;
import JSyndicateFS.backend.ipc.struct.IPCFileHandle;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPCRandomAccess implements JSFSRandomAccess {
    
    public static final Log LOG = LogFactory.getLog(IPCRandomAccess.class);
    
    private IPCFileSystem filesystem;
    private IPCFileHandle handle;
    private long offset;
    
    public IPCRandomAccess(IPCFileSystem fs, IPCFileHandle handle) {
        this.filesystem = fs;
        this.handle = handle;
        
        this.offset = 0;
    }
    
    @Override
    public void close() throws IOException {
        this.handle.close();
        this.filesystem.notifyClosed(this);
    }

    @Override
    public int read() throws IOException {
        byte[] buffer = new byte[1];
        int read = this.handle.readFileData(buffer, 1, 0, this.offset);
        if(read != 1) {
            LOG.error("Read failed");
            throw new IOException("Read failed");
        }
        this.offset++;
        return buffer[0];
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        int read = this.handle.readFileData(bytes, bytes.length, 0, this.offset);
        this.offset += read;
        return read;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        int read = this.handle.readFileData(bytes, len, off, this.offset);
        this.offset += read;
        return read;
    }

    @Override
    public int skip(int n) throws IOException {
        long size = this.handle.getStatus().getSize();
        if(size > this.offset + n) {
            this.offset += n;
        } else {
            n = (int) (size - this.offset);
            this.offset += n;
        }
        return n;
    }

    @Override
    public long getFilePointer() throws IOException {
        return this.offset;
    }

    @Override
    public long length() throws IOException {
        return this.handle.getStatus().getSize();
    }

    @Override
    public void seek(long l) throws IOException {
        if(l < 0) {
            LOG.error("seek point can not be negative");
            throw new IOException("seek point can not be negative");
        }
        this.offset = l;
    }
}
