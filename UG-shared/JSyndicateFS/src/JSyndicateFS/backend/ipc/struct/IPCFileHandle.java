/*
 * File Handle class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc.struct;

import JSyndicateFS.JSFSPath;
import JSyndicateFS.backend.ipc.IPCFileSystem;
import JSyndicateFS.backend.ipc.IPCInterfaceClient;
import JSyndicateFS.backend.ipc.message.IPCFileInfo;
import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPCFileHandle implements Closeable {
    private static final Log LOG = LogFactory.getLog(IPCFileHandle.class);

    private IPCFileSystem filesystem;
    private IPCInterfaceClient client;
    private IPCFileStatus status;
    private IPCFileInfo fileinfo;
    private boolean closed = true;
    
    public IPCFileHandle(IPCFileSystem fs, IPCInterfaceClient client, IPCFileStatus status, IPCFileInfo fi) {
        this.filesystem = fs;
        this.client = client;
        this.status = status;
        this.fileinfo = fi;
        this.closed = false;
    }
    
    public IPCFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public IPCInterfaceClient getClient() {
        return this.client;
    }
    
    public JSFSPath getPath() {
        return this.status.getPath();
    }
    
    public IPCFileStatus getStatus() {
        return this.status;
    }
    
    public int readFileData(byte[] buffer, int size, int offset, long fileoffset) throws IOException {
        return this.client.readFileData(this.fileinfo, buffer, size, offset, fileoffset);
    }
    
    public void writeFileData(byte[] buffer, int size, int offset, long fileoffset) throws IOException {
        this.client.writeFileData(this.fileinfo, buffer, size, offset, fileoffset);
    }
    
    public boolean isOpen() {
        return !this.closed;
    }
    
    public void flush() throws IOException {
        this.client.flush(this.fileinfo);
    }

    @Override
    public void close() throws IOException {
        this.client.closeFileHandle(this.fileinfo);
        this.closed = true;
    }
}
