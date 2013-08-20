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
    private boolean readonly = false;
    private boolean closed = true;
    
    public IPCFileHandle(IPCFileSystem fs, IPCInterfaceClient client, IPCFileStatus status, IPCFileInfo fi, boolean readonly) {
        this.filesystem = fs;
        this.client = client;
        this.status = status;
        this.fileinfo = fi;
        this.readonly = readonly;
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
    
    public int readFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        return this.client.readFileData(this.fileinfo, fileoffset, buffer, offset, size);
    }
    
    public void writeFileData(long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not write data to readonly handle");
        }
        
        this.client.writeFileData(this.fileinfo, fileoffset, buffer, offset, size);
        this.status.notifySizeChanged(fileoffset + size);
    }
    
    public void truncate(long fileoffset) throws IOException {
        if(this.readonly) {
            throw new IOException("Can not truncate data to readonly handle");
        }
        
        this.client.truncateFile(this.fileinfo, fileoffset);
        this.status.notifySizeChanged(fileoffset);
    }
    
    public boolean isOpen() {
        return !this.closed;
    }
    
    public void flush() throws IOException {
        if(this.readonly) {
            throw new IOException("Can not flush data to readonly handle");
        }
        
        this.client.flush(this.fileinfo);
    }
    
    public boolean isReadonly() {
        return this.readonly;
    }
    
    @Override
    public void close() throws IOException {
        this.client.closeFileHandle(this.fileinfo);
        this.closed = true;
        
        if(!this.readonly) {
            this.status.setDirty();
        }
    }
}
