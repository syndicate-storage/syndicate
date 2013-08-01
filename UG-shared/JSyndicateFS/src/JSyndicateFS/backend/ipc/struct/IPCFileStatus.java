/*
 * FileStatus class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc.struct;

import JSyndicateFS.JSFSPath;
import JSyndicateFS.backend.ipc.IPCFileSystem;
import JSyndicateFS.backend.ipc.message.IPCStat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPCFileStatus {

    private static final Log LOG = LogFactory.getLog(IPCFileStatus.class);
    
    private IPCFileSystem filesystem;
    private JSFSPath path;
    private IPCStat stat;
    private boolean dirty;
    private boolean sizeModified;
    private long localFileSize;

    public IPCFileStatus(IPCFileSystem fs, JSFSPath path, IPCStat stat) {
        this.filesystem = fs;
        this.path = path;
        this.stat = stat;
        this.dirty = false;
        this.sizeModified = false;
        this.localFileSize = 0;
    }
    
    public IPCFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    public JSFSPath getPath() {
        return this.path;
    }
    
    public boolean isDirectory() {
        if((this.stat.getMode() & IPCStat.S_IFDIR) == IPCStat.S_IFDIR)
            return true;
        return false;
    }

    public boolean isFile() {
        if((this.stat.getMode() & IPCStat.S_IFREG) == IPCStat.S_IFREG)
            return true;
        return false;
    }

    public long getSize() {
        if(this.sizeModified)
            return this.localFileSize;
        else
            return this.stat.getSize();
    }

    public long getBlockSize() {
        return this.stat.getBlksize();
    }

    public long getBlocks() {
        if(this.sizeModified) {
            long blockSize = this.stat.getBlocks();
            long blocks = this.localFileSize / blockSize;
            if(this.localFileSize % blockSize != 0)
                blocks++;
            return blocks;
        } else {
            return this.stat.getBlocks();
        }
    }

    public long getLastAccess() {
        return this.stat.getAtim();
    }

    public long getLastModification() {
        return this.stat.getMtim();
    }

    public boolean isDirty() {
        return this.dirty;
    }

    public void setDirty() {
        this.dirty = true;
    }

    public void notifySizeChanged(long size) {
        this.localFileSize = size;
        this.sizeModified = true;
    }
}
