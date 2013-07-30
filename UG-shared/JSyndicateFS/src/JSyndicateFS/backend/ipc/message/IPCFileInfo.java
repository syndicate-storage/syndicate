/*
 * FileHandle class for JSyndicateFS with IPC backend
 */
package JSyndicateFS.backend.ipc.message;

/**
 *
 * @author iychoi
 */
public class IPCFileInfo {
    private /* uint64_t */ long handle;

    public IPCFileInfo() {
        this.handle = 0;
    }

    public long getFileHandle() {
        return handle;
    }

    public void setFileHandle(long handle) {
        this.handle = handle;
    }
}
