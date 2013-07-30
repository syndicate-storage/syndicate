/*
 * InterfaceClient class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.backend.ipc.message.IPCFileInfo;
import JSyndicateFS.backend.ipc.message.IPCStat;
import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author iychoi
 */
public class IPCInterfaceClient implements Closeable {
    private String UGName;
    private int port;
    
    public IPCInterfaceClient(String UGName, int port) {
        this.UGName = UGName;
        this.port = port;
    }

    @Override
    public void close() throws IOException {
        
    }

    public IPCStat getStat(String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void delete(String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void removeDirectory(String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void rename(String path, String newPath) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void mkdir(String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String[] readDirectoryEntries(String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public IPCFileInfo getFileHandle(String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public IPCStat createNewFile(String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public int readFileData(IPCFileInfo fileinfo, byte[] buffer, int size, int offset, long fileoffset) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void writeFileData(IPCFileInfo fileinfo, byte[] buffer, int size, int offset, long fileoffset) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void flush(IPCFileInfo fileinfo) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void closeFileHandle(IPCFileInfo fileinfo) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
