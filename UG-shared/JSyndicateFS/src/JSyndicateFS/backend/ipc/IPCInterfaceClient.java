/*
 * InterfaceClient class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.backend.ipc.message.IPCFileInfo;
import JSyndicateFS.backend.ipc.message.IPCMessageBuilder;
import JSyndicateFS.backend.ipc.message.IPCStat;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPCInterfaceClient implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(IPCInterfaceClient.class);
    
    private int port;
    private Socket clientSocket;
    //private InputStream socketInputStream;
    private DataInputStream socketDataInputStream;
    //private OutputStream socketOutputStream;
    private DataOutputStream socketDataOutputStream;
    
    public IPCInterfaceClient(int port) throws InstantiationException {
        this.port = port;
        
        try {
            this.clientSocket = new Socket("localhost", port);
            this.socketDataInputStream = new DataInputStream(this.clientSocket.getInputStream());
            this.socketDataOutputStream = new DataOutputStream(this.clientSocket.getOutputStream());
        } catch (UnknownHostException ex) {
            LOG.error(ex);
            throw new InstantiationException(ex.getMessage());
        } catch (IOException ex) {
            LOG.error(ex);
            throw new InstantiationException(ex.getMessage());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        this.socketDataInputStream.close();
        this.socketDataOutputStream.close();
        this.clientSocket.close();
    }

    public synchronized IPCStat getStat(String path) throws IOException {
        LOG.info("getStat : " + path);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_GET_STAT, path);
        // recv
        return IPCMessageBuilder.readStatMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_GET_STAT);
    }

    public synchronized void delete(String path) throws IOException {
        LOG.info("delete : " + path);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_DELETE, path);
        // recv
        IPCMessageBuilder.readResultMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_DELETE);
    }

    public synchronized void removeDirectory(String path) throws IOException {
        LOG.info("removeDirectory : " + path);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_REMOVE_DIRECTORY, path);
        // recv
        IPCMessageBuilder.readResultMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_REMOVE_DIRECTORY);
    }

    public synchronized void rename(String path, String newPath) throws IOException {
        LOG.info("rename : " + path + ", " + newPath);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_RENAME, path, newPath);
        // recv
        IPCMessageBuilder.readResultMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_RENAME);
    }

    public synchronized void mkdir(String path) throws IOException {
        LOG.info("mkdir : " + path);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_MKDIR, path);
        // recv
        IPCMessageBuilder.readResultMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_MKDIR);
    }

    public synchronized String[] readDirectoryEntries(String path) throws IOException {
        LOG.info("readDirectoryEntries : " + path);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_READ_DIRECTORY, path);
        // recv
        String[] entries = IPCMessageBuilder.readDirectoryMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_READ_DIRECTORY);
        List<String> entry_arr = new ArrayList<String>();
        for(String entry : entries) {
            if(!entry.equals(".") && !entry.equals("..")) {
                entry_arr.add(entry);    
            }
        }
        
        String[] new_entries = new String[entry_arr.size()];
        new_entries = entry_arr.toArray(new_entries);
        return new_entries;
    }

    public synchronized IPCFileInfo getFileHandle(String path) throws IOException {
        LOG.info("getFileHandle : " + path);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_GET_FILE_HANDLE, path);
        // recv
        IPCFileInfo fi = IPCMessageBuilder.readFileInfoMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_GET_FILE_HANDLE);
        LOG.debug("filehandle : " + fi.getFileHandle());
        return fi;
    }

    public synchronized IPCStat createNewFile(String path) throws IOException {
        LOG.info("createNewFile : " + path);
        
        // send
        IPCMessageBuilder.sendStringsMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_CREATE_NEW_FILE, path);
        // recv
        return IPCMessageBuilder.readStatMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_CREATE_NEW_FILE);
    }

    public synchronized int readFileData(IPCFileInfo fileinfo, long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        LOG.info("readFileData : " + fileoffset + ", " + offset + ", " + size);
        
        // send
        IPCMessageBuilder.sendFileReadMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_READ_FILEDATA, fileinfo, fileoffset, size);
        // recv
        return IPCMessageBuilder.readFileData(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_READ_FILEDATA, buffer, offset);
    }

    public synchronized void writeFileData(IPCFileInfo fileinfo, long fileoffset, byte[] buffer, int offset, int size) throws IOException {
        LOG.info("writeFileData : " + fileoffset + ", " + offset + ", " + size);
        
        // send
        IPCMessageBuilder.sendFileWriteMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_WRITE_FILEDATA, fileinfo, fileoffset, buffer, offset, size);
        // recv
        IPCMessageBuilder.readResultMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_WRITE_FILEDATA);
    }

    public synchronized void flush(IPCFileInfo fileinfo) throws IOException {
        LOG.info("flush");
        
        // send
        IPCMessageBuilder.sendFileInfoMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_FLUSH, fileinfo);
        // recv
        IPCMessageBuilder.readResultMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_FLUSH);
    }

    public synchronized void closeFileHandle(IPCFileInfo fileinfo) throws IOException {
        LOG.info("closeFileHandle");
        
        // send
        IPCMessageBuilder.sendFileInfoMessage(this.socketDataOutputStream, IPCMessageBuilder.IPCMessageOperations.OP_CLOSE_FILE_HANDLE, fileinfo);
        // recv
        IPCMessageBuilder.readResultMessage(this.socketDataInputStream, IPCMessageBuilder.IPCMessageOperations.OP_CLOSE_FILE_HANDLE);
    }
}
