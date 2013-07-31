/*
 * FileSystem class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.JSFSConfiguration;
import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSFilenameFilter;
import JSyndicateFS.JSFSPath;
import JSyndicateFS.JSFSPathFilter;
import JSyndicateFS.JSFSPendingExceptions;
import JSyndicateFS.JSFSRandomAccess;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPCFileSystem extends JSFSFileSystem {

    public static final Log LOG = LogFactory.getLog(IPCFileSystem.class);

    private IPCConfiguration configuration;
    private String UGName;
    private int UGPort;
    
    private ArrayList<IPCInputStream> openInputStream = new ArrayList<IPCInputStream>();
    private ArrayList<IPCOutputStream> openOutputStream = new ArrayList<IPCOutputStream>();
    private ArrayList<IPCRandomAccess> openRandomAccess = new ArrayList<IPCRandomAccess>();
    
    public IPCFileSystem(JSFSConfiguration conf) {
        LOG.info("Initialize FileSystem");
        
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        super.raiseOnBeforeCreateEvent(conf);
        
        this.configuration = (IPCConfiguration)conf;
        this.UGName = this.configuration.getUGName();
        this.UGPort = this.configuration.getPort();
        
        super.initialize(conf);
        
        super.raiseOnAfterCreateEvent();
    }
    
    @Override
    public boolean exists(JSFSPath path) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isDirectory(JSFSPath path) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isFile(JSFSPath path) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long getSize(JSFSPath path) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long getBlockSize() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void delete(JSFSPath path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void rename(JSFSPath path, JSFSPath newpath) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void mkdir(JSFSPath path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream getFileInputStream(JSFSPath path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public OutputStream getFileOutputStream(JSFSPath path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JSFSRandomAccess getRandomAccess(JSFSPath path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String[] readDirectoryEntryNames(JSFSPath path, JSFSFilenameFilter filter) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String[] readDirectoryEntryNames(JSFSPath path, JSFSPathFilter filter) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    void notifyClosed(IPCInputStream inputStream) {
        this.openInputStream.remove(inputStream);
    }
    
    void notifyClosed(IPCOutputStream outputStream) {
        this.openOutputStream.remove(outputStream);
    }
    
    void notifyClosed(IPCRandomAccess raf) {
        this.openRandomAccess.remove(raf);
    }
    
    @Override
    public synchronized void close() throws IOException {
        LOG.info("Close FileSystem");
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        // call handler
        super.raiseOnBeforeDestroyEvent();
        
        // close all open files
        JSFSPendingExceptions pe = new JSFSPendingExceptions();
        for(IPCInputStream is : openInputStream) {
            try {
                is.close();
            } catch (IOException ex) {
                pe.add(ex);
            }
        }
        
        for(IPCOutputStream os : openOutputStream) {
            try {
                os.close();
            } catch (IOException ex) {
                pe.add(ex);
            }
        }
        
        for(IPCRandomAccess raf : openRandomAccess) {
            try {
                raf.close();
            } catch (IOException ex) {
                pe.add(ex);
            }
        }
        
        super.close();
        
        super.raiseOnAfterDestroyEvent(this.conf);
        
        if(!pe.isEmpty()) {
            throw new IOException(pe.getMessage());
        }
    }
}
