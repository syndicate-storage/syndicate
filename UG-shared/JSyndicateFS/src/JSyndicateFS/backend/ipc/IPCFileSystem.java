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
import JSyndicateFS.backend.ipc.cache.ICache;
import JSyndicateFS.backend.ipc.cache.TimeoutCache;
import JSyndicateFS.backend.ipc.message.IPCFileInfo;
import JSyndicateFS.backend.ipc.message.IPCStat;
import JSyndicateFS.backend.ipc.struct.IPCFileHandle;
import JSyndicateFS.backend.ipc.struct.IPCFileStatus;
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
    private IPCInterfaceClient client;
    
    private ArrayList<IPCInputStream> openInputStream = new ArrayList<IPCInputStream>();
    private ArrayList<IPCOutputStream> openOutputStream = new ArrayList<IPCOutputStream>();
    private ArrayList<IPCRandomAccess> openRandomAccess = new ArrayList<IPCRandomAccess>();
    
    private ICache<JSFSPath, IPCFileStatus> filestatus_cache = new TimeoutCache<JSFSPath, IPCFileStatus>(0, 60);
    
    public IPCFileSystem(IPCConfiguration conf) {
        initialize(conf);
    }
    
    public IPCFileSystem(JSFSConfiguration conf) {
        initialize((IPCConfiguration)conf);
    }
    
    private void initialize(IPCConfiguration conf) {
        LOG.info("Initialize FileSystem");
        
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        super.raiseOnBeforeCreateEvent(conf);
        
        this.configuration = conf;
        this.UGName = this.configuration.getUGName();
        this.UGPort = this.configuration.getPort();
        this.client = new IPCInterfaceClient(this.UGName, this.UGPort);
        
        super.initialize(conf);
        
        super.raiseOnAfterCreateEvent();
    }
    
    private IPCFileStatus getFileStatus(JSFSPath abspath) {
        if(abspath == null) {
            LOG.error("Can not get FileStatus from null abspath");
            throw new IllegalArgumentException("Can not get FileStatus from null abspath");
        }
        
        // check memory cache
        IPCFileStatus cached_status = this.filestatus_cache.get(abspath);
        
        if(cached_status != null && !cached_status.isDirty())
            return cached_status;
        
        // check parent dir's FileStatus recursively
        if(abspath.getParent() != null) {
            IPCFileStatus parentStatus = getFileStatus(abspath.getParent());
            if(parentStatus == null) {
                LOG.error("parentStatus is null");
                return null;
            } 
            
            if(!parentStatus.isDirectory()) {
                LOG.error("parentStatus is not a directory");
                return null;
            }
        }
        
        // not in memory cache
        IPCStat stat = null;
        try {
            stat = client.getStat(abspath.getPath());
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
        
        IPCFileStatus status = new IPCFileStatus(this, abspath, stat);
        this.filestatus_cache.insert(abspath, status);
        
        return status;
    }
    
    private IPCFileHandle getFileHandle(IPCFileStatus status) throws IOException {
        if(status == null) {
            LOG.error("Can not get FileHandle from null status");
            throw new IllegalArgumentException("Can not get FileHandle from null status");
        }
        if(status.isDirty()) {
            LOG.error("Can not get FileHandle from dirty status");
            throw new IllegalArgumentException("Can not get FileHandle from dirty status");
        }
        
        IPCFileInfo fi = client.getFileHandle(status.getPath().getPath());
        if(fi == null) {
            LOG.error("Can not get file handle from status");
            throw new IOException("Can not get file handle from status");
        }
        
        return new IPCFileHandle(this, this.client, status, fi);
    }
    
    private IPCFileHandle createNewFile(JSFSPath abspath) throws IOException {
        if(abspath == null) {
            LOG.error("abspath is null");
            throw new IllegalArgumentException("abspath is null");
        }
        
        if(abspath.getParent() != null) {
            IPCFileStatus parent = getFileStatus(abspath.getParent());
            if(parent == null) {
                LOG.error("Parent directory does not exist");
                throw new IOException("Parent directory does not exist");
            }
            
            if(!parent.isDirectory()) {
                LOG.error("Parent directory does not exist");
                throw new IOException("Parent directory does not exist");
            }
        }
        
        IPCStat stat = client.createNewFile(abspath.getPath());
        if(stat == null) {
            LOG.error("Can not create a new file");
            throw new IOException("Can not create a new file");
        }
        
        IPCFileStatus status = new IPCFileStatus(this, abspath, stat);
        this.filestatus_cache.insert(abspath, status);
        
        return getFileHandle(status);
    }
    
    @Override
    public boolean exists(JSFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isDirectory(JSFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.isDirectory();
        }
        return false;
    }

    @Override
    public boolean isFile(JSFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.isFile();
        }
        return false;
    }

    @Override
    public long getSize(JSFSPath path) {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status != null) {
            return status.getSize();
        }
        return 0;
    }

    @Override
    public long getBlockSize() {
        IPCFileStatus status = getFileStatus(new JSFSPath("/"));
        if(status != null) {
            return status.getBlockSize();
        }
        return 0;
    }

    @Override
    public void delete(JSFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status == null)
            return;
        
        if(status.isFile()) {
            this.client.delete(absPath.getPath());   
        } else if(status.isDirectory()) {
            this.client.removeDirectory(absPath.getPath());
        } else {
            throw new IOException("Can not delete from unknown status");
        }
        
        this.filestatus_cache.invalidate(absPath);
    }

    @Override
    public void rename(JSFSPath path, JSFSPath newpath) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        if(newpath == null) {
            LOG.error("newpath is null");
            throw new IllegalArgumentException("newpath is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        JSFSPath absNewPath = getAbsolutePath(newpath);
        
        IPCFileStatus status = getFileStatus(absPath);
        IPCFileStatus newStatus = getFileStatus(absNewPath);
        IPCFileStatus newStatusParent = getFileStatus(absNewPath.getParent());
        
        if(status == null) {
            LOG.error("source file does not exist");
            throw new IOException("source file does not exist");
        }
        
        if(newStatus != null) {
            LOG.error("target file already exists");
            throw new IOException("target file already exists");
        }
        
        if(newStatusParent == null) {
            LOG.error("parent directory of target file does not exist");
            throw new IOException("parent directory of target file does not exist");
        }
        
        this.client.rename(absPath.getPath(), absNewPath.getPath());
        
        this.filestatus_cache.invalidate(absPath);
    }

    @Override
    public void mkdir(JSFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath.getParent());
        if(status == null) {
            LOG.error("parent directory does not exist");
            throw new IOException("parent directory does not exist");
        }
        this.client.mkdir(absPath.getPath());
    }

    @Override
    public InputStream getFileInputStream(JSFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status == null) {
            throw new IOException("Can not open the file to read");
        }
        
        IPCFileHandle handle = getFileHandle(status);
        if(handle == null) {
            throw new IOException("Can not open the file to read");
        }
        
        return new IPCInputStream(this, handle);
    }

    @Override
    public OutputStream getFileOutputStream(JSFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        
        IPCFileHandle handle = null;
        if(status == null) {
            // create new file
            handle = createNewFile(absPath);
        } else {
            handle = getFileHandle(status);
        }
        
        if(handle == null) {
            throw new IOException("Can not open the file to write");
        }
        
        return new IPCOutputStream(this, handle);
    }

    @Override
    public JSFSRandomAccess getRandomAccess(JSFSPath path) throws IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status == null) {
            throw new IOException("Can not open the file to read");
        }
        
        IPCFileHandle handle = getFileHandle(status);
        if(handle == null) {
            throw new IOException("Can not open the file to read");
        }
        
        return new IPCRandomAccess(this, handle);
    }

    @Override
    public String[] readDirectoryEntryNames(JSFSPath path, JSFSFilenameFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("directory does not exist");
            throw new IOException("directory does not exist");
        }
        
        String[] entries = this.client.readDirectoryEntries(absPath.getPath());
        
        if(filter == null) {
            return entries;
        } else {
            ArrayList<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                if(filter.accept(absPath, entry)) {
                    arr.add(entry);
                }
            }
            
            String[] entries_filtered = new String[arr.size()];
            entries_filtered = arr.toArray(entries_filtered);
            return entries_filtered;
        }
    }

    @Override
    public String[] readDirectoryEntryNames(JSFSPath path, JSFSPathFilter filter) throws FileNotFoundException, IOException {
        if(path == null) {
            LOG.error("path is null");
            throw new IllegalArgumentException("path is null");
        }
        
        JSFSPath absPath = getAbsolutePath(path);
        IPCFileStatus status = getFileStatus(absPath);
        if(status == null) {
            LOG.error("directory does not exist");
            throw new IOException("directory does not exist");
        }
        
        String[] entries = this.client.readDirectoryEntries(absPath.getPath());
        
        if(filter == null) {
            return entries;
        } else {
            ArrayList<String> arr = new ArrayList<String>();
            for(String entry : entries) {
                if(filter.accept(new JSFSPath(absPath, entry))) {
                    arr.add(entry);
                }
            }
            
            String[] entries_filtered = new String[arr.size()];
            entries_filtered = arr.toArray(entries_filtered);
            return entries_filtered;
        }
    }
    
    void notifyClosed(IPCInputStream inputStream) {
        if(inputStream == null) {
            LOG.error("inputStream is null");
            throw new IllegalArgumentException("inputStream is null");
        }
        
        this.openInputStream.remove(inputStream);
    }
    
    void notifyClosed(IPCOutputStream outputStream) {
        if(outputStream == null) {
            LOG.error("outputStream is null");
            throw new IllegalArgumentException("outputStream is null");
        }
        
        this.openOutputStream.remove(outputStream);
    }
    
    void notifyClosed(IPCRandomAccess raf) {
        if(raf == null) {
            LOG.error("RandomAccess is null");
            throw new IllegalArgumentException("RandomAccess is null");
        }
        
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
        
        this.filestatus_cache.clear();
        
        this.client.close();
        
        super.close();
        
        super.raiseOnAfterDestroyEvent(this.conf);
        
        if(!pe.isEmpty()) {
            throw new IOException(pe.getMessage());
        }
    }
}
