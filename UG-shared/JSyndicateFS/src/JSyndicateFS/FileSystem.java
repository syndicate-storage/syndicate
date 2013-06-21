/*
 * FileSystem class for JSyndicateFS
 */
package JSyndicateFS;

import JSyndicateFS.cache.ICache;
import JSyndicateFS.cache.TimeoutCache;
import JSyndicateFSJNI.struct.JSFSConfig;
import JSyndicateFSJNI.struct.JSFSStat;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FileSystem implements Closeable {

    public static final Log LOG = LogFactory.getLog(FileSystem.class);
    private static final String FS_ROOT_PATH = "file:///";
    
    private static FileSystem fsInstance;
    
    private Configuration conf;
    private Path workingDir;
    
    private boolean closed = true;
    
    private ICache<Path, FileStatus> metadataCache;
    private Map<Long, File> openFiles = new Hashtable<Long, File>();
    private long lastFileID = 0;
    
    /*
     * Construct or Get FileSystem from configuration
     */
    public synchronized static FileSystem getInstance(Configuration conf) throws InstantiationException {
        if(fsInstance == null) {
            fsInstance = new FileSystem(conf);
        } else {
            LOG.info("Get FileSystem instance already created : " + conf.getUsername() + "," + conf.getVolumeName() + "," + conf.getMSUrl().toString());
        }
            
        return fsInstance;
    }
    
    /*
     * Construct FileSystem from configuration
     */
    protected FileSystem(Configuration conf) throws InstantiationException {
        initialize(conf);
    }
    
    private void initialize(Configuration conf) throws InstantiationException {
        if(conf == null)
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        
        LOG.info("Initialize FileSystem : " + conf.getUsername() + "," + conf.getVolumeName() + "," + conf.getMSUrl().toString());
        
        // set configuration unmodifiable
        conf.lock();
        
        // Convert Configuration to JSFSConfig
        JSFSConfig config = conf.getJSFSConfig();
        int ret = JSyndicateFSJNI.JSyndicateFS.jsyndicatefs_init(config);
        if(ret != 0) {
            throw new java.lang.InstantiationException("jsyndicatefs_init failed : " + ret);
        }
        
        this.conf = conf;
        
        this.workingDir = getRootPath();
        
        this.metadataCache = new TimeoutCache<Path, FileStatus>(conf.getMaxMetadataCacheSize(), conf.getCacheTimeoutSecond());
        
        this.closed = false;
    }
    
    /*
     * Return the root path of the filesystem
     */
    public Path getRootPath() {
        return new Path(FS_ROOT_PATH);
    }
    
    /*
     * Return the working directory of the filesystem
     */
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    /*
     * Set the working directory of the filesystem
     */
    public void setWorkingDirectory(Path path) {
        if(path == null)
            this.workingDir = new Path(FS_ROOT_PATH);
        else
            this.workingDir = path;
    }
    
    public synchronized void notifyFileOpen(File file) {
        if(file == null)
            throw new IllegalArgumentException("Can not add null file to the list");
        
        if(!this.openFiles.containsKey(file.getID()))
            this.openFiles.put(file.getID(), file);
    }
    
    public synchronized void notifyFileClose(File file) {
        if(file == null)
            throw new IllegalArgumentException("Can not add null file to the list");
        
        if(this.openFiles.containsKey(file.getID()))
            this.openFiles.remove(file.getID());
    }
    
    private void closeAllOpenFiles() throws PendingExceptions {
        PendingExceptions pe = new PendingExceptions();
        
        Collection<File> values = this.openFiles.values();
        for(File file : values) {
            if(file.isOpen()) {
                try {
                    file.close();
                } catch(IOException e) {
                    LOG.error("File close exception (Pending) : " + file.getPath().toString());
                    pe.add(e);
                }
            }
        }
        
        this.openFiles.clear();
        
        if(!pe.isEmpty())
            throw pe;
    }
    
    /*
     * Close and release all resources of the filesystem
     */
    @Override
    public void close() throws IOException {
        if(this.closed)
            throw new IOException("The filesystem is already closed");
        
        PendingExceptions pe = new PendingExceptions();
        
        // destroy all caches
        this.metadataCache.clear();
        
        // destroy all opened files
        try {
            closeAllOpenFiles();
        } catch (PendingExceptions ex) {
            pe.addAll(ex);
        }
        
        // destroy underlying syndicate
        int ret = JSyndicateFSJNI.JSyndicateFS.jsyndicatefs_destroy();
        if(ret != 0) {
            LOG.error("jsyndicatefs_destroy failed : " + ret);
            pe.add(new IOException("jsyndicatefs_destroy failed : " + ret));
        }
        
        this.closed = true;
        
        if(!pe.isEmpty()) {
            throw new IOException(pe.getMessage());
        }
    }
    
    public Path getAbsolutePath(Path path) {
        if(path == null)
            throw new IllegalArgumentException("Can not get absolute file path from null path");
        
        Path absolute = null;
        if(!path.isAbsolute()) {
            // start from working dir
            absolute = new Path(this.workingDir, path);
        } else {
            absolute = path;
        }
        return path;
    }
    
    /*
     * Return FileStatus from path
     */
    public FileStatus getFileStatus(Path path) throws FileNotFoundException, IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not get file status from null path");
        
        Path absolute = null;
        if(!path.isAbsolute()) {
            // start from working dir
            absolute = new Path(this.workingDir, path);
        } else {
            absolute = path;
        }
        
        // check filestatus cache
        FileStatus cachedFileStatus = this.metadataCache.get(absolute);
        if(cachedFileStatus != null) {
            // has cache
            return cachedFileStatus;
        }
        
        JSFSStat statbuf = new JSFSStat();
        int ret = JSyndicateFSJNI.JSyndicateFS.jsyndicatefs_getattr(absolute.getPath(), statbuf);
        if(ret != 0) {
            throw new IOException("jsyndicatefs_getattr failed : " + ret);
        }
        
        FileStatus status = new FileStatus(absolute, statbuf);
        
        // cache filestatus
        this.metadataCache.insert(absolute, status);
        
        return status;
    }
    
    /*
     * True if the path exists
     */
    public boolean exists(Path path) throws IOException {
        try {
            if(getFileStatus(path) == null)
                return false;
            else
                return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /*
     * True if the path is a directory
     */
    public boolean isDirectory(Path path) throws IOException {
        try {
            return getFileStatus(path).isDirectory();
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /*
     * True if the path is a regular file
     */
    public boolean isFile(Path path) throws IOException {
        try {
            return getFileStatus(path).isFile();
        } catch (FileNotFoundException e) {
            return false;
        }
    }
    
    public File getFile(Path path) throws FileNotFoundException, IOException {
        FileStatus status = getFileStatus(path);
        
        if(status == null)
            throw new FileNotFoundException("Can not find file information from the path");
        
        return new File(this, status);
    }

    public synchronized long generateFileID() {
        this.lastFileID++;
        return this.lastFileID;
    }
}
