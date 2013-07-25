/*
 * FileSystem class for JSyndicateFS
 */
package JSyndicateFS;

import JSyndicateFS.cache.ICache;
import JSyndicateFS.cache.TimeoutCache;
import JSyndicateFSJNI.JSyndicateFS;
import JSyndicateFSJNI.struct.JSFSConfig;
import JSyndicateFSJNI.struct.JSFSFileInfo;
import JSyndicateFSJNI.struct.JSFSStat;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FileSystem implements Closeable {

    public static final Log LOG = LogFactory.getLog(FileSystem.class);
    
    private static final String FS_ROOT_PATH = "file:///";
    private static final int DEFAULT_NEW_FILE_PERMISSION = 33204;
    private static final int DEFAULT_NEW_DIR_PERMISSION = 509;
    
    private static FileSystem fsInstance;
    private static ArrayList<FileSystemEventHandler> eventHandlers = new ArrayList<FileSystemEventHandler>();
    
    private Configuration conf;
    private Path workingDir;
    
    private boolean closed = true;
    
    private ICache<Path, FileStatus> metadataCache;
    private Map<Long, FileHandle> openFileHandles = new Hashtable<Long, FileHandle>();

    public static void init(Configuration conf) throws InstantiationException {
        if(fsInstance == null) {
            fsInstance = new FileSystem(conf);
        } else {
            LOG.info("Get FileSystem instance already created : " + conf.getUGName());
        }
    }
    
    /*
     * Construct or Get FileSystem from configuration
     */
    public synchronized static FileSystem getInstance() throws InstantiationException {
        if(fsInstance == null) {
            LOG.error("FileSystem is not initialized");
            throw new InstantiationException("FileSystem is not initialized");
        }
        return fsInstance;
    }
    
    public static void addEventHandler(FileSystemEventHandler handler) {
        if(handler == null) 
            throw new IllegalArgumentException("Cannot add null handler");
        
        eventHandlers.add(handler);
    }
    
    public static void removeEventHandler(FileSystemEventHandler handler) {
        if(handler == null) 
            throw new IllegalArgumentException("Cannot remove null handler");
        
        eventHandlers.remove(handler);
    }
    
    /*
     * Construct FileSystem from configuration
     */
    protected FileSystem(Configuration conf) throws InstantiationException {
        initialize(conf);
    }
    
    private void initialize(Configuration conf) throws InstantiationException {
        LOG.info("Initialize FileSystem");
        
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        // call handler
        for(FileSystemEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(this);
        }
        
        // set configuration unmodifiable
        conf.lock();
        
        if(conf.getNativeLibraryFile() != null) {
            JSyndicateFSJNI.JSyndicateFS.setLibraryPath(conf.getNativeLibraryFile().getAbsolutePath());
        }
        
        // Convert Configuration to JSFSConfig
        JSFSConfig config = conf.getJSFSConfig();
        int ret = JSyndicateFSJNI.JSyndicateFS.jsyndicatefs_init(config);
        if(ret != 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            LOG.error("FileSystem Initialize failed : " + errmsg);
            throw new java.lang.InstantiationException("jsyndicatefs_init failed : " + errmsg);
        }
        
        this.conf = conf;
        this.workingDir = getRootPath();
        this.metadataCache = new TimeoutCache<Path, FileStatus>(conf.getMaxMetadataCacheSize(), conf.getCacheTimeoutSecond());
        this.closed = false;
        
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("Runtime shutdown was detected");
                
                if(closed == false) {
                    try {
                        close();
                    } catch (IOException ex) {
                        LOG.error(ex);
                    }
                }
            }
        });
        
        // call handler
        for(FileSystemEventHandler handler : eventHandlers) {
            handler.onAfterCreate(this);
        }
    }
    
    public synchronized boolean isClosed() {
        return this.closed;
    }
    
    public Configuration getConfiguration() {
        return this.conf;
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
    public synchronized Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    /*
     * Set the working directory of the filesystem
     */
    public synchronized void setWorkingDirectory(Path path) {
        LOG.info("setWorkingDirectory");
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            LOG.debug("path : null -- set default");
            this.workingDir = new Path(FS_ROOT_PATH);
        } else {
            LOG.debug("path : " + path.getPath());
            if(path.isAbsolute()) {
                this.workingDir = new Path(FS_ROOT_PATH, path);
            }
        }
    }
    
    private synchronized void closeAllOpenFiles() throws PendingExceptions {
        LOG.info("closeAllOpenFiles");
        
        PendingExceptions pe = new PendingExceptions();
        
        Collection<FileHandle> values = this.openFileHandles.values();
        
        // This is necessary in order to avoid accessing value collection while openFileHandles is modifying
        FileHandle[] tempHandles = new FileHandle[values.size()];
        tempHandles = values.toArray(tempHandles);
        
        for(FileHandle handle : tempHandles) {
            LOG.debug("Close opened file handle : " + handle.getPath().getPath());
            
            try {
                closeFileHandle(handle);
            } catch(IOException e) {
                LOG.error("FileHandle close exception (Pending) : " + handle.getPath().toString());
                pe.add(e);
            }
        }
        
        this.openFileHandles.clear();
        
        if(!pe.isEmpty())
            throw pe;
    }
    
    /*
     * Close and release all resources of the filesystem
     */
    @Override
    public synchronized void close() throws IOException {
        LOG.info("close");
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        // call handler
        for(FileSystemEventHandler handler : eventHandlers) {
            handler.onBeforeDestroy(this);
        }
        
        PendingExceptions pe = new PendingExceptions();
        
        // destroy all opened files
        try {
            closeAllOpenFiles();
        } catch (PendingExceptions ex) {
            pe.addAll(ex);
        }
        
        // destroy underlying syndicate
        int ret = JSyndicateFSJNI.JSyndicateFS.jsyndicatefs_destroy();
        if(ret != 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            LOG.error("jsyndicatefs_destroy failed : " + errmsg);
            pe.add(new IOException("jsyndicatefs_destroy failed : " + errmsg));
        }
        
        this.closed = true;
        
        // destroy all caches
        this.metadataCache.clear();
        
        // call handler
        for(FileSystemEventHandler handler : eventHandlers) {
            handler.onAfterDestroy(this);
        }
        
        if(!pe.isEmpty()) {
            throw new IOException(pe.getMessage());
        }
    }
    
    /*
     * Return absolute path
     */
    public Path getAbsolutePath(Path path) {
        if(path == null)
            throw new IllegalArgumentException("Can not get absolute file path from null path");
        
        Path absolute;
        if(!path.isAbsolute()) {
            // start from working dir
            absolute = new Path(this.workingDir, path);
        } else {
            absolute = new Path(FS_ROOT_PATH, path);
        }
        
        return absolute;
    }
    
    /*
     * Return FileStatus from path
     */
    public synchronized FileStatus getFileStatus(Path path) throws FileNotFoundException, IOException {
        LOG.info("getFileStatus");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not get file status from null path");
        
        LOG.debug("path : " + path.getPath());
        
        Path absolute = getAbsolutePath(path);
        
        // check filestatus cache
        FileStatus cachedFileStatus = this.metadataCache.get(absolute);
        if(cachedFileStatus != null && !cachedFileStatus.isDirty()) {
            // has cache
            LOG.debug("Returning file status from cache : " + absolute.getPath());
            return cachedFileStatus;
        }
        
        // check parent dir's FileStatus recursively
        if(absolute.getParent() != null) {
            FileStatus parentStatus = getFileStatus(absolute.getParent());
            if(parentStatus == null) {
                LOG.error("parentStatus is null");
                return null;
            } 
            
            if(!parentStatus.isDirectory()) {
                LOG.error("Can not get file status of parent directory");
                throw new FileNotFoundException("Can not get file status of parent directory");
            }
        }
        
        JSFSStat statbuf = new JSFSStat();
        int ret = JSyndicateFSJNI.JSyndicateFS.jsyndicatefs_getattr(absolute.getPath(), statbuf);
        if(ret != 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            throw new IOException("jsyndicatefs_getattr failed : " + errmsg);
        }
        
        FileStatus status = new FileStatus(absolute, statbuf);
        
        // cache filestatus
        LOG.debug("insert status to cache : " + absolute.getPath());
        this.metadataCache.insert(absolute, status);
        
        return status;
    }
    
    public synchronized void invalidateFileStatus(Path path) {
        LOG.info("invalidateFileStatus");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not invalidate file status from null path");
        
        // invalidate cache
        LOG.debug("invalidate cache : " + path.getPath());
        this.metadataCache.invalidate(path);
    }
    
    public synchronized void invalidateFileStatus(FileStatus status) {
        LOG.info("invalidateFileStatus");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not invalidate file status from null status");
        
        invalidateFileStatus(status.getPath());
        
        status.setDirty();
    }
    
    /*
     * True if the path exists
     */
    public boolean exists(Path path) {
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        try {
            if(getFileStatus(path) == null)
                return false;
            else
                return true;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException ex) {
            return false;
        }
    }

    /*
     * True if the path is a directory
     */
    public boolean isDirectory(Path path) {
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        try {
            return getFileStatus(path).isDirectory();
        } catch (FileNotFoundException ex) {
            return false;
        } catch (IOException ex) {
            return false;
        }
    }

    /*
     * True if the path is a regular file
     */
    public boolean isFile(Path path) {
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        try {
            return getFileStatus(path).isFile();
        } catch (FileNotFoundException ex) {
            return false;
        } catch (IOException ex) {
            return false;
        }
    }
    
    /*
     * Return the file handle from file status
     */
    synchronized FileHandle openFileHandle(FileStatus status) throws IOException {
        LOG.info("openFileHandle");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not open file handle from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not open file handle from dirty status");
        
        LOG.debug("path : " + status.getPath().getPath());
        
        JSFSFileInfo fileinfo = new JSFSFileInfo();
        
        if(status.isFile()) {
            int ret = JSyndicateFS.jsyndicatefs_open(status.getPath().getPath(), fileinfo);
            if(ret != 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_open failed : " + errmsg);
                throw new IOException("jsyndicatefs_open failed : " + errmsg);
            }
        } else if(status.isDirectory()) {
            int ret = JSyndicateFS.jsyndicatefs_opendir(status.getPath().getPath(), fileinfo);
            if(ret != 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_opendir failed : " + errmsg);
                throw new IOException("jsyndicatefs_opendir failed : " + errmsg);
            }
        } else {
            LOG.error("Can not open file handle from unknown status");
            throw new IOException("Can not open file handle from unknown status");
        }
        
        FileHandle filehandle = new FileHandle(this, status, fileinfo);

        // add to list
        if(!this.openFileHandles.containsKey(filehandle.getHandleID()))
            this.openFileHandles.put(filehandle.getHandleID(), filehandle);
        
        return filehandle;
    }
    
    /*
     * Return the file handle from path
     */
    synchronized FileHandle openFileHandle(Path path) throws FileNotFoundException, IOException {
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        FileStatus status = getFileStatus(path);
        
        if(status == null)
            throw new FileNotFoundException("Can not find file information from the path");
        
        return openFileHandle(status);
    }
    
    synchronized void flushFileHandle(FileHandle filehandle) throws IOException {
        LOG.info("flushFileHandle");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(filehandle == null)
            throw new IllegalArgumentException("Can not flush null filehandle");
        if(filehandle.isDirty())
            throw new IOException("Can not flush dirty file handle");
        
        LOG.debug("path : " + filehandle.getPath().getPath());
        
        if(filehandle.isOpen()) {
            int ret = JSyndicateFS.jsyndicatefs_flush(filehandle.getStatus().getPath().getPath(), filehandle.getFileInfo());
            if(ret != 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_flush failed : " + errmsg);
                throw new IOException("jsyndicatefs_flush failed : " + errmsg);
            }
        }
    }
    
    /*
     * Close file handle
     */
    synchronized void closeFileHandle(FileHandle filehandle) throws IOException {
        LOG.info("closeFileHandle");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(filehandle == null)
            throw new IllegalArgumentException("Can not close null filehandle");
        //if(filehandle.isDirty())
        //    throw new IOException("Can not close dirty file handle");
        
        LOG.debug("path : " + filehandle.getPath().getPath());
        
        if(filehandle.isOpen() && !filehandle.isDirty()) {
            FileStatus status = filehandle.getStatus();
                
            if(status.isFile()) {
                int ret = JSyndicateFS.jsyndicatefs_release(filehandle.getStatus().getPath().getPath(), filehandle.getFileInfo());
                if(ret != 0) {
                    String errmsg = ErrorUtils.generateErrorMessage(ret);
                    LOG.error("jsyndicatefs_release failed : " + errmsg);
                    throw new IOException("jsyndicatefs_release failed : " + errmsg);
                }
            } else if(status.isDirectory()) {
                int ret = JSyndicateFS.jsyndicatefs_releasedir(filehandle.getStatus().getPath().getPath(), filehandle.getFileInfo());
                if(ret != 0) {
                    String errmsg = ErrorUtils.generateErrorMessage(ret);
                    LOG.error("jsyndicatefs_release failed : " + errmsg);
                    throw new IOException("jsyndicatefs_releasedir failed : " + errmsg);
                }
            } else {
                LOG.error("Can not close file handle from unknown status");
                throw new IOException("Can not close file handle from unknown status");
            }
        }
        
        // notify object is closed
        filehandle.notifyClose();
        
        if(this.openFileHandles.containsKey(filehandle.getHandleID()))
            this.openFileHandles.remove(filehandle.getHandleID());
    }

    /*
     * Return input stream from file status
     */
    public synchronized InputStream getFileInputStream(FileStatus status) throws IOException {
        LOG.info("getFileInputStream");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not open file handle from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not open file handle from dirty status");
        if(!status.isFile())
            throw new IllegalArgumentException("Can not open file handle from status that is not a file");
        
        LOG.debug("path : " + status.getPath().getPath());
        
        FileHandle filehandle = openFileHandle(status);
        return new FSInputStream(filehandle);
    }
    
    /*
     * Return output stream frmo file status
     */
    public synchronized OutputStream getFileOutputStream(FileStatus status) throws IOException {
        LOG.info("getFileOutputStream");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not open file handle from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not open file handle from dirty status");
        if(!status.isFile())
            throw new IllegalArgumentException("Can not open file handle from status that is not a file");
        
        LOG.debug("path : " + status.getPath().getPath());
        
        FileHandle filehandle = openFileHandle(status);
        return new FSOutputStream(filehandle);
    }
    
    synchronized int readFileData(FileHandle filehandle, byte[] buffer, int size, long offset) throws IOException {
        LOG.info("readFileData");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(filehandle == null)
            throw new IllegalArgumentException("Can not read from null filehandle");
        if(!filehandle.isOpen())
            throw new IllegalArgumentException("Can not read from closed filehandle");
        if(filehandle.isDirty())
            throw new IllegalArgumentException("Can not read from dirty filehandle");
        if(!filehandle.getStatus().isFile())
            throw new IllegalArgumentException("Can not read from filehandle that is not a file");
        if(buffer == null)
            throw new IllegalArgumentException("Can not read to null buffer");
        if(buffer.length < size)
            throw new IllegalArgumentException("Can not read to too small buffer");
        if(size <= 0)
            throw new IllegalArgumentException("Can not read negative size data");
        if(offset < 0)
            throw new IllegalArgumentException("Can not read negative offset");
        
        LOG.debug("path : " + filehandle.getPath().getPath());
        LOG.debug("size : " + size);
        LOG.debug("offset : " + offset);
        
        int ret = JSyndicateFS.jsyndicatefs_read(filehandle.getStatus().getPath().getPath(), buffer, size, offset, filehandle.getFileInfo());
        if(ret < 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            LOG.error("jsyndicatefs_read failed : " + errmsg);
            throw new IOException("jsyndicatefs_read failed : " + errmsg);
        }
        
        return ret;
    }

    synchronized void writeFileData(FileHandle filehandle, byte[] buffer, int size, long offset) throws IOException {
        LOG.info("writeFileData");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(filehandle == null)
            throw new IllegalArgumentException("Can not write to null filehandle");
        if(!filehandle.isOpen())
            throw new IllegalArgumentException("Can not write to closed filehandle");
        if(filehandle.isDirty())
            throw new IllegalArgumentException("Can not write to from dirty filehandle");
        if(!filehandle.getStatus().isFile())
            throw new IllegalArgumentException("Can not write to filehandle that is not a file");
        if(buffer == null)
            throw new IllegalArgumentException("Can not write null buffer");
        if(buffer.length < size)
            throw new IllegalArgumentException("Can not write too small buffer");
        if(size <= 0)
            throw new IllegalArgumentException("Can not write negative size data");
        if(offset < 0)
            throw new IllegalArgumentException("Can not write negative offset");
        
        LOG.debug("path : " + filehandle.getPath().getPath());
        LOG.debug("size : " + size);
        LOG.debug("offset : " + offset);
        
        int ret = JSyndicateFS.jsyndicatefs_write(filehandle.getStatus().getPath().getPath(), buffer, size, offset, filehandle.getFileInfo());
        if(ret < 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            LOG.error("jsyndicatefs_write failed : " + errmsg);
            throw new IOException("jsyndicatefs_write failed : " + errmsg);
        }
        
        if(ret < size) {
            // pending?
            LOG.error("unexpected return : " + ret);
            throw new IOException("unexpected return : " + ret);
        }
        
        // update file size temporarily
        if(filehandle.getStatus().getSize() < offset + size)
            filehandle.getStatus().setSize(offset + size);
    }
    
    public synchronized void truncateFile(Path path, long size) throws IOException {
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not truncate file from null path");
        if(size < 0)
            throw new IllegalArgumentException("Can not truncate file to negative size data");
        
        FileStatus status = getFileStatus(path);
        if(status == null) {
            LOG.error("Can not truncate file from null file status");
            throw new IOException("Can not truncate file from null file status");
        }
        
        truncateFile(status, size);
    }
    
    public synchronized void truncateFile(FileStatus status, long size) throws IOException {
        LOG.info("truncateFile");

        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not truncate file from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not truncate file from dirty status");
        if(size < 0)
            throw new IllegalArgumentException("Can not truncate file to negative size data");
        
        if(!status.isFile())
            throw new IOException("Can not truncate non-file");
        
        LOG.debug("path : " + status.getPath().getPath());
        LOG.debug("size : " + size);
        
        
        int ret = JSyndicateFS.jsyndicatefs_truncate(status.getPath().getPath(), size);
        if(ret < 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            LOG.error("jsyndicatefs_truncate failed : " + errmsg);
            throw new IOException("jsyndicatefs_truncate failed : " + errmsg);
        }
        
        // update file size temporarily
        status.setSize(size);
    }
    
    public synchronized String[] readDirectoryEntries(Path path) throws FileNotFoundException, IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not read directory entries from null path");
            
        FileStatus status = getFileStatus(path);
        if(status == null) {
            LOG.error("Can not read directory from null file status");
            throw new IOException("Can not read directory from null file status");
        }
        
        return readDirectoryEntries(status);
    }
    
    public synchronized String[] readDirectoryEntries(FileStatus status) throws IOException {
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not read directory entries from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not read directory entries from dirty status");
        
        if(!status.isDirectory()) {
            LOG.error("Can not read directory from a file");
            throw new IOException("Can not read directory from a file");
        }
        
        FileHandle filehandle = openFileHandle(status);
        if(filehandle == null) {
            LOG.error("Can not read directory entries from null file handle");
            throw new IOException("Can not read directory entries from null file handle");
        }
        
        String[] result = readDirectoryEntries(filehandle);
        filehandle.close();
        return result;
    }
    
    synchronized String[] readDirectoryEntries(FileHandle filehandle) throws IOException {
        LOG.info("readDirectoryEntries");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(filehandle == null)
            throw new IllegalArgumentException("Can not read directory entries from null filehandle");
        if(filehandle.isDirty())
            throw new IllegalArgumentException("Can not read directory entries from dirty filehandle");

        LOG.debug("path : " + filehandle.getPath().getPath());
        
        DirFillerImpl filler = new DirFillerImpl();
        
        if(!filehandle.getStatus().isDirectory()) {
            LOG.error("Can not read directory entries from filehandle that is not a directory");
            throw new IllegalArgumentException("Can not read directory entries from filehandle that is not a directory");
        }
        
        int ret = JSyndicateFS.jsyndicatefs_readdir(filehandle.getPath().getPath(), filler, 0, filehandle.getFileInfo());
        if(ret != 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            LOG.error("jsyndicatefs_readdir failed : " + errmsg);
            throw new IOException("jsyndicatefs_readdir failed : " + errmsg);
        }
        
        return filler.getEntryNames();
    }
    
    public synchronized void delete(Path path) throws IOException {
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not delete from null path");
        
        FileStatus status = getFileStatus(path);
        if(status == null) {
            LOG.error("Can not delete file from null file status");
            throw new IOException("Can not delete file from null file status");
        }
        
        delete(status);
    }
            
    public synchronized void delete(FileStatus status) throws IOException {
        LOG.info("delete");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not delete file from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not delete file from dirty status");
        
        if(status.getPath().getParent() == null)
            throw new IOException("Can not delete root dir");

        LOG.debug("path : " + status.getPath().getPath());
        
        if(status.isFile()) {
            int ret = JSyndicateFS.jsyndicatefs_unlink(status.getPath().getPath());
            if(ret < 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_unlink failed : " + errmsg);
                throw new IOException("jsyndicatefs_unlink failed : " + errmsg);
            }
        } else if(status.isDirectory()) {
            int ret = JSyndicateFS.jsyndicatefs_rmdir(status.getPath().getPath());
            if(ret < 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_rmdir failed : " + errmsg);
                throw new IOException("jsyndicatefs_rmdir failed : " + errmsg);
            }
        } else {
            LOG.error("Can not delete file from unknown status");
            throw new IOException("Can not delete file from unknown status");
        }
        
        invalidateFileStatus(status);
    }

    public synchronized void rename(FileStatus status, Path newpath) throws IOException {
        LOG.info("rename");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(status == null)
            throw new IllegalArgumentException("Can not rename file from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not rename file from dirty status");
        if(newpath == null)
            throw new IllegalArgumentException("Can not rename file to null new name");
        
        LOG.debug("path : " + status.getPath().getPath());
        LOG.debug("new path : " + newpath.getPath());
        
        if(status.isFile()) {
            
            Path absNewPath = getAbsolutePath(newpath);
            
            // check parent dir's FileStatus
            if(absNewPath.getParent() != null) {
                FileStatus parentStatus = getFileStatus(absNewPath.getParent());
                if(parentStatus == null) {
                    LOG.error("Can not move the file to non-exist directory");
                    throw new IOException("Can not move the file to non-exist directory");
                }
                
                if(!parentStatus.isDirectory()) {
                    LOG.error("Can not get file status of target parent directory");
                    throw new IOException("Can not get file status of target parent directory");
                }
            }
            
            int ret = JSyndicateFS.jsyndicatefs_rename(status.getPath().getPath(), absNewPath.getPath());
            if(ret < 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_rename failed : " + errmsg);
                throw new IOException("jsyndicatefs_rename failed : " + errmsg);
            }
        } else if(status.isDirectory()) {
            LOG.error("Can not rename directory - not supported yet");
            throw new IOException("Can not rename directory - not supported yet");
        } else {
            LOG.error("Can not rename file from unknown status");
            throw new IOException("Can not rename file from unknown status");
        }
        
        invalidateFileStatus(status);
    }

    public synchronized void mkdir(Path path) throws IOException {
        LOG.info("mkdir");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not create a new directory from null path");
        
        LOG.debug("path : " + path.getPath());
        
        Path absPath = getAbsolutePath(path);
        
        // check parent dir's FileStatus
        if(absPath.getParent() != null) {
            FileStatus parentStatus = getFileStatus(absPath.getParent());
            if(parentStatus == null) {
                LOG.error("Can not create a new directory without existing parent directory");
                throw new IOException("Can not create a new directory without existing parent directory");
            }
            
            if(!parentStatus.isDirectory()) {
                LOG.error("Can not get file status of parent directory");
                throw new IOException("Can not get file status of parent directory");
            }
        }
        
        int ret = JSyndicateFS.jsyndicatefs_mkdir(absPath.getPath(), DEFAULT_NEW_DIR_PERMISSION);
        if(ret < 0) {
            String errmsg = ErrorUtils.generateErrorMessage(ret);
            LOG.error("jsyndicatefs_mkdir failed : " + errmsg);
            throw new IOException("jsyndicatefs_mkdir failed : " + errmsg);
        }
    }

    public synchronized void mkdirs(Path path) throws IOException {
        LOG.info("mkdirs");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not create a new directory from null path");
        
        LOG.debug("path : " + path.getPath());
        
        Path absPath = getAbsolutePath(path);
        
        Path[] ancestors = absPath.getAncestors();
        if(ancestors != null) {
            for(Path ancestor : ancestors) {
                if(!exists(ancestor)) {
                    mkdir(ancestor);
                }
            }
        }
        
        if(!exists(absPath)) {
            mkdir(absPath);
        }
    }

    public synchronized boolean createNewFile(Path path) throws IOException {
        LOG.info("createNewFile");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can create new file from null path");
        
        LOG.debug("path : " + path.getPath());
        
        Path absPath = getAbsolutePath(path);
        
        // check parent dir's FileStatus
        if(absPath.getParent() != null) {
            FileStatus parentStatus = getFileStatus(absPath.getParent());
            if(parentStatus == null) {
                LOG.error("Can not create a file without existing parent directory");
                throw new IOException("Can not create a file without existing parent directory");
            }
            
            if(!parentStatus.isDirectory()) {
                LOG.error("Can not get file status of parent directory");
                throw new IOException("Can not get file status of parent directory");
            }
        }
        
        FileStatus status = null;
        try {
            status = getFileStatus(absPath);
        } catch(IOException ex) {}
        
        if(status == null) {
            JSFSFileInfo fi = new JSFSFileInfo();
            int ret = JSyndicateFS.jsyndicatefs_create(absPath.getPath(), DEFAULT_NEW_FILE_PERMISSION, fi);
            if(ret < 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_create failed : " + errmsg);
                throw new IOException("jsyndicatefs_create failed : " + errmsg);
            }
            
            ret = JSyndicateFS.jsyndicatefs_flush(absPath.getPath(), fi);
            if(ret < 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_flush failed : " + errmsg);
                throw new IOException("jsyndicatefs_flush failed : " + errmsg);
            }
            
            ret = JSyndicateFS.jsyndicatefs_release(absPath.getPath(), fi);
            if(ret < 0) {
                String errmsg = ErrorUtils.generateErrorMessage(ret);
                LOG.error("jsyndicatefs_release failed : " + errmsg);
                throw new IOException("jsyndicatefs_release failed : " + errmsg);
            }
            return true;
        } else {
            return false;
        }
    }
    
    private synchronized ArrayList<Path> listAllFilesRecursive(Path absPath) throws IOException {
        if(absPath == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        ArrayList<Path> result = new ArrayList<Path>();
        
        if(isFile(absPath)) {
            result.add(absPath);
        } else if(isDirectory(absPath)) {
            // entries
            String[] entries = readDirectoryEntries(absPath);
            
            if(entries != null) {
                for(String entry : entries) {
                    Path newEntryPath = new Path(absPath, entry);
                    ArrayList<Path> rec_result = listAllFilesRecursive(newEntryPath);
                    result.addAll(rec_result);
                }
            }
        }
        
        return result;
    }
    
    public synchronized Path[] listAllFiles(Path path) throws IOException {
        LOG.info("listAllFiles");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
     
        LOG.debug("path : " + path.getPath());
        
        Path absPath = getAbsolutePath(path);
        
        ArrayList<Path> result = listAllFilesRecursive(absPath);
        
        Path[] paths = new Path[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    private synchronized ArrayList<Path> listAllFilesRecursive(Path absPath, FilenameFilter filter) throws IOException {
        if(absPath == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        ArrayList<Path> result = new ArrayList<Path>();
        
        if(isFile(absPath)) {
            if(filter != null) {
                if(filter.accept(new File(this, absPath.getParent()), absPath.getName())) {
                    result.add(absPath);
                }
            } else {
                result.add(absPath);
            }
        } else if(isDirectory(absPath)) {
            // entries
            String[] entries = readDirectoryEntries(absPath);
            
            if(entries != null) {
                for(String entry : entries) {
                    Path newEntryPath = new Path(absPath, entry);
                    
                    if(filter != null) {
                        if(filter.accept(new File(this, absPath), entry)) {
                            ArrayList<Path> rec_result = listAllFilesRecursive(newEntryPath);
                            result.addAll(rec_result);
                        }
                    } else {
                        ArrayList<Path> rec_result = listAllFilesRecursive(newEntryPath);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    public synchronized Path[] listAllFiles(Path path, FilenameFilter filter) throws IOException {
        LOG.info("listAllFiles");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
     
        LOG.debug("path : " + path);
        
        Path absPath = getAbsolutePath(path);
        
        ArrayList<Path> result = listAllFilesRecursive(absPath, filter);
        
        Path[] paths = new Path[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    private synchronized void deleteAllRecursive(Path absPath) throws IOException {
        if(absPath == null)
            throw new IllegalArgumentException("Can not delete files from null path");
        
        if(isFile(absPath)) {
            // remove file
            LOG.debug("delete file : " + absPath.getPath());
            delete(absPath);
        } else if(isDirectory(absPath)) {
            // entries
            String[] entries = readDirectoryEntries(absPath);
            
            if(entries != null) {
                for(String entry : entries) {
                    Path newEntryPath = new Path(absPath, entry);
                    deleteAllRecursive(newEntryPath);
                }
            }
            
            // remove dir
            LOG.debug("delete directory : " + absPath.getPath());
            delete(absPath);
        }
    }
    
    public synchronized void deleteAll(Path path) throws IOException {
        LOG.info("deleteAll");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not remove from null path");
        
        LOG.debug("path : " + path);
        
        Path absPath = getAbsolutePath(path);
        
        deleteAllRecursive(absPath);
    }
}
