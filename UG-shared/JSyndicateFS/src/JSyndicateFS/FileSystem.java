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
    
    private Configuration conf;
    private Path workingDir;
    
    private boolean closed = true;
    
    private ICache<Path, FileStatus> metadataCache;
    private Map<Long, FileHandle> openFileHandles = new Hashtable<Long, FileHandle>();
    
    /*
     * Construct or Get FileSystem from configuration
     */
    public synchronized static FileSystem getInstance(Configuration conf) throws InstantiationException {
        if(fsInstance == null) {
            fsInstance = new FileSystem(conf);
        } else {
            LOG.info("Get FileSystem instance already created : " + conf.getUGName() + "," + conf.getVolumeName() + "," + conf.getMSUrl().toString());
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
        LOG.info("Initialize FileSystem");
        
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        // set configuration unmodifiable
        conf.lock();
        
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
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    /*
     * Set the working directory of the filesystem
     */
    public void setWorkingDirectory(Path path) {
        LOG.info("setWorkingDirectory");
        
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
    
    private void closeAllOpenFiles() throws PendingExceptions {
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
    public void close() throws IOException {
        LOG.info("close");
        
        if(this.closed) {
            LOG.error("FileSystem is already closed");
            throw new IOException("The filesystem is already closed");
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
    public FileStatus getFileStatus(Path path) throws FileNotFoundException, IOException {
        LOG.info("getFileStatus");
        
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
                LOG.error("parent is not a directory : " + parentStatus.getPath().getPath());
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
    
    public void invalidateFileStatus(FileStatus status) {
        LOG.info("invalidateFileStatus");
        
        if(status == null)
            throw new IllegalArgumentException("Can not invalidate file status from null status");
        
        // invalidate cache
        LOG.debug("invalidate cache : " + status.getPath().getPath());
        this.metadataCache.invalidate(status.getPath());
        
        status.setDirty();
    }
    
    /*
     * True if the path exists
     */
    public boolean exists(Path path) {
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
    public FileHandle openFileHandle(FileStatus status) throws IOException {
        LOG.info("openFileHandle");
        
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
            LOG.error("status is unknown");
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
    public FileHandle openFileHandle(Path path) throws FileNotFoundException, IOException {
        FileStatus status = getFileStatus(path);
        
        if(status == null)
            throw new FileNotFoundException("Can not find file information from the path");
        
        return openFileHandle(status);
    }
    
    public void flushFileHandle(FileHandle filehandle) throws IOException {
        LOG.info("flushFileHandle");
        
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
    public void closeFileHandle(FileHandle filehandle) throws IOException {
        LOG.info("closeFileHandle");
        
        if(filehandle == null)
            throw new IllegalArgumentException("Can not close null filehandle");
        //if(filehandle.isDirty())
        //    throw new IOException("Can not close dirty file handle");
        
        LOG.debug("path : " + filehandle.getPath().getPath());
        
        if(filehandle.isOpen()) {
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
                LOG.error("status unknown");
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
    public InputStream getFileInputStream(FileStatus status) throws IOException {
        LOG.info("getFileInputStream");
        
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
    public OutputStream getFileOutputStream(FileStatus status) throws IOException {
        LOG.info("getFileOutputStream");
        
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
    
    public int readFileData(FileHandle filehandle, byte[] buffer, int size, long offset) throws IOException {
        LOG.info("readFileData");
        
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

    public void writeFileData(FileHandle filehandle, byte[] buffer, int size, long offset) throws IOException {
        LOG.info("writeFileData");
        
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
    
    public String[] readDirectoryEntries(Path path) throws FileNotFoundException, IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not read directory entries from null path");
            
        FileHandle filehandle = openFileHandle(path);
        if(filehandle == null) {
            LOG.error("null file handle");
            throw new IOException("Can not read directory entries from null file handle");
        }
        
        return readDirectoryEntries(filehandle);
    }
    
    public String[] readDirectoryEntries(FileStatus status) throws IOException {
        if(status == null)
            throw new IllegalArgumentException("Can not read directory entries from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not read directory entries from dirty status");
        
        FileHandle filehandle = openFileHandle(status);
        if(filehandle == null) {
            LOG.error("null file handle");
            throw new IOException("Can not read directory entries from null file handle");
        }
        
        return readDirectoryEntries(filehandle);
    }
    
    public String[] readDirectoryEntries(FileHandle filehandle) throws IOException {
        LOG.info("readDirectoryEntries");
        
        if(filehandle == null)
            throw new IllegalArgumentException("Can not read directory entries from null filehandle");
        if(filehandle.isDirty())
            throw new IllegalArgumentException("Can not read directory entries from dirty filehandle");

        LOG.debug("path : " + filehandle.getPath().getPath());
        
        DirFillerImpl filler = new DirFillerImpl();
        
        if(!filehandle.getStatus().isDirectory()) {
            LOG.error("filehandle is a file");
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
    
    public void delete(Path path) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not delete from null path");
        
        FileStatus status = getFileStatus(path);
        if(status == null) {
            LOG.error("Can not delete file from null file status");
            throw new IOException("Can not delete file from null file status");
        }
        
        delete(status);
    }
            
    public void delete(FileStatus status) throws IOException {
        LOG.info("delete");
        
        if(status == null)
            throw new IllegalArgumentException("Can not delete file from null status");
        if(status.isDirty())
            throw new IllegalArgumentException("Can not delete file from dirty status");

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
            LOG.error("unknown status");
            throw new IOException("Can not delete file from unknown status");
        }
        
        invalidateFileStatus(status);
    }

    public void rename(FileStatus status, Path newpath) throws IOException {
        LOG.info("rename");
        
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
                    LOG.error("Can not get file status of parent directory");
                    throw new IOException("Can not get file status of parent directory");
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

    public void mkdir(Path path) throws IOException {
        LOG.info("mkdir");
        
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

    public void mkdirs(Path path) throws IOException {
        LOG.info("mkdirs");
        
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

    public boolean createNewFile(Path path) throws IOException {
        LOG.info("createNewFile");
        
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
            return true;
        } else {
            return false;
        }
    }
    
    private ArrayList<Path> listAllFilesRecursive(Path absPath) throws IOException {
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
    
    public Path[] listAllFiles(Path path) throws IOException {
        LOG.info("listAllFiles");
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
     
        LOG.debug("path : " + path.getPath());
        
        Path absPath = getAbsolutePath(path);
        
        ArrayList<Path> result = listAllFilesRecursive(absPath);
        
        Path[] paths = new Path[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    private ArrayList<Path> listAllFilesRecursive(Path absPath, FilenameFilter filter) throws IOException {
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
    
    public Path[] listAllFiles(Path path, FilenameFilter filter) throws IOException {
        LOG.info("listAllFiles");
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
     
        LOG.debug("path : " + path);
        
        Path absPath = getAbsolutePath(path);
        
        ArrayList<Path> result = listAllFilesRecursive(absPath, filter);
        
        Path[] paths = new Path[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
}
