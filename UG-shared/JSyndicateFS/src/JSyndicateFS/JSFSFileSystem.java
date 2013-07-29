/*
 * FileSystem class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public abstract class JSFSFileSystem implements Closeable {
    
    public static final Log LOG = LogFactory.getLog(JSFSFileSystem.class);
    
    protected static final String FS_ROOT_PATH_STRING = "file:///";
    protected static final JSFSPath FS_ROOT_PATH = new JSFSPath(FS_ROOT_PATH_STRING);
    
    protected static ArrayList<JSFSFileSystemEventHandler> eventHandlers = new ArrayList<JSFSFileSystemEventHandler>();
    
    protected JSFSConfiguration conf;
    protected JSFSPath workingDir;
    
    protected boolean closed = true;
    
    public synchronized static JSFSFileSystem createInstance(JSFSConfiguration conf) throws InstantiationException {
        JSFSFileSystem instance = null;
        Class fs_class = conf.getFileSystemClass();
        Constructor<JSFSFileSystem> fs_constructor = null;

        try {
            Class[] argTypes = new Class[]{conf.getClass()};
            fs_constructor = fs_class.getConstructor(argTypes);
            instance = fs_constructor.newInstance(conf);
        } catch (NoSuchMethodException ex) {
            throw new InstantiationException(ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new InstantiationException(ex.getMessage());
        } catch (IllegalArgumentException ex) {
            throw new InstantiationException(ex.getMessage());
        } catch (InvocationTargetException ex) {
            throw new InstantiationException(ex.getMessage());
        }
        return instance;
    }
    
    public synchronized static void addEventHandler(JSFSFileSystemEventHandler eventHandler) {
        if(eventHandler == null) 
            throw new IllegalArgumentException("Cannot add null event handler");
        
        eventHandlers.add(eventHandler);
    }
    
    public synchronized static void removeEventHandler(JSFSFileSystemEventHandler eventHandler) {
        if(eventHandler == null) 
            throw new IllegalArgumentException("Cannot remove null event handler");
        
        eventHandlers.remove(eventHandler);
    }
    
    protected synchronized void raiseOnBeforeCreateEvent(JSFSConfiguration conf) {
        for(JSFSFileSystemEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(conf);
        }
    }
    
    protected synchronized void raiseOnAfterCreateEvent() {
        for(JSFSFileSystemEventHandler handler : eventHandlers) {
            handler.onAfterCreate(this);
        }
    }
    
    protected synchronized void raiseOnBeforeDestroyEvent() {
        for(JSFSFileSystemEventHandler handler : eventHandlers) {
            handler.onBeforeDestroy(this);
        }
    }
    
    protected synchronized void raiseOnAfterDestroyEvent(JSFSConfiguration conf) {
        for(JSFSFileSystemEventHandler handler : eventHandlers) {
            handler.onBeforeCreate(conf);
        }
    }
    
    protected void initialize(JSFSConfiguration conf) {
        // set configuration unmodifiable
        conf.lock();
        
        this.conf = conf;
        this.workingDir = getRootPath();
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
    }
    
    public synchronized boolean isClosed() {
        return this.closed;
    }
    
    public synchronized JSFSConfiguration getConfiguration() {
        return this.conf;
    }
    
    public synchronized JSFSPath getRootPath() {
        return FS_ROOT_PATH;
    }
    
    public synchronized JSFSPath getWorkingDirectory() {
        return this.workingDir;
    }
    
    public synchronized void setWorkingDirectory(JSFSPath path) {
        LOG.info("setWorkingDirectory");
        
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            LOG.debug("path : null -- set default");
            this.workingDir = FS_ROOT_PATH;
        } else {
            LOG.debug("path : " + path.getPath());
            if(path.isAbsolute()) {
                this.workingDir = new JSFSPath(FS_ROOT_PATH, path);
            }
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        this.closed = true;
    }
    
    public synchronized JSFSPath getAbsolutePath(JSFSPath path) {
        if(path == null)
            throw new IllegalArgumentException("Can not get absolute file path from null path");
        
        JSFSPath absolute;
        if(!path.isAbsolute()) {
            // start from working dir
            absolute = new JSFSPath(this.workingDir, path);
        } else {
            absolute = new JSFSPath(FS_ROOT_PATH, path);
        }
        
        return absolute;
    }
    
    public abstract boolean exists(JSFSPath path);
    
    public abstract boolean isDirectory(JSFSPath path);
            
    public abstract boolean isFile(JSFSPath path);
    
    public abstract long getSize(JSFSPath path);
    
    public abstract long getBlockSize();
    
    public abstract void delete(JSFSPath path) throws IOException;
    
    public abstract void rename(JSFSPath path, JSFSPath newpath) throws IOException;
    
    public abstract void mkdir(JSFSPath path) throws IOException;
    
    public synchronized void mkdirs(JSFSPath path) throws IOException {
        LOG.info("mkdirs");
        
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not create a new directory from null path");
        
        JSFSPath absPath = getAbsolutePath(path);
        
        JSFSPath[] ancestors = absPath.getAncestors();
        if(ancestors != null) {
            for(JSFSPath ancestor : ancestors) {
                if(!exists(ancestor)) {
                    mkdir(ancestor);
                }
            }
        }
        
        if(!exists(absPath)) {
            mkdir(absPath);
        }
    }
    
    public abstract InputStream getFileInputStream(JSFSPath path) throws IOException;
    
    public abstract OutputStream getFileOutputStream(JSFSPath path) throws IOException;
    
    public abstract RandomAccessFile getRandomAccessFile(JSFSPath path) throws IOException;
    
    public synchronized String[] readDirectoryEntryNames(JSFSPath path) throws FileNotFoundException, IOException {
        return readDirectoryEntryNames(path, (JSFSFilenameFilter)null);
    }
    
    public abstract String[] readDirectoryEntryNames(JSFSPath path, JSFSFilenameFilter filter) throws FileNotFoundException, IOException;
    
    public abstract String[] readDirectoryEntryNames(JSFSPath path, JSFSPathFilter filter) throws FileNotFoundException, IOException;
    
    public synchronized JSFSPath[] listAllFiles(JSFSPath path) throws IOException {
        return listAllFiles(path, (JSFSFilenameFilter)null);
    }
    
    public synchronized JSFSPath[] listAllFiles(JSFSPath path, JSFSFilenameFilter filter) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        JSFSPath absPath = getAbsolutePath(path);
        
        ArrayList<JSFSPath> result = listAllFilesRecursive(absPath, filter);
        
        JSFSPath[] paths = new JSFSPath[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    private synchronized ArrayList<JSFSPath> listAllFilesRecursive(JSFSPath path, JSFSFilenameFilter filter) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        ArrayList<JSFSPath> result = new ArrayList<JSFSPath>();
        
        if(isFile(path)) {
            if(filter != null) {
                if(filter.accept(path.getParent(), path.getName())) {
                    result.add(path);
                }
            } else {
                result.add(path);
            }
        } else if(isDirectory(path)) {
            // entries
            String[] entries = readDirectoryEntryNames(path, filter);
            
            if(entries != null) {
                for(String entry : entries) {
                    JSFSPath newEntryPath = new JSFSPath(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(path, entry)) {
                            ArrayList<JSFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        ArrayList<JSFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    public synchronized JSFSPath[] listAllFiles(JSFSPath path, JSFSPathFilter filter) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        JSFSPath absPath = getAbsolutePath(path);
        
        ArrayList<JSFSPath> result = listAllFilesRecursive(absPath, filter);
        
        JSFSPath[] paths = new JSFSPath[result.size()];
        paths = result.toArray(paths);
        return paths;
    }
    
    private synchronized ArrayList<JSFSPath> listAllFilesRecursive(JSFSPath path, JSFSPathFilter filter) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not list files from null path");
        
        ArrayList<JSFSPath> result = new ArrayList<JSFSPath>();
        
        if(isFile(path)) {
            if(filter != null) {
                if(filter.accept(path)) {
                    result.add(path);
                }
            } else {
                result.add(path);
            }
        } else if(isDirectory(path)) {
            // entries
            String[] entries = readDirectoryEntryNames(path, filter);
            
            if(entries != null) {
                for(String entry : entries) {
                    JSFSPath newEntryPath = new JSFSPath(path, entry);
                    
                    if(filter != null) {
                        if(filter.accept(newEntryPath)) {
                            ArrayList<JSFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                            result.addAll(rec_result);
                        }
                    } else {
                        ArrayList<JSFSPath> rec_result = listAllFilesRecursive(newEntryPath, filter);
                        result.addAll(rec_result);
                    }
                }
            }
        }
        
        return result;
    }
    
    public synchronized void deleteAll(JSFSPath path) throws IOException {
        if (this.closed) {
            LOG.error("filesystem is already closed");
            throw new IllegalStateException("filesystem is already closed");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("Can not remove from null path");
        }
        
        LOG.debug("path : " + path);
        
        JSFSPath absPath = getAbsolutePath(path);
        
        deleteAllRecursive(absPath);
    }
    
    private synchronized void deleteAllRecursive(JSFSPath path) throws IOException {
        if(path == null)
            throw new IllegalArgumentException("Can not delete files from null path");
        
        if(isFile(path)) {
            // remove file
            delete(path);
        } else if(isDirectory(path)) {
            // entries
            String[] entries = readDirectoryEntryNames(path);
            
            if(entries != null) {
                for(String entry : entries) {
                    JSFSPath newEntryPath = new JSFSPath(path, entry);
                    deleteAllRecursive(newEntryPath);
                }
            }
            
            // remove dir
            delete(path);
        }
    }
    
    @Override
    public String toString() {
        return "JSFSFileSystem with " + this.conf.getBackendName() + " backend";
    }
}
