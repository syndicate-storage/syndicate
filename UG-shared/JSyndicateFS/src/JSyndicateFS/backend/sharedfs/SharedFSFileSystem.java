/*
 * FileSystem class for JSyndicateFS with Shared FileSystem backend
 */
package JSyndicateFS.backend.sharedfs;

import JSyndicateFS.JSFSConfiguration;
import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSFilenameFilter;
import JSyndicateFS.JSFSPath;
import JSyndicateFS.JSFSPathFilter;
import JSyndicateFS.JSFSPendingExceptions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class SharedFSFileSystem extends JSFSFileSystem {

    public static final Log LOG = LogFactory.getLog(SharedFSFileSystem.class);
    
    private SharedFSConfiguration configuration;
    private File mountPoint;
    
    private ArrayList<SharedFSInputStream> openInputStream = new ArrayList<SharedFSInputStream>();
    private ArrayList<SharedFSOutputStream> openOutputStream = new ArrayList<SharedFSOutputStream>();
    private ArrayList<SharedFSRandomAccessFile> openRandomAccessFile = new ArrayList<SharedFSRandomAccessFile>();
    
    public SharedFSFileSystem(JSFSConfiguration conf) {
        LOG.info("Initialize FileSystem");
        
        if(conf == null) {
            LOG.error("FileSystem Initialize failed : configuration is null");
            throw new IllegalArgumentException("Can not initialize the filesystem from null configuration");
        }
        
        super.raiseOnBeforeCreateEvent(conf);
        
        this.configuration = (SharedFSConfiguration)conf;
        this.mountPoint = this.configuration.getMountPoint();
        
        super.initialize(conf);
        
        super.raiseOnAfterCreateEvent();
    }
    
    private String getLocalPath(JSFSPath path) {
        if(path == null)
            throw new IllegalArgumentException("Can not get LocalAbsolutePath from null path");
        
        JSFSPath absPath = getAbsolutePath(path);
        String absMountPath = this.mountPoint.getAbsolutePath();
        
        String filePath = absPath.getPath();
        
        if(!absMountPath.endsWith("/")) {
            if(filePath.startsWith("/")) {
                return absMountPath + filePath;
            } else {
                return absMountPath + "/" + filePath;
            }
        } else {
            if(filePath.startsWith("/")) {
                if(filePath.length() > 1)
                    return absMountPath + filePath.substring(1);
                else
                    return absMountPath;
            } else {
                return absMountPath + filePath;
            }
        }
    }
    
    
    @Override
    public boolean exists(JSFSPath path) {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.exists();
    }

    @Override
    public boolean isDirectory(JSFSPath path) {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.isDirectory();
    }

    @Override
    public boolean isFile(JSFSPath path) {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.isFile();
    }
    
    @Override
    public long getSize(JSFSPath path) {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.length();
    }

    @Override
    public long getBlockSize() {
        // default filesystem block size = 4KB
        return 1024 * 4;
    }

    @Override
    public void delete(JSFSPath path) throws IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        if(!file.delete()) {
            throw new IOException("Can not delete file : " + path.getPath());
        }
    }

    @Override
    public void rename(JSFSPath path, JSFSPath newpath) throws IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        String realToPath = getLocalPath(newpath);
        File destfile = new File(realToPath);
        if(!file.renameTo(destfile)) {
            throw new IOException("Can not rename file : " + path.getPath());
        }
    }

    @Override
    public void mkdir(JSFSPath path) throws IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        if(!file.mkdir()) {
            throw new IOException("Can not make directory : " + path.getPath());
        }
    }

    @Override
    public InputStream getFileInputStream(JSFSPath path) throws IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return new SharedFSInputStream(this, file);
    }

    @Override
    public OutputStream getFileOutputStream(JSFSPath path) throws IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return new SharedFSOutputStream(this, file);
    }
    
    @Override
    public RandomAccessFile getRandomAccessFile(JSFSPath path) throws IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return new SharedFSRandomAccessFile(this, file, "rw");
    }

    @Override
    public String[] readDirectoryEntryNames(final JSFSPath path, final JSFSFilenameFilter filter) throws FileNotFoundException, IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.list(new FilenameFilter() {

            @Override
            public boolean accept(File file, String string) {
                if(filter != null) {
                    return filter.accept(path, string);
                } else {
                    return true;
                }
            }
        });
    }

    @Override
    public String[] readDirectoryEntryNames(final JSFSPath path, final JSFSPathFilter filter) throws FileNotFoundException, IOException {
        String realPath = getLocalPath(path);
        File file = new File(realPath);
        return file.list(new FilenameFilter() {

            @Override
            public boolean accept(File file, String string) {
                if(filter != null) {
                    JSFSPath newPath = new JSFSPath(path, string);
                    return filter.accept(newPath);
                } else {
                    return true;
                }
            }
        });
    }

    void notifyClosed(SharedFSInputStream inputStream) {
        this.openInputStream.remove(inputStream);
    }
    
    void notifyClosed(SharedFSOutputStream outputStream) {
        this.openOutputStream.remove(outputStream);
    }
    
    void notifyClosed(SharedFSRandomAccessFile raf) {
        this.openRandomAccessFile.remove(raf);
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
        for(SharedFSInputStream is : openInputStream) {
            try {
                is.close();
            } catch (IOException ex) {
                pe.add(ex);
            }
        }
        
        for(SharedFSOutputStream os : openOutputStream) {
            try {
                os.close();
            } catch (IOException ex) {
                pe.add(ex);
            }
        }
        
        for(SharedFSRandomAccessFile raf : openRandomAccessFile) {
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
