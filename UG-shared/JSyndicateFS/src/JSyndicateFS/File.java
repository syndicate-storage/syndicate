/*
 * File class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class File {
    
    public static final Log LOG = LogFactory.getLog(File.class);
    
    private FileSystem filesystem;
    private FileStatus status;
    private Path path;
    private boolean loadStatus = false;
    
    /*
     * Construct File from FileSystem and Path
     */
    public File(FileSystem fs, String path) {
        this(fs, new Path(path));
    }
    
    public File(FileSystem fs, Path path) {
        initialize(fs, path, null);
    }
    
    public File(FileSystem fs, FileStatus status) {
        initialize(fs, status.getPath(), status);
    }
    
    private void initialize(FileSystem fs, Path path, FileStatus status) {
        if(fs == null)
            throw new IllegalArgumentException("Can not create File from null filesystem");
        if(path == null)
            throw new IllegalArgumentException("Can not create File from null path");
        
        // status can be empty
        
        this.filesystem = fs;
        this.path = fs.getAbsolutePath(path);
        this.status = status;
        
        if(status != null) {
            this.loadStatus = true;
        }
    }
    
    private void loadStatus() {
        if(!this.loadStatus) {
            try {
                this.status = this.filesystem.getFileStatus(this.path);
            } catch (FileNotFoundException ex) {
                LOG.debug("Fail loading FileStatus : " + this.path.toString());
            } catch (IOException ex) {
                LOG.debug("Fail loading FileStatus : " + this.path.toString());
            }
            this.loadStatus = true;
        }
    }
    
    private void reloadStatus() {
        this.loadStatus = false;
        loadStatus();
    }
    
    /*
     * Return FileSystem of the file
     */
    public FileSystem getFileSystem() {
        return this.filesystem;
    }
    
    /*
     * Return Path of the file
     */
    public Path getPath() {
        return this.path;
    }
    
    /*
     * Return the name of the file
     */
    public String getName() {
        return this.path.getName();
    }
    
    /*
     * Return the path string of parent of the file
     */
    public String getParent() {
        return this.path.getParent().getPath();
    }
    
    public File getParentFile() {
        return new File(this.filesystem, this.path.getParent());
    }
    
    /*
     * True if the file exist
     */
    public boolean exist() {
        loadStatus();
        
        if(this.status == null)
            return false;
        
        return true;
    }
    
    /*
     * True if the file is a regular file
     */
    public boolean isFile() {
        loadStatus();
        
        if(this.status == null)
            return false;
        
        return this.status.isFile();
    }
    
    /*
     * True if the file is a directory
     */
    public boolean isDirectory() {
        loadStatus();
        
        if(this.status == null)
            return false;
        
        return this.status.isDirectory();
    }
    
    /*
     * Return the directory entries of the file
     */
    public String[] list() {
        loadStatus();
        
        if(this.status == null)
           return null;
        
        if(!this.status.isDirectory())
           return null;
        
        try {
            return this.filesystem.readDirectoryEntries(this.status);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    /*
     * Return the directory entries of the file
     */
    public String[] list(FilenameFilter filter) {
        if(filter == null)
            throw new IllegalArgumentException("Can not filter files from null filter");
        
        String[] entries = list();
        
        if(entries == null)
            return null;
        
        ArrayList<String> files = new ArrayList<String>();
            
        for(int i=0;i<entries.length;i++) {
            if(filter.accept(this, entries[i])) {
                files.add(entries[i]);
            }
        }

        String[] files_arr = new String[files.size()];
        files_arr = files.toArray(files_arr);

        return files_arr;
    }

    /*
     * Return an array of abstract pathnames denoting the files in the directory denoted by this abstract pathname. 
     */
    public File[] listFiles() {
        String[] entries = list();
        
        if(entries != null) {
            File[] files = new File[entries.length];
            
            for(int i=0;i<entries.length;i++) {
                Path absPath = new Path(this.path, entries[i]);
                
                files[i] = new File(this.filesystem, absPath);
            }
            return files;
        } else {
            return null;
        }
    }

    /*
     * Returns an array of abstract pathnames denoting the files and directories in the directory denoted by this abstract pathname that satisfy the specified filter. 
     */
    public File[] listFiles(FileFilter filter) {
        if(filter == null)
            throw new IllegalArgumentException("Can not filter files from null filter");
        
        String[] entries = list();
        
        if(entries != null) {
            ArrayList<File> files = new ArrayList<File>();
            
            for(int i=0;i<entries.length;i++) {
                Path absPath = new Path(this.path, entries[i]);
                
                File file = new File(this.filesystem, absPath);
                
                if(filter.accept(file)) {
                    files.add(file);
                }
            }
            
            File[] files_arr = new File[files.size()];
            files_arr = files.toArray(files_arr);
            
            return files_arr;
        } else {
            return null;
        }
    }
    
    /*
     * Returns an array of abstract pathnames denoting the files and directories in the directory denoted by this abstract pathname that satisfy the specified filter. 
     */
    public File[] listFiles(FilenameFilter filter) {
        if(filter == null)
            throw new IllegalArgumentException("Can not filter files from null filter");
        
        String[] entries = list();
        
        if(entries != null) {
            ArrayList<File> files = new ArrayList<File>();
            
            for(int i=0;i<entries.length;i++) {
                
                if(filter.accept(this, entries[i])) {
                    Path absPath = new Path(this.path, entries[i]);
                    File file = new File(this.filesystem, absPath);
                    
                    files.add(file);
                }
            }
            
            File[] files_arr = new File[files.size()];
            files_arr = files.toArray(files_arr);
            
            return files_arr;
        } else {
            return null;
        }
    }
    
    /*
     * Return the size in byte of this file
     */
    public long getSize() {
       loadStatus();
       
       if(this.status == null)
           return 0;
       
       return this.status.getSize();
    }
    
    /*
     * Return the last access time
     */
    public long getLastAccess() {
       loadStatus();
       
       if(this.status == null)
           return 0;
       
        return this.status.getLastAccess();
    }
    
    /*
     * Return the last modification time
     */
    public long getLastModification() {
       loadStatus();
       
       if(this.status == null)
           return 0;
       
        return this.status.getLastModification();
    }
    
    /*
     * Delete the file from filesystem
     */
    public boolean delete() {
        loadStatus();
        
        if(this.status == null)
           return false;
        
        try {
            this.filesystem.delete(this.status);
            return true;
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }
    
    /*
     * Create the directory
     */
    public boolean mkdir() {
        loadStatus();
        
        if(this.status == null) {
            try {
                this.filesystem.mkdir(this.path);
                return true;
            } catch (IOException ex) {
                LOG.error(ex);
                return false;
            }    
        } else {
           // already exist
           return false;
        }
    }
    
    /*
     * Create the directories 
     */
    public boolean mkdirs() {
        loadStatus();
        
        if(this.status == null) {
            
            reloadStatus();
            try {
                this.filesystem.mkdirs(this.path);
                return true;
            } catch (IOException ex) {
                LOG.error(ex);
                return false;
            }
        } else {
           // already exist
           return false;
        }
    }

    /*
     * Rename the file denoted by this abstract pathname. 
     */
    public boolean renameTo(File dest) {
        loadStatus();
        
        if(this.status == null)
           return false;
        
        try {
            this.filesystem.rename(this.status, dest.path);
            return true;
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }
}
