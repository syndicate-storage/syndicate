/*
 * Configuration class for JSyndicateFS with Shared FileSystem backend
 */
package JSyndicateFS.backend.sharedfs;

import JSyndicateFS.JSFSConfiguration;

/**
 *
 * @author iychoi
 */
public class SharedFSConfiguration extends JSFSConfiguration {
    
    private final static String BACKEND_NAME = "SharedFileSystem";
    
    private java.io.File sharedFileSystemMountPoint;
    
    public SharedFSConfiguration() {
        this.sharedFileSystemMountPoint = null;
    }
    
    public java.io.File getMountPoint() {
        return this.sharedFileSystemMountPoint;
    }
    
    public void setMountPoint(java.io.File mountPoint) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.sharedFileSystemMountPoint = mountPoint;
    }
    
    @Override
    public boolean equals(Object o) {
        if(!super.equals(o))
            return false;
        
        if (!(o instanceof SharedFSConfiguration))
            return false;
        
        SharedFSConfiguration other = (SharedFSConfiguration) o;
        if(!this.sharedFileSystemMountPoint.equals(other.sharedFileSystemMountPoint))
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return super.hashCode() ^ this.sharedFileSystemMountPoint.hashCode() ^ BACKEND_NAME.hashCode();
    }
    
    @Override
    protected String getBackendName() {
        return BACKEND_NAME;
    }

    @Override
    protected Class getFileSystemClass() {
        return SharedFSFileSystem.class;
    }
}
