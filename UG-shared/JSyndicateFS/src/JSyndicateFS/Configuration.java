/*
 * Configuration class for JSyndicateFS
 */
package JSyndicateFS;

import JSyndicateFSJNI.struct.JSFSConfig;
import java.io.File;

/**
 *
 * @author iychoi
 */
public class Configuration {
    /*
     * if locked, values in this class become unmodifiable
     */
    private boolean lock = false;
    
    private JSFSConfig nativeConfig;
    private File nativeLibraryFile;
    
    // unlimited size
    public static final int MAX_METADATA_CACHE_SIZE = 0;
    // no timeout
    public static final int CACHE_TIMEOUT_SECOND = 0;
    // read buffer size
    public static final int READ_BUFFER_SIZE = 4096;
    // write buffer size
    public static final int WRITE_BUFFER_SIZE = 4096;
    
    private int maxMetadataCacheSize;
    private int cacheTimeoutSecond;
    private int readBufferSize;
    private int writeBufferSize;
    
    public Configuration() {
        this.nativeConfig = new JSFSConfig();
        
        // set default
        this.nativeLibraryFile = null;
        
        this.maxMetadataCacheSize = MAX_METADATA_CACHE_SIZE;
        this.cacheTimeoutSecond = CACHE_TIMEOUT_SECOND;
        this.readBufferSize = READ_BUFFER_SIZE;
        this.writeBufferSize = WRITE_BUFFER_SIZE;
    }
    
    public File getNativeLibraryFile() {
        return this.nativeLibraryFile;
    }
    
    public void setNativeLibraryFile(File file) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeLibraryFile = file.getAbsoluteFile();
    }
    
    public String getUGName() {
        return this.nativeConfig.getUGName();
    }
    
    public void setUGName(String ug_name) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setUGName(ug_name);
    }

    public int getMaxMetadataCacheSize() {
        return this.maxMetadataCacheSize;
    }
    
    public void setMaxMetadataCacheSize(int max) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.maxMetadataCacheSize = max;
    }
    
    public int getCacheTimeoutSecond() {
        return this.cacheTimeoutSecond;
    }
    
    public void setCacheTimeoutSecond(int timeoutSecond) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.cacheTimeoutSecond = timeoutSecond;
    }
    
    public int getReadBufferSize() {
        return this.readBufferSize;
    }
    
    public void setReadBufferSize(int bufferSize) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.readBufferSize = bufferSize;
    }
    
    public int getWriteBufferSize() {
        return this.writeBufferSize;
    }
    
    public void setWriteBufferSize(int bufferSize) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.writeBufferSize = bufferSize;
    }
    
    public void lock() {
        this.lock = true;
    }
    
    public boolean isLocked() {
        return this.lock;
    }
    
    /*
     * Return JSFSConfig which is a JNI layer config type
     */
    JSFSConfig getJSFSConfig() {
        return this.nativeConfig;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Configuration))
            return false;
        
        Configuration other = (Configuration) o;
        if(!this.nativeConfig.equals(other.nativeConfig))
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.nativeConfig.hashCode();
    }
}
