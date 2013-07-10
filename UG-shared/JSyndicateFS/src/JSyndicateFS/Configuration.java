/*
 * Configuration class for JSyndicateFS
 */
package JSyndicateFS;

import JSyndicateFSJNI.struct.JSFSConfig;
import java.io.File;
import java.net.URI;

/**
 *
 * @author iychoi
 */
public class Configuration {
    /*
     * if locked, values in this class become unmodifiable
     */
    private boolean lock = false;
    
    private File configFile;
    private URI msURL;
    private JSFSConfig nativeConfig;
    
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
        this.configFile = new File(JSFSConfig.DEFAULT_CONFIG_FILE_PATH);
        this.nativeConfig.setConfig_file(this.configFile.getAbsolutePath());
        
        this.maxMetadataCacheSize = MAX_METADATA_CACHE_SIZE;
        this.cacheTimeoutSecond = CACHE_TIMEOUT_SECOND;
        this.readBufferSize = READ_BUFFER_SIZE;
        this.writeBufferSize = WRITE_BUFFER_SIZE;
    }
    
    public File getConfigFile() {
        return this.configFile;
    }
    
    public void setConfigFile(File file) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.configFile = file.getAbsoluteFile();
        this.nativeConfig.setConfig_file(file.getAbsolutePath());
    }

    public String getUGPassword() {
        return this.nativeConfig.getUGPassword();
    }
    
    public void setUGPassword(String ug_password) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setUGPassword(ug_password);
    }

    public String getUGName() {
        return this.nativeConfig.getUGName();
    }
    
    public void setUGName(String ug_name) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setUGName(ug_name);
    }

    public String getVolumeName() {
        return this.nativeConfig.getVolume_name();
    }
    
    public void setVolumeName(String volumeName) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setVolume_name(volumeName);
    }

    public String getVolumeSecret() {
        return this.nativeConfig.getVolume_secret();
    }
    
    public void setVolumeSecret(String volumeSecret) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setVolume_secret(volumeSecret);
    }

    public URI getMSUrl() {
        return this.msURL;
    }
    
    public void setMSUrl(URI msUrl) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.msURL = msUrl;
        this.nativeConfig.setMs_url(msUrl.toString());
    }

    public int getPort() {
        return this.nativeConfig.getPortnum();
    }
    
    public void setPort(int port) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setPortnum(port);
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
        if(!this.configFile.equals(other.configFile))
            return false;
        if(!this.msURL.equals(other.msURL))
            return false;
        if(!this.nativeConfig.equals(other.nativeConfig))
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.configFile.hashCode() ^ this.msURL.hashCode() ^ this.nativeConfig.hashCode();
    }
}
