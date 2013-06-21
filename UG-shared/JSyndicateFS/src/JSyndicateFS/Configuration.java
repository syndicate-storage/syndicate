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
    
    private int maxMetadataCacheSize;
    private int cacheTimeoutSecond;
    
    public Configuration() {
        this.nativeConfig = new JSFSConfig();
        
        // set default
        this.configFile = new File(JSFSConfig.DEFAULT_CONFIG_FILE_PATH);
        this.nativeConfig.setConfig_file(this.configFile.getAbsolutePath());
        
        this.maxMetadataCacheSize = MAX_METADATA_CACHE_SIZE;
        this.cacheTimeoutSecond = CACHE_TIMEOUT_SECOND;
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

    public String getPassword() {
        return this.nativeConfig.getPassword();
    }
    
    public void setPassword(String password) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setPassword(password);
    }

    public String getUsername() {
        return this.nativeConfig.getUsername();
    }
    
    public void setUsername(String username) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.nativeConfig.setUsername(username);
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
    
    public void lock() {
        this.lock = true;
    }
    
    public boolean isLocked() {
        return this.lock;
    }
    
    /*
     * Return JSFSConfig which is a JNI layer config type
     */
    public JSFSConfig getJSFSConfig() {
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
