/**
 * Configuration helper tool for Syndicate
 */
package SyndicateHadoop.util;

import java.net.URL;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class SyndicateConfigUtil {
    
    public static final Log LOG = LogFactory.getLog(SyndicateConfigUtil.class);
    
    public static final String CONFIG_FILE_KEY = "syndicate.conf.config_file";
    public static final String PASSWORD_KEY = "syndicate.conf.password";
    public static final String USERNAME_KEY = "syndicate.conf.username";
    public static final String VOLUME_NAME_KEY = "syndicate.conf.volume_name";
    public static final String VOLUME_SECRET_KEY = "syndicate.conf.volume_secret";
    public static final String MSURL_KEY = "syndicate.conf.ms_url";
    public static final String PORT_KEY = "syndicate.conf.port";
    public static final String MAX_METADATA_CACHE_KEY = "syndicate.conf.max_metadata_cache";
    public static final String TIMEOUT_METADATA_CACHE_KEY = "syndicate.conf.timeout_metadata_cache";
    public static final String FILE_READ_BUFFER_SIZE_KEY = "syndicate.conf.file_read_buffer_size";
    public static final String FILE_WRITE_BUFFER_SIZE_KEY = "syndicate.conf.file_write_buffer_size";
    
    public static void setConfigFile(Configuration conf, String path) {
        conf.set(CONFIG_FILE_KEY, path);
    }
    
    public static void setPassword(Configuration conf, String password) {
        conf.set(PASSWORD_KEY, password);
    }
    
    public static void setUserName(Configuration conf, String username) {
        conf.set(USERNAME_KEY, username);
    }
    
    public static void setVolumeName(Configuration conf, String volumename) {
        conf.set(VOLUME_NAME_KEY, volumename);
    }
    
    public static void setVolumeSecret(Configuration conf, String volumesecret) {
        conf.set(VOLUME_SECRET_KEY, volumesecret);
    }
    
    public static void setMSUrl(Configuration conf, String msurl) {
        try {
            URL url = new URL(msurl);
            setMSUrl(conf, url);
        } catch(Exception ex) {
            throw new IllegalArgumentException("Given msurl is not a valid format");
        }
    }
    
    public static void setMSUrl(Configuration conf, URL msurl) {
        if(msurl == null)
            throw new IllegalArgumentException("Can not set url from null parameter");
        
        conf.set(MSURL_KEY, msurl.toString());
    }
    
    public static void setPort(Configuration conf, int port) {
        conf.setInt(PORT_KEY, port);
    }
    
    public static void setMaxMetadataCacheNum(Configuration conf, int maxCache) {
        conf.setInt(MAX_METADATA_CACHE_KEY, maxCache);
    }
    
    public static void setMetadataCacheTimeout(Configuration conf, int timeoutSec) {
        conf.setInt(TIMEOUT_METADATA_CACHE_KEY, timeoutSec);
    }
    
    public static void setFileReadBufferSize(Configuration conf, int bufferSize) {
        conf.setInt(FILE_READ_BUFFER_SIZE_KEY, bufferSize);
    }
    
    public static void setFileWriteBufferSize(Configuration conf, int bufferSize) {
        conf.setInt(FILE_WRITE_BUFFER_SIZE_KEY, bufferSize);
    }
}
