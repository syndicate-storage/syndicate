/*
 * Configuration class for JSyndicateFS
 * - This class is used between JNI layers
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public class JSFSConfig {
    
    public static final String DEFAULT_CONFIG_FILE_PATH = "/etc/syndicate/syndicate-UG.conf";
    
    private String config_file;
    private String ug_name;
    private String ug_password;
    private String volume_name;
    private String volume_secret;
    private String ms_url;
    private int portnum;

    public JSFSConfig() {
        this.config_file = DEFAULT_CONFIG_FILE_PATH;
        this.ug_name = "";
        this.ug_password = "";
        this.volume_name = "";
        this.volume_secret = "";
        this.ms_url = "";
        this.portnum = -1;
    }
    
    public JSFSConfig(String config_file, String ug_name, String ug_password, String volume_name, String volume_secret, String ms_url, int portnum) {
        this.config_file = config_file;
        this.ug_name = ug_name;
        this.ug_password = ug_password;
        this.volume_name = volume_name;
        this.volume_secret = volume_secret;
        this.ms_url = ms_url;
        this.portnum = portnum;
    }

    /**
     * @return the config_file
     */
    public String getConfig_file() {
        return config_file;
    }

    /**
     * @param config_file the config_file to set
     */
    public void setConfig_file(String config_file) {
        this.config_file = config_file;
    }

    /**
     * @return the UG name
     */
    public String getUGName() {
        return ug_name;
    }

    /**
     * @param ug_name the UG name to set
     */
    public void setUGName(String ug_name) {
        this.ug_name = ug_name;
    }

    /**
     * @return the UG password
     */
    public String getUGPassword() {
        return ug_password;
    }

    /**
     * @param ug_password the password to set
     */
    public void setUGPassword(String ug_password) {
        this.ug_password = ug_password;
    }

    /**
     * @return the volume_name
     */
    public String getVolume_name() {
        return volume_name;
    }

    /**
     * @param volume_name the volume_name to set
     */
    public void setVolume_name(String volume_name) {
        this.volume_name = volume_name;
    }

    /**
     * @return the volume_secret
     */
    public String getVolume_secret() {
        return volume_secret;
    }

    /**
     * @param volume_secret the volume_secret to set
     */
    public void setVolume_secret(String volume_secret) {
        this.volume_secret = volume_secret;
    }

    /**
     * @return the ms_url
     */
    public String getMs_url() {
        return ms_url;
    }

    /**
     * @param ms_url the ms_url to set
     */
    public void setMs_url(String ms_url) {
        this.ms_url = ms_url;
    }

    /**
     * @return the portnum
     */
    public int getPortnum() {
        return portnum;
    }

    /**
     * @param portnum the portnum to set
     */
    public void setPortnum(int portnum) {
        this.portnum = portnum;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JSFSConfig))
            return false;
        
        JSFSConfig other = (JSFSConfig) o;
        if(!this.config_file.equals(other.config_file))
            return false;
        if(!this.ug_name.equals(other.ug_name))
            return false;
        if(!this.ug_password.equals(other.ug_password))
            return false;
        if(!this.volume_name.equals(other.volume_name))
            return false;
        if(!this.volume_secret.equals(other.volume_secret))
            return false;
        if(!this.ms_url.equals(other.ms_url))
            return false;
        if(portnum != other.portnum)
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.config_file.hashCode() ^ this.ug_name.hashCode() ^ this.ug_password.hashCode()
                ^ this.volume_name.hashCode() ^ this.volume_secret.hashCode() ^ this.ms_url.hashCode() 
                ^ this.portnum;
    }
}
