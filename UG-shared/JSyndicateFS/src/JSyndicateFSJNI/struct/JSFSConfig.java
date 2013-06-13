/*
 * Configuration class for JSyndicateFS
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public class JSFSConfig {
    
    public static final String DEFAULT_CONFIG_FILE_PATH = "/etc/syndicate/syndicate-client.conf";
    
    private String config_file;
    private String username;
    private String password;
    private String volume_name;
    private String volume_secret;
    private String ms_url;
    private int portnum;

    public JSFSConfig() {
        this.config_file = DEFAULT_CONFIG_FILE_PATH;
        this.username = "";
        this.password = "";
        this.volume_name = "";
        this.volume_secret = "";
        this.ms_url = "";
        this.portnum = -1;
    }
    
    public JSFSConfig(String config_file, String username, String password, String volume_name, String volume_secret, String ms_url, int portnum) {
        this.config_file = config_file;
        this.username = username;
        this.password = password;
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
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
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
    
}
