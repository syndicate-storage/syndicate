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
    
    private String ug_name;

    public JSFSConfig() {
        this.ug_name = "";
    }
    
    public JSFSConfig(String ug_name) {
        this.ug_name = ug_name;
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JSFSConfig))
            return false;
        
        JSFSConfig other = (JSFSConfig) o;
        if(!this.ug_name.equals(other.ug_name))
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.ug_name.hashCode();
    }
}
