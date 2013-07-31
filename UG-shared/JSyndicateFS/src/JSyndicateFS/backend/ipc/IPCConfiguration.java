/*
 * JSFSConfiguration class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc;

import JSyndicateFS.JSFSConfiguration;

/**
 *
 * @author iychoi
 */
public class IPCConfiguration extends JSFSConfiguration {
    
    private final static String BACKEND_NAME = "IPC";
    
    // IPC backend mode default port
    public static final int DEFAULT_IPC_PORT = 9910;
    
    private int ipcPort;
    private String UGName;
    
    public IPCConfiguration() {
        this.ipcPort = DEFAULT_IPC_PORT;
        this.UGName = null;
    }
    
    public String getUGName() {
        return this.UGName;
    }
    
    public void setUGName(String ug_name) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.UGName = ug_name;
    }
    
    public int getPort() {
        return this.ipcPort;
    }
    
    public void setPort(int port) throws IllegalAccessException {
        if(this.lock)
            throw new IllegalAccessException("Can not modify the locked object");
        
        this.ipcPort = port;
    }
    
    @Override
    public boolean equals(Object o) {
        if(!super.equals(o))
            return false;
        
        if (!(o instanceof IPCConfiguration))
            return false;
        
        IPCConfiguration other = (IPCConfiguration) o;
        if(!this.UGName.equals(other.UGName))
            return false;
        if(this.ipcPort != other.ipcPort)
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return super.hashCode() ^ this.UGName.hashCode() ^ this.ipcPort ^ BACKEND_NAME.hashCode();
    }

    @Override
    protected String getBackendName() {
        return BACKEND_NAME;
    }

    @Override
    protected Class getFileSystemClass() {
        return IPCFileSystem.class;
    }
}
