/*
 * JSFSConfiguration class for JSyndicateFS
 */
package JSyndicateFS;

/**
 *
 * @author iychoi
 */
public abstract class JSFSConfiguration {
    /*
     * if locked, values in this class become unmodifiable
     */
    protected boolean lock = false;
    
    // read buffer size
    public static final int READ_BUFFER_SIZE = 4096;
    // write buffer size
    public static final int WRITE_BUFFER_SIZE = 4096;
    
    protected int readBufferSize;
    protected int writeBufferSize;
    
    public JSFSConfiguration() {
        this.readBufferSize = READ_BUFFER_SIZE;
        this.writeBufferSize = WRITE_BUFFER_SIZE;
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
    
    protected abstract String getBackendName();
    
    protected abstract Class getFileSystemClass();
    
    public void lock() {
        this.lock = true;
    }
    
    public boolean isLocked() {
        return this.lock;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JSFSConfiguration))
            return false;
        
        JSFSConfiguration other = (JSFSConfiguration) o;
        
        if(this.readBufferSize != other.readBufferSize)
            return false;
        if(this.writeBufferSize != other.writeBufferSize)
            return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.readBufferSize ^ this.writeBufferSize ^ getBackendName().hashCode();
    }
}
