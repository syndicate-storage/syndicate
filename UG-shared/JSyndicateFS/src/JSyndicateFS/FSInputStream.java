/*
 * FSInputStream class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FSInputStream extends InputStream {

    public static final Log LOG = LogFactory.getLog(FSInputStream.class);
    
    private FileHandle filehandle;
    private byte[] buffer = new byte[4096];
    private long offset;
    
    FSInputStream(File file) {
        if(file == null)
            throw new IllegalArgumentException("Can not create input stream from null file");
        
        FileSystem filesystem = file.getFileSystem();
        
        if(filesystem != null) {
            Path path = file.getPath();
            try {
                FileHandle filehandle = filesystem.openFileHandle(path);
                initialize(filehandle);
            } catch (FileNotFoundException ex) {
                LOG.error(ex);
                throw new IllegalArgumentException(ex.toString());
            } catch (IOException ex) {
                LOG.error(ex);
                throw new IllegalArgumentException(ex.toString());
            }
        } else {
            throw new IllegalArgumentException("Can not create input stream from null filesystem");
        }
    }
    
    FSInputStream(FileHandle filehandle) {
        if(filehandle == null)
            throw new IllegalArgumentException("Can not create input stream from null filehandle");

        initialize(filehandle);
    }
    
    private void initialize(FileHandle filehandle) {
        if(filehandle == null)
            throw new IllegalArgumentException("Can not create input stream from null filehandle");
        if(filehandle.isDirty())
            throw new IllegalArgumentException("Can not create input stream from dirty filehandle");
        if(!filehandle.isOpen())
            throw new IllegalArgumentException("Can not create input stream from closed filehandle");
        
        this.filehandle = filehandle;
        this.offset = 0;
    }

    /*
     * Reads the next byte of data from the input stream. The value byte is returned as an int in the range 0 to 255. If no byte is available because the end of the stream has been reached, the value -1 is returned. This method blocks until input data is available, the end of the stream is detected, or an exception is thrown. 
     * A subclass must provide an implementation of this method. 
     */
    @Override
    public int read() throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not read data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not read data from dirty file handle");
            
        if(this.filehandle.getStatus().getSize() <= this.offset) {
            // EOF
            return -1;
        }
        
        int readData = 0;
        
        try {
            int ret = this.filehandle.readFileData(this.buffer, 1, this.offset);
            if(ret <= 0) {
                throw new IOException("Can not read data from the file handle, readSize is " + ret);
            }
            readData = (int)this.buffer[0];
        } catch (Exception ex) {
            throw new IOException(ex.toString());
        }
        
        this.offset++;
        
        return readData;
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not read data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not read data from dirty file handle");
        
        if(this.filehandle.getStatus().getSize() <= this.offset) {
            // EOF
            return -1;
        }
        
        int readLen = 0;
        int ret = this.filehandle.readFileData(b, b.length, this.offset);
        if(ret <= 0) {
            throw new IOException("Can not read data from the file handle, readSize is " + ret);
        }
        
        readLen = ret;
        this.offset += readLen;
        
        return readLen;
    }
    
    @Override
    public int read(byte[] bytes, int offset, int size) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not read data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not read data from dirty file handle");
        
        if(this.filehandle.getStatus().getSize() <= this.offset) {
            // EOF
            return -1;
        }
        
        long readLeft = size;
        if(this.filehandle.getStatus().getSize() <= this.offset + size) {
            readLeft = this.filehandle.getStatus().getSize() - this.offset;
        }
        
        int targetOffset = offset;
        int readTotal = 0;
        
        while(readLeft > 0) {
            int readLen = 0;
            int readWant = (int)readLeft;
            if(readLeft > this.buffer.length)
                readWant = this.buffer.length;
                
            int ret = this.filehandle.readFileData(this.buffer, readWant, this.offset);
            if(ret <= 0) {
                throw new IOException("Can not read data from the file handle, readSize is " + ret);
            }
         
            readLen = ret;
            this.offset += readLen;
            
            System.arraycopy(this.buffer, 0, bytes, targetOffset, readLen);
            
            targetOffset += readLen;
            readTotal += readLen;
            readLeft -= readLen;
        }
        
        return readTotal;
    }
    
    @Override
    public long skip(long n) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not read data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not read data from dirty file handle");
        
        if(this.filehandle.getStatus().getSize() <= this.offset) {
            // EOF
            return -1;
        }
        
        long bytesSkip = n;
        
        if(this.filehandle.getStatus().getSize() <= this.offset + n) {
            bytesSkip = this.filehandle.getStatus().getSize() - this.offset;
        }
        
        this.offset += bytesSkip;
        return bytesSkip;
    }
    
    @Override
    public int available() throws IOException {
        return (int)(this.filehandle.getStatus().getSize() - this.offset);
    }
    
    public synchronized void mark(int readlimit) {
    }
    
    public synchronized void reset() throws IOException {
    }
    
    public boolean markSupported() {
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.filehandle.close();
        this.offset = 0;
    }
}
