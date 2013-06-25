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
    
    public static final int DEFAULT_BUFFER_SIZE = 4096;
    
    private FileHandle filehandle;
    
    private byte[] buffer;
    private int bufferSize;
    private int availSizeOfBuffer;
    private int curOffsetOfBuffer;
    private long curOffset;
    private long nextReadOffset;
    private long fileSize;
    
    private long markedOffset;
    private byte[] markedData;
    
    FSInputStream(File file) {
        if(file == null)
            throw new IllegalArgumentException("Can not create input stream from null file");
        
        int bufferSize = DEFAULT_BUFFER_SIZE;
        
        FileSystem filesystem = file.getFileSystem();
        
        if(filesystem != null) {
            Configuration conf = filesystem.getConfiguration();
            if(conf != null) {
                bufferSize = conf.getReadBufferSize();
            }
            
            Path path = file.getPath();
            try {
                FileHandle filehandle = filesystem.openFileHandle(path);
                initialize(filehandle, bufferSize);
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

        int bufferSize = DEFAULT_BUFFER_SIZE;
        
        FileSystem filesystem = filehandle.getFileSystem();
        if(filesystem != null) {
            Configuration conf = filesystem.getConfiguration();
            if(conf != null) {
                bufferSize = conf.getReadBufferSize();
            }
        }
            
        initialize(filehandle, bufferSize);
    }
    
    FSInputStream(FileHandle filehandle, int bufferSize) {
        initialize(filehandle, bufferSize);
    }
    
    private void initialize(FileHandle filehandle, int bufferSize) {
        if(filehandle == null)
            throw new IllegalArgumentException("Can not create input stream from null filehandle");
        
        if(!filehandle.isOpen())
            throw new IllegalArgumentException("Can not create input stream from closed filehandle");
        
        this.filehandle = filehandle;
        if(bufferSize <= 0)
            this.bufferSize = DEFAULT_BUFFER_SIZE;
        else
            this.bufferSize = bufferSize;
        
        this.buffer = new byte[this.bufferSize];
        this.availSizeOfBuffer = 0;
        this.curOffsetOfBuffer = 0;
        this.curOffset = 0;
        this.nextReadOffset = 0;
        this.fileSize = filehandle.getStatus().getSize();
    }

    private void fillBuffer() throws IOException {
        if(this.availSizeOfBuffer <= this.curOffsetOfBuffer) {
            int ret = this.filehandle.readFileData(this.buffer, this.bufferSize, this.nextReadOffset);
            if(ret <= 0)
                throw new IOException("Can not read data from the file handle, readSize is " + ret);
            
            this.availSizeOfBuffer = ret;
            this.nextReadOffset += ret;
            this.curOffsetOfBuffer = 0;
        }
    }
    
    /*
     * Reads the next byte of data from the input stream. The value byte is returned as an int in the range 0 to 255. If no byte is available because the end of the stream has been reached, the value -1 is returned. This method blocks until input data is available, the end of the stream is detected, or an exception is thrown. 
     * A subclass must provide an implementation of this method. 
     */
    @Override
    public int read() throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not read data from closed file handle");
        
        if(this.fileSize <= this.curOffset) {
            // EOF
            return -1;
        }
        
        fillBuffer();
        
        int readData = 0;
        
        try {
            readData = (int)this.buffer[this.curOffsetOfBuffer];
        } catch (Exception ex) {
            // array bound exception
            throw new IOException(ex.toString());
        }
        
        this.curOffset++;
        this.curOffsetOfBuffer++;
        
        return readData;
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not read data from closed file handle");
        
        if(this.fileSize <= this.curOffset) {
            // EOF
            return -1;
        }
        
        long bytesLeft = len;
        int targetOffset = off;
        int readBytes = 0;
        
        if(this.fileSize <= this.curOffset + len) {
            bytesLeft = this.fileSize - this.curOffset;
        }
        
        while(bytesLeft > 0) {
            try {
                fillBuffer();

                int bufferBytesLeft = this.availSizeOfBuffer - this.curOffsetOfBuffer;
                System.arraycopy(this.buffer, this.curOffsetOfBuffer, b, targetOffset, bufferBytesLeft);

                this.curOffsetOfBuffer += bufferBytesLeft;
                this.curOffset += bufferBytesLeft;
                bytesLeft -= bufferBytesLeft;
                targetOffset += bufferBytesLeft;
                
                readBytes += bufferBytesLeft;
                
            } catch(Exception ex) {
                // array bound exception
                throw new IOException(ex.toString());
            }    
        }
        
        return readBytes;
    }
    
    @Override
    public long skip(long n) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not read data from closed file handle");
        
        if(this.fileSize <= this.curOffset) {
            // EOF
            return -1;
        }
        
        long bytesLeft = n;
        
        if(this.fileSize <= this.curOffset + n) {
            bytesLeft = this.fileSize - this.curOffset;
        }
        
        int bufferBytesLeft = this.availSizeOfBuffer - this.curOffsetOfBuffer;
        
        if(bufferBytesLeft > bytesLeft) {
            this.curOffsetOfBuffer += bytesLeft;
            this.curOffset += bytesLeft;
            return bytesLeft;
        } else {
            this.curOffsetOfBuffer += bufferBytesLeft;
            
            this.curOffset += bytesLeft;
            this.curOffsetOfBuffer = 0;
            this.availSizeOfBuffer = 0;
            this.nextReadOffset = this.curOffset;
            return bytesLeft;
        }
    }
    
    @Override
    public int available() throws IOException {
        return (int)(this.fileSize - this.curOffset);
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
        this.availSizeOfBuffer = 0;
        this.curOffsetOfBuffer = 0;
        this.curOffset = 0;
        this.nextReadOffset = 0;
    }
}
