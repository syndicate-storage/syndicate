/*
 * FSOutputStream class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author iychoi
 */
public class FSOutputStream extends OutputStream {

    public static final int DEFAULT_BUFFER_SIZE = 4096;
    
    private FileHandle filehandle;
    
    private byte[] buffer;
    private int bufferSize;
    private int curOffsetOfBuffer;
    private long curOffset;
    private long nextWriteOffset;

    FSOutputStream(FileHandle filehandle) {
        initialize(filehandle, DEFAULT_BUFFER_SIZE);
    }
    
    FSOutputStream(FileHandle filehandle, int bufferSize) {
        initialize(filehandle, bufferSize);
    }
    
    private void initialize(FileHandle filehandle, int bufferSize) {
        if(filehandle == null)
            throw new IllegalArgumentException("Can not create output stream from null filehandle");
        
        if(!filehandle.isOpen())
            throw new IllegalArgumentException("Can not create output stream from closed filehandle");
        
        this.filehandle = filehandle;
        if(bufferSize <= 0)
            this.bufferSize = DEFAULT_BUFFER_SIZE;
        else
            this.bufferSize = bufferSize;
        
        this.buffer = new byte[this.bufferSize];
        this.curOffsetOfBuffer = 0;
        this.curOffset = 0;
        this.nextWriteOffset = 0;
    }

    @Override
    public void write(int i) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not write data from closed file handle");
        
        try {
            this.buffer[this.curOffsetOfBuffer] = (byte)i;
        } catch (Exception ex) {
            // array bound exception
            throw new IOException(ex.toString());
        }
        
        this.curOffset++;
        this.curOffsetOfBuffer++;
        
        if(this.bufferSize <= this.curOffsetOfBuffer) {
            flush();
        }
    }
    
    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }
    
    @Override
    public void write(byte[] bytes, int offset, int size) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not write data from closed file handle");
        
        int bufferAvailable = this.bufferSize - this.curOffsetOfBuffer;
        if(size > bufferAvailable) {
            // need more space
            int bytesLeft = size;
            int windowOffset = offset;
            
            while(bytesLeft > 0) {
                try {
                    int windowSize;
                    if(bytesLeft < bufferAvailable)
                        windowSize = bytesLeft;
                    else
                        windowSize = bufferAvailable;
                    
                    System.arraycopy(bytes, windowOffset, this.buffer, this.curOffsetOfBuffer, windowSize);
                    
                    this.curOffset += windowSize;
                    this.curOffsetOfBuffer += windowSize;
                    windowOffset += windowSize;
                    bytesLeft -= windowSize;
                    
                    if(this.curOffsetOfBuffer >= this.bufferSize) {
                        flush();
                    }
                    
                    bufferAvailable = this.bufferSize - this.curOffsetOfBuffer;
                    
                } catch (Exception ex) {
                    // array bound exception
                    throw new IOException(ex.toString());
                }    
            }
        } else {
            // write to buffer
            try {
                System.arraycopy(bytes, offset, this.buffer, this.curOffsetOfBuffer, size);
                this.curOffset += size;
                this.curOffsetOfBuffer += size;
                
                if(this.curOffsetOfBuffer >= this.bufferSize) {
                    flush();
                }
            } catch (Exception ex) {
                // array bound exception
                throw new IOException(ex.toString());
            }
        }
    }
    
    @Override
    public void flush() throws IOException {
        if(this.curOffsetOfBuffer > 0) {
            this.filehandle.writeFileData(this.buffer, this.curOffsetOfBuffer, this.nextWriteOffset);

            this.nextWriteOffset += this.curOffsetOfBuffer;
            this.curOffsetOfBuffer = 0;
        }
    }
    
    @Override
    public void close() throws IOException {
        
        flush();
        
        this.filehandle.close();
        this.curOffsetOfBuffer = 0;
        this.curOffset = 0;
        this.nextWriteOffset = 0;
    }
}
