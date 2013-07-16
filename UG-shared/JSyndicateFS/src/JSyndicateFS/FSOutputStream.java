/*
 * FSOutputStream class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FSOutputStream extends OutputStream {

    public static final Log LOG = LogFactory.getLog(FSOutputStream.class);
    
    private FileHandle filehandle;
    private byte[] buffer = new byte[4096];
    private long offset;

    public FSOutputStream(File file) throws IOException {
        if(file == null)
            throw new IllegalArgumentException("Can not create output stream from null file");
        
        FileSystem filesystem = file.getFileSystem();
        
        if(filesystem != null) {
            Path path = file.getPath();
            
            if(!filesystem.exists(path)) {
                // create before write
                try {
                    boolean created = filesystem.createNewFile(path);
                    if(!created) {
                        LOG.error("Can not create the file");
                        throw new IOException("Can not create the file");
                    }
                } catch (IOException ex) {
                    LOG.error(ex);
                    throw new IOException(ex.toString());
                }
            } else {
                // truncate before write
                filesystem.truncateFile(path, 0);
            }
            
            try {
                FileHandle filehandle = filesystem.openFileHandle(path);
                initialize(filehandle);
            } catch (FileNotFoundException ex) {
                LOG.error(ex);
                throw new IOException(ex.toString());
            } catch (IOException ex) {
                LOG.error(ex);
                throw new IOException(ex.toString());
            }
        } else {
            throw new IllegalArgumentException("Can not create input stream from null filesystem");
        }
    }
    
    FSOutputStream(FileHandle filehandle) {
        if(filehandle == null)
            throw new IllegalArgumentException("Can not create output stream from null filehandle");

        initialize(filehandle);
    }
    
    private void initialize(FileHandle filehandle) {
        if(filehandle == null)
            throw new IllegalArgumentException("Can not create output stream from null filehandle");
        if(filehandle.isDirty())
            throw new IllegalArgumentException("Can not create output stream from dirty filehandle");
        if(!filehandle.isOpen())
            throw new IllegalArgumentException("Can not create output stream from closed filehandle");
        
        this.filehandle = filehandle;
        this.offset = 0;
    }

    @Override
    public void write(int i) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not write data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not write data from dirty file handle");
        
        try {
            this.buffer[0] = (byte)i;
            this.filehandle.writeFileData(this.buffer, 1, this.offset);
        } catch (Exception ex) {
            throw new IOException(ex.toString());
        }
        
        this.offset++;
    }
    
    @Override
    public void write(byte[] bytes) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not write data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not write data from dirty file handle");
        
        this.filehandle.writeFileData(bytes, bytes.length, this.offset);
        
        this.offset += bytes.length;
    }
    
    @Override
    public void write(byte[] bytes, int offset, int size) throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not write data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not write data from dirty file handle");
        
        int writeLeft = size;
        int targetOffset = offset;
        
        while(writeLeft > 0) {
            int writeLen = writeLeft;
            if(writeLeft > this.buffer.length)
                writeLen = this.buffer.length;
            
            System.arraycopy(bytes, targetOffset, this.buffer, 0, writeLen);
            
            this.filehandle.writeFileData(this.buffer, writeLen, this.offset);
         
            this.offset += writeLen;
            targetOffset += writeLen;
            writeLeft -= writeLen;
        }
    }
    
    @Override
    public void flush() throws IOException {
        if(!this.filehandle.isOpen())
            throw new IOException("Can not write data from closed file handle");
        if(this.filehandle.isDirty())
            throw new IOException("Can not write data from dirty file handle");

        this.filehandle.flush();
    }
    
    @Override
    public void close() throws IOException {
        try {
            this.filehandle.flush();
        } catch(Exception ex) {
            LOG.error(ex);
        }
        
        this.filehandle.close();
        this.offset = 0;
    }
}
