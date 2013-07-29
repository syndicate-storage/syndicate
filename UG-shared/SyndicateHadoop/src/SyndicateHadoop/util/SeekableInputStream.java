/*
 * SeekableInputStream class for Syndicate
 */
package SyndicateHadoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import org.apache.hadoop.fs.Seekable;

/**
 *
 * @author iychoi
 */
public class SeekableInputStream extends InputStream implements Seekable {

    private RandomAccessFile raf;
    
    public SeekableInputStream(RandomAccessFile raf) {
        this.raf = raf;
    }

    @Override
    public int read() throws IOException {
        return this.raf.read();
    }

    @Override
    public void seek(long l) throws IOException {
        this.raf.seek(l);
    }

    @Override
    public long getPos() throws IOException {
        return this.raf.getFilePointer();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.raf.close();
    }
}
