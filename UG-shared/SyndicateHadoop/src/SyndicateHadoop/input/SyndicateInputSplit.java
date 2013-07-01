/*
 * InputSplit class for Syndicate
 */
package SyndicateHadoop.input;

import JSyndicateFS.FileSystem;
import JSyndicateFS.Path;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author iychoi
 */
public class SyndicateInputSplit extends InputSplit {

    private FileSystem filesystem;
    private Path path;
    private long start;
    private long length;

    /*
     * Constructs a split
     */
    public SyndicateInputSplit(FileSystem fs, Path path, long start, long length) {
        if(fs == null)
            throw new IllegalArgumentException("Can not create Input Split from null file system");
        if(path == null)
            throw new IllegalArgumentException("Can not create Input Split from null path");
        
        this.filesystem = fs;
        this.path = path;
        this.start = start;
        this.length = length;
    }

    public FileSystem getFileSystem() {
        return this.filesystem;
    }
    
    /*
     * The file containing this split's data
     */
    public Path getPath() {
        return this.path;
    }

    /*
     * The position of split start
     */
    public long getStart() {
        return this.start;
    }

    /*
     * The number of bytes in the file to process
     */
    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    public String toString() {
        return this.path.getPath() + ":" + start + "+" + length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return null;
    }
}
