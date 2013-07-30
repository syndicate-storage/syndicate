/*
 * InputSplit class for Syndicate
 */
package SyndicateHadoop.input;

import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSPath;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author iychoi
 */
public class SyndicateInputSplit extends InputSplit implements Writable {

    private JSFSFileSystem filesystem;
    private JSFSPath path;
    private long start;
    private long length;

    // default constructor
    public SyndicateInputSplit() {
    }
    
    /*
     * Constructs a split
     */
    public SyndicateInputSplit(JSFSFileSystem fs, JSFSPath path, long start, long length) {
        if(fs == null)
            throw new IllegalArgumentException("Can not create Input Split from null file system");
        if(path == null)
            throw new IllegalArgumentException("Can not create Input Split from null path");
        
        this.filesystem = fs;
        this.path = path;
        this.start = start;
        this.length = length;
    }

    public JSFSFileSystem getFileSystem() {
        return this.filesystem;
    }
    
    /*
     * The file containing this split's data
     */
    public JSFSPath getPath() {
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
        String fsName = this.filesystem.toString();
        return new String[] {fsName};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.path.getPath());
        out.writeLong(this.start);
        out.writeLong(this.length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.path = new JSFSPath(Text.readString(in));
        this.start = in.readLong();
        this.length = in.readLong();
    }
}
