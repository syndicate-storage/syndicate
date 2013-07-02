/*
 * Text Record Reader class for Syndicate
 */
package SyndicateHadoop.mapred.input;

import JSyndicateFS.FSInputStream;
import JSyndicateFS.File;
import JSyndicateFS.FileSystem;
import JSyndicateFS.Path;
import SyndicateHadoop.input.SyndicateInputSplit;
import SyndicateHadoop.util.FileSystemUtil;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author iychoi
 */
public class SyndicateTextRecordReader extends RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(SyndicateTextRecordReader.class);
    
    public static final String MAX_LINE_LENGTH = "mapred.linerecordreader.maxlength";
    
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(genericSplit instanceof SyndicateInputSplit))
            throw new IllegalArgumentException("Creation of a new RecordReader requires a SyndicateInputSplit instance.");
        
        SyndicateInputSplit split = (SyndicateInputSplit) genericSplit;
        Configuration conf = context.getConfiguration();
        
        this.maxLineLength = conf.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        
        Path path = split.getPath();
        
        FileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(conf);
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        File file = new File(syndicateFS, path);
        FSInputStream is = new FSInputStream(file);
        
        boolean skipFirstLine = false;
        if (this.start != 0) {
            skipFirstLine = true;
            --this.start;
            is.seek(this.start);
        }
        
        int bufferSize = (int)file.getBlockSize();
        if(SyndicateConfigUtil.getFileReadBufferSize(conf) != 0) {
            bufferSize = SyndicateConfigUtil.getFileReadBufferSize(conf);
        }
        this.in = new LineReader(is, bufferSize);
        
        if (skipFirstLine) {
            // skip first line and re-establish "start".
            this.start += this.in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, this.end - this.start));
        }
        this.pos = this.start;
    }
    
    @Override
    public void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (this.start == this.end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (this.pos - this.start) / (float) (this.end - this.start));
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.key == null) {
            this.key = new LongWritable();
        }
        
        this.key.set(this.pos);
        
        if (this.value == null) {
            this.value = new Text();
        }
        
        int newSize = 0;
        while (this.pos < this.end) {
            newSize = this.in.readLine(this.value, this.maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));
            if (newSize == 0) {
                break;
            }
            
            this.pos += newSize;
            if (newSize < this.maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - newSize));
        }
        
        if (newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }
}
