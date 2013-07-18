/*
 * Record Writer class for Syndicate
 */
package SyndicateHadoop.mapred.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class SyndicateRecordWriter<K, V> extends RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;

    static {
        try {
            newline = "\n".getBytes(utf8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");
        }
    }
    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public SyndicateRecordWriter(DataOutputStream out) {
        this(out, "\t");
    }

    public SyndicateRecordWriter(DataOutputStream out, String keyValueSeparator) {
        this.out = out;
        try {
            this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");
        }
    }

    private void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
            Text to = (Text) o;
            out.write(to.getBytes(), 0, to.getLength());
        } else {
            out.write(o.toString().getBytes(utf8));
        }
    }

    public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = false;
        if (key == null || key instanceof NullWritable) {
            nullKey = true;
        }

        boolean nullValue = false;
        if (value == null || value instanceof NullWritable) {
            nullValue = true;
        }

        if (nullKey && nullValue) {
            return;
        }

        if (!nullKey) {
            writeObject(key);
        }

        if (!(nullKey || nullValue)) {
            out.write(keyValueSeparator);
        }

        if (!nullValue) {
            writeObject(value);
        }

        out.write(newline);
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
