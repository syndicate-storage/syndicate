/*
 * Text Output Format for Syndicate
 */
package SyndicateHadoop.mapred;

import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSPath;
import SyndicateHadoop.mapred.output.SyndicateRecordWriter;
import SyndicateHadoop.util.FileSystemUtil;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class SyndicateTextOutputFormat<K, V> extends SyndicateOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String keyValueSeparator = SyndicateConfigUtil.getTextOutputFormatSeparator(conf);
        
        String extension = "";
        JSFSPath path = getDefaultWorkFile(context, extension);
        
        JSFSFileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(context.getConfiguration());
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        DataOutputStream fileOut = new DataOutputStream(syndicateFS.getFileOutputStream(path));
        return new SyndicateRecordWriter<K, V>(fileOut, keyValueSeparator);
    }
}
