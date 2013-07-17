/*
 * Text Output Format for Syndicate
 */
package SyndicateHadoop.mapred;

import JSyndicateFS.FSOutputStream;
import JSyndicateFS.File;
import JSyndicateFS.FileSystem;
import JSyndicateFS.Path;
import SyndicateHadoop.mapred.output.SyndicateHadoopRecordWriter;
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
        Path path = getDefaultWorkFile(context, extension);
        
        FileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(context.getConfiguration());
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        File file = new File(syndicateFS, path);
        DataOutputStream fileOut = new DataOutputStream(new FSOutputStream(file));
        return new SyndicateHadoopRecordWriter<K, V>(fileOut, keyValueSeparator);
    }
}
