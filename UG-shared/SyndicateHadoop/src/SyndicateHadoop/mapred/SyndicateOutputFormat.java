/*
 * Output Format for Syndicate
 */
package SyndicateHadoop.mapred;

import JSyndicateFS.File;
import JSyndicateFS.FileSystem;
import JSyndicateFS.Path;
import SyndicateHadoop.output.SyndicateOutputCommitter;
import SyndicateHadoop.util.FileSystemUtil;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public abstract class SyndicateOutputFormat<K, V> extends OutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(SyndicateOutputFormat.class);
    
    private SyndicateOutputCommitter committer = null;
    
    @Override
    public void checkOutputSpecs(JobContext context) throws FileAlreadyExistsException, IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path outDir = SyndicateConfigUtil.getOutputPath(conf);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
        
        FileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(context.getConfiguration());
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        File outFile = new File(syndicateFS, outDir);
        if(outFile.exist()) {
            throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = SyndicateConfigUtil.getOutputPath(context.getConfiguration());
            committer = new SyndicateOutputCommitter(output, context);
        }
        return committer;
    }
}
