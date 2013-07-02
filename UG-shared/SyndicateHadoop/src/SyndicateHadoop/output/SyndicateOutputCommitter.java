/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SyndicateHadoop.output;

import JSyndicateFS.Path;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class SyndicateOutputCommitter extends OutputCommitter {

    private static final Log LOG = LogFactory.getLog(SyndicateOutputCommitter.class);
    
    public SyndicateOutputCommitter(Path output, TaskAttemptContext context) {
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        LOG.info("Setting up job.");
    }

    @Override
    public void setupTask(TaskAttemptContext tac) throws IOException {
        LOG.info("Setting up task.");
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException {
        return true;
    }

    @Override
    public void commitTask(TaskAttemptContext tac) throws IOException {
        LOG.info("Committing task.");
    }

    @Override
    public void abortTask(TaskAttemptContext tac) throws IOException {
        LOG.info("Aborting task.");
    }
}
