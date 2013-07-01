/*
 * Text Input Format for Syndicate
 */
package SyndicateHadoop.mapred;

import SyndicateHadoop.input.SyndicateInputSplit;
import SyndicateHadoop.mapred.input.SyndicateTextRecordReader;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class SyndicateTextInputFormat extends SyndicateInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (!(split instanceof SyndicateInputSplit))
            throw new IllegalStateException("Creation of a new RecordReader requires a SyndicateInputSplit instance.");
        
        return new SyndicateTextRecordReader();
    }
}
