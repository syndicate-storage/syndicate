/*
 * NLine Input Format for Syndicate
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package SyndicateHadoop.mapred;

import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSPath;
import SyndicateHadoop.input.SyndicateInputSplit;
import SyndicateHadoop.mapred.input.SyndicateTextRecordReader;
import SyndicateHadoop.util.FileSystemUtil;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author iychoi
 */
public class SyndicateNLineInputFormat extends SyndicateInputFormat<LongWritable, Text> {
    
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (!(split instanceof SyndicateInputSplit))
            throw new IllegalStateException("Creation of a new RecordReader requires a SyndicateInputSplit instance.");
        
        Configuration conf = context.getConfiguration();
        String delimiter = SyndicateConfigUtil.getTextRecordDelimiter(conf);
        byte[] delimiter_bytes = null;
        if(delimiter != null) {
            delimiter_bytes = delimiter.getBytes();
        }
        return new SyndicateTextRecordReader(delimiter_bytes);
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Configuration conf = context.getConfiguration();
        int numLinesPerSplit = SyndicateConfigUtil.getTextLinesPerMap(conf);
        for(JSFSPath path : listFiles(context)) {
            splits.addAll(getSplitsForFile(path, conf, numLinesPerSplit));
        }
        return splits;
    }
    
    private List<SyndicateInputSplit> getSplitsForFile(JSFSPath path, Configuration conf, int numLinesPerSplit) throws IOException {
        List<SyndicateInputSplit> splits = new ArrayList<SyndicateInputSplit>();
        
        JSFSFileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(conf);
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        String delimiter = SyndicateConfigUtil.getTextRecordDelimiter(conf);
        byte[] delimiter_bytes = null;
        if(delimiter != null) {
            delimiter_bytes = delimiter.getBytes();
        }
        
        LineReader reader = null;
        try {
            InputStream in = syndicateFS.getFileInputStream(path);
            reader = new LineReader(in, conf, delimiter_bytes);
            Text line = new Text();
            int numLines = 0;
            long begin = 0;
            long length = 0;
            int num = -1;
            while ((num = reader.readLine(line)) > 0) {
                numLines++;
                length += num;
                if (numLines == numLinesPerSplit) {
                    if (begin == 0) {
                        splits.add(new SyndicateInputSplit(syndicateFS, path, begin, length - 1));
                    } else {
                        splits.add(new SyndicateInputSplit(syndicateFS, path, begin - 1, length));
                    }
                    begin += length;
                    length = 0;
                    numLines = 0;
                }
            }
            if (numLines != 0) {
                splits.add(new SyndicateInputSplit(syndicateFS, path, begin, length));
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        return splits;
    }
}
