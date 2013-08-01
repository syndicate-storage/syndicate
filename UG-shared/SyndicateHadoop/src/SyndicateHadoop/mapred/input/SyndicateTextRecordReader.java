/*
 * Text Record Reader class for Syndicate
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
package SyndicateHadoop.mapred.input;

import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSPath;
import SyndicateHadoop.input.SyndicateInputSplit;
import SyndicateHadoop.util.FileSystemUtil;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.IOException;
import java.io.InputStream;
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
    
    private long start;
    private long pos;
    private long end;
    private LineReader reader;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private byte[] recordDelimiterBytes;
    
    public SyndicateTextRecordReader() {
    }

    public SyndicateTextRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        if (!(genericSplit instanceof SyndicateInputSplit)) {
            throw new IllegalArgumentException("Creation of a new RecordReader requires a SyndicateInputSplit instance.");
        }

        SyndicateInputSplit split = (SyndicateInputSplit) genericSplit;
        Configuration conf = context.getConfiguration();

        this.maxLineLength = SyndicateConfigUtil.getTextInputMaxLength(conf);
        this.start = split.getStart();
        this.end = this.start + split.getLength();

        JSFSPath path = split.getPath();

        JSFSFileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(conf);
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }

        InputStream is = syndicateFS.getFileInputStream(path);

        boolean skipFirstLine = false;
        if (this.start != 0) {
            skipFirstLine = true;
            --this.start;
            is.skip(this.start);
            //is.seek(this.start);
        }

        int bufferSize = (int) syndicateFS.getBlockSize();
        if (SyndicateConfigUtil.getFileReadBufferSize(conf) != 0) {
            bufferSize = SyndicateConfigUtil.getFileReadBufferSize(conf);
        }

        if (this.recordDelimiterBytes == null) {
            this.reader = new LineReader(is, conf);
        } else {
            this.reader = new LineReader(is, conf, this.recordDelimiterBytes);
        }
        
        this.reader = new LineReader(is, bufferSize);

        if (skipFirstLine) {
            // skip first line and re-establish "start".
            this.start += this.reader.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, this.end - this.start));
        }
        this.pos = this.start;
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.reader != null) {
            this.reader.close();
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
            newSize = this.reader.readLine(this.value, this.maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));
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
