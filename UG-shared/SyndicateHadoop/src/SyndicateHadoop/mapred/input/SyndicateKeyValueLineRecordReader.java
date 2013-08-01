/*
 * Key Value Line Reader class for Syndicate
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package SyndicateHadoop.mapred.input;

import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class SyndicateKeyValueLineRecordReader extends RecordReader<Text, Text> {

    private final SyndicateTextRecordReader lineRecordReader;
    private byte separator;
    private Text innerValue;
    private Text key;
    private Text value;

    public Class<?> getKeyClass() {
        return Text.class;
    }

    public SyndicateKeyValueLineRecordReader(Configuration conf) {
        this.lineRecordReader = new SyndicateTextRecordReader();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.separator = SyndicateConfigUtil.getTextKeyValueSeparator(conf);
        
        this.lineRecordReader.initialize(split, context);
    }
    
    public static int findSeparator(byte[] utf, int start, int length, byte sep) {
        for (int i = start; i < (start + length); i++) {
            if (utf[i] == sep) {
                return i;
            }
        }
        return -1;
    }

    public static void setKeyValue(Text key, Text value, byte[] line, int lineLen, int pos) {
        if (pos == -1) {
            key.set(line, 0, lineLen);
            value.set("");
        } else {
            int keyLen = pos;
            byte[] keyBytes = new byte[keyLen];
            System.arraycopy(line, 0, keyBytes, 0, keyLen);
            int valLen = lineLen - keyLen - 1;
            byte[] valBytes = new byte[valLen];
            System.arraycopy(line, pos + 1, valBytes, 0, valLen);
            key.set(keyBytes);
            value.set(valBytes);
        }
    }

    @Override
    public synchronized boolean nextKeyValue() throws IOException, InterruptedException {
        byte[] line = null;
        int lineLen = -1;
        if (this.lineRecordReader.nextKeyValue()) {
            this.innerValue = this.lineRecordReader.getCurrentValue();
            line = this.innerValue.getBytes();
            lineLen = this.innerValue.getLength();
        } else {
            return false;
        }
        if (line == null) {
            return false;
        }
        if (this.key == null) {
            this.key = new Text();
        }
        if (this.value == null) {
            this.value = new Text();
        }
        int pos = findSeparator(line, 0, lineLen, this.separator);
        setKeyValue(this.key, this.value, line, lineLen, pos);
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this.lineRecordReader.close();
    }
}
