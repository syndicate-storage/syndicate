/*
 * Key-Value Text Input Format for Syndicate
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

import SyndicateHadoop.input.SyndicateInputSplit;
import SyndicateHadoop.mapred.input.SyndicateKeyValueLineRecordReader;
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
public class SyndicateKeyValueTextInputFormat extends SyndicateInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (!(split instanceof SyndicateInputSplit))
            throw new IllegalStateException("Creation of a new RecordReader requires a SyndicateInputSplit instance.");
        
        Configuration conf = context.getConfiguration();
        return new SyndicateKeyValueLineRecordReader(conf);
    }
}
