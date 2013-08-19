/*
 * Text Output Format for Syndicate
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
