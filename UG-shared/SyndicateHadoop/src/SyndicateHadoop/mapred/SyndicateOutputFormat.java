/*
 * Output Format for Syndicate
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
import SyndicateHadoop.output.SyndicateOutputCommitter;
import SyndicateHadoop.util.FileSystemUtil;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.IOException;
import java.text.NumberFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;

/**
 *
 * @author iychoi
 */
public abstract class SyndicateOutputFormat<K, V> extends OutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(SyndicateOutputFormat.class);
    
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    
    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }
    
    private SyndicateOutputCommitter committer = null;
    
    @Override
    public void checkOutputSpecs(JobContext context) throws FileAlreadyExistsException, IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        JSFSPath outDir = SyndicateConfigUtil.getOutputPath(conf);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
        
        JSFSFileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(context.getConfiguration());
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        if(syndicateFS.exists(outDir)) {
            throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (this.committer == null) {
            JSFSPath output = SyndicateConfigUtil.getOutputPath(context.getConfiguration());
            
            JSFSFileSystem syndicateFS = null;
            try {
                syndicateFS = FileSystemUtil.getFileSystem(context.getConfiguration());
            } catch (InstantiationException ex) {
                throw new IOException(ex);
            }
            
            this.committer = new SyndicateOutputCommitter(syndicateFS, output, context);
        }
        return this.committer;
    }
    
    public JSFSPath getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        SyndicateOutputCommitter committer = (SyndicateOutputCommitter) getOutputCommitter(context);
        Configuration conf = context.getConfiguration();
        String basename = SyndicateConfigUtil.getOutputBaseName(conf);
        String uniquename = getUniqueFile(context, basename, extension);
        return new JSFSPath(committer.getOutputPath(), uniquename);
    }
    
    public synchronized static String getUniqueFile(TaskAttemptContext context, String name, String extension) {
        TaskID taskId = context.getTaskAttemptID().getTaskID();
        int partition = taskId.getId();
        StringBuilder result = new StringBuilder();
        result.append(name);
        result.append('-');
        result.append(taskId.isMap() ? 'm' : 'r');
        result.append('-');
        result.append(NUMBER_FORMAT.format(partition));
        result.append(extension);
        return result.toString();
    }
}
