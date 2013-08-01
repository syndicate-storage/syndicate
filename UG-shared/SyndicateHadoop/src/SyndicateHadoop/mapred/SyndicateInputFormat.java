/*
 * Input Format for Syndicate
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
import JSyndicateFS.JSFSFilenameFilter;
import JSyndicateFS.JSFSPath;
import SyndicateHadoop.input.SyndicateInputSplit;
import SyndicateHadoop.util.FileSystemUtil;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author iychoi
 */
public abstract class SyndicateInputFormat<K extends Object, V extends Object> extends InputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(SyndicateInputFormat.class);
    
    private static final double SPLIT_SLOP = 1.1;   // 10% slop
    private static final long MEGABYTE = 1024 * 1024;

    private static final JSFSFilenameFilter hiddenFileFilter = new JSFSFilenameFilter() {

        @Override
        public boolean accept(JSFSPath dir, String name) {
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    private static class MultiPathFilter implements JSFSFilenameFilter {

        private List<JSFSFilenameFilter> filters;

        public MultiPathFilter(List<JSFSFilenameFilter> filters) {
            this.filters = filters;
        }

        @Override
        public boolean accept(JSFSPath dir, String name) {
            for (JSFSFilenameFilter filter : filters) {
                if (!filter.accept(dir, name)) {
                    return false;
                }
            }
            return true;
        }
    }

    private long getFormatMinSplitSize() {
        return MEGABYTE;
    }

    private JSFSFilenameFilter getInputPathFilter(JobContext context) {
        Configuration conf = context.getConfiguration();
        Class<? extends JSFSFilenameFilter> filterClass = SyndicateConfigUtil.getInputPathFilter(conf);
        if(filterClass != null)
            return (JSFSFilenameFilter) ReflectionUtils.newInstance(filterClass, conf);
        else
            return null;
    }

    protected ArrayList<JSFSPath> listFiles(JobContext context) throws IOException {
        ArrayList<JSFSPath> result = new ArrayList<JSFSPath>();
        JSFSPath[] dirs = SyndicateConfigUtil.getInputPaths(context.getConfiguration());
        if(dirs == null || dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        
        LOG.info("input paths length : " + dirs.length);
        for(JSFSPath dir : dirs) {
            LOG.info("input path : " + dir.getPath());
        }
        
        ArrayList<JSFSFilenameFilter> filters = new ArrayList<JSFSFilenameFilter>();
        
        // add hidden file filter by default
        filters.add(hiddenFileFilter);
        
        // add user filter
        JSFSFilenameFilter jobFilter = getInputPathFilter(context);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        
        // bind them together
        JSFSFilenameFilter inputFilter = new MultiPathFilter(filters);

        JSFSFileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(context.getConfiguration());
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        for(int i=0;i<dirs.length;++i) {
            JSFSPath path = dirs[i];
            
            JSFSPath[] paths = syndicateFS.listAllFiles(path, inputFilter);
            if(paths != null) {
                result.addAll(Arrays.asList(paths)); 
            }
        }

        LOG.info("Total input paths to process : " + result.size());
        
        return result;
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration config = context.getConfiguration();
        
        long minSize = Math.max(getFormatMinSplitSize(), SyndicateConfigUtil.getMinInputSplitSize(config));
        long maxSize = SyndicateConfigUtil.getMaxInputSplitSize(config);
        
        JSFSFileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(config);
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for(JSFSPath path : listFiles(context)) {
            long length = syndicateFS.getSize(path);
            long blockSize = syndicateFS.getBlockSize();
            long splitSize = computeSplitSize(blockSize, minSize, maxSize);
            
            if (length != 0) {
                long bytesLeft = length;
                while (((double) bytesLeft) / splitSize > SPLIT_SLOP) {
                    SyndicateInputSplit inputSplit = new SyndicateInputSplit(syndicateFS, path, length - bytesLeft, splitSize);
                    splits.add(inputSplit);
                    
                    bytesLeft -= splitSize;
                }

                if (bytesLeft != 0) {
                    SyndicateInputSplit inputSplit = new SyndicateInputSplit(syndicateFS, path, length - bytesLeft, bytesLeft);
                    splits.add(inputSplit);
                }
            }
        }
        
        LOG.info("Total # of splits: " + splits.size());
        return splits;
    }
    
    private long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return Math.max(minSize, Math.min(maxSize, blockSize));
    }
}
