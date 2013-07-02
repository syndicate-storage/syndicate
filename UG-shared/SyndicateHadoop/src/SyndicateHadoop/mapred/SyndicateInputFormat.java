/*
 * Input Format for Syndicate
 */
package SyndicateHadoop.mapred;

import JSyndicateFS.File;
import JSyndicateFS.FileSystem;
import JSyndicateFS.FilenameFilter;
import JSyndicateFS.Path;
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

    private static final FilenameFilter hiddenFileFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    private static class MultiPathFilter implements FilenameFilter {

        private List<FilenameFilter> filters;

        public MultiPathFilter(List<FilenameFilter> filters) {
            this.filters = filters;
        }

        @Override
        public boolean accept(File dir, String name) {
            for (FilenameFilter filter : filters) {
                if (!filter.accept(dir, name)) {
                    return false;
                }
            }
            return true;
        }
    }

    private long getFormatMinSplitSize() {
        return 1;
    }

    private FilenameFilter getInputPathFilter(JobContext context) {
        Configuration conf = context.getConfiguration();
        Class<? extends FilenameFilter> filterClass = SyndicateConfigUtil.getInputPathFilter(conf);
        if(filterClass != null)
            return (FilenameFilter) ReflectionUtils.newInstance(filterClass, conf);
        else
            return null;
    }

    private ArrayList<Path> listFiles(JobContext context) throws IOException {
        ArrayList<Path> result = new ArrayList<Path>();
        Path[] dirs = SyndicateConfigUtil.getInputPaths(context.getConfiguration());
        if(dirs == null || dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        
        ArrayList<FilenameFilter> filters = new ArrayList<FilenameFilter>();
        
        // add hidden file filter by default
        filters.add(hiddenFileFilter);
        
        // add user filter
        FilenameFilter jobFilter = getInputPathFilter(context);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        
        // bind them together
        FilenameFilter inputFilter = new MultiPathFilter(filters);

        FileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(context.getConfiguration());
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        for(int i=0;i<dirs.length;++i) {
            Path path = dirs[i];
            
            Path[] paths = syndicateFS.listAllFiles(path, inputFilter);
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
        
        FileSystem syndicateFS = null;
        try {
            syndicateFS = FileSystemUtil.getFileSystem(config);
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for(Path path : listFiles(context)) {
            File file = new File(syndicateFS, path);
            
            long length = file.getSize();
            long blockSize = file.getBlockSize();
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
        
        LOG.debug("Total # of splits: " + splits.size());
        return splits;
    }
    
    private long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return Math.max(minSize, Math.min(maxSize, blockSize));
    }
}
