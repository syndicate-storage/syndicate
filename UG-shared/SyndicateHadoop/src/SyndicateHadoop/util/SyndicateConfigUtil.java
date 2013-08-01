/*
 * Configuration helper tool for Syndicate
 */
package SyndicateHadoop.util;

import JSyndicateFS.JSFSConfiguration;
import JSyndicateFS.JSFSFilenameFilter;
import JSyndicateFS.JSFSPath;
import JSyndicateFS.backend.ipc.IPCConfiguration;
import JSyndicateFS.backend.sharedfs.SharedFSConfiguration;
import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class SyndicateConfigUtil {
    
    public static final Log LOG = LogFactory.getLog(SyndicateConfigUtil.class);
    
    private static JSFSConfiguration instance;
    
    public static enum Backend {
        IPC(0), SHARED_FS(1);
        
        private int code = -1;
        
        private Backend(int c) {
            this.code = c;
        }

        public int getCode() {
            return this.code;
        }

        public static Backend getFrom(int code) {
            Backend[] backends = Backend.values();
            for(Backend backend : backends) {
                if(backend.getCode() == code)
                    return backend;
            }
            return null;
        }
    }

    public static final String BACKEND = "syndicate.conf.backend";
    public static final String IPC_UG_NAME = "syndicate.conf.ipc.ug.name";
    public static final String IPC_PORT = "syndicate.conf.ipc.port";
    public static final String SFS_MOUNT_PATH = "syndicate.conf.sfs.mountpath";
    
    public static final String MAX_METADATA_CACHE = "syndicate.conf.max_metadata_cache";
    public static final String TIMEOUT_METADATA_CACHE = "syndicate.conf.timeout_metadata_cache";
    public static final String FILE_READ_BUFFER_SIZE = "syndicate.conf.file_read_buffer_size";
    public static final String FILE_WRITE_BUFFER_SIZE = "syndicate.conf.file_write_buffer_size";
    
    public static final String JOB_MAPPER = "mapreduce.map.class";
    public static final String JOB_COMBINER = "mapreduce.combine.class";
    public static final String JOB_PARTITIONER = "mapreduce.partitioner.class";
    public static final String JOB_REDUCER = "mapreduce.reduce.class";
    public static final String JOB_SORT_COMPARATOR = "mapred.output.key.comparator.class";

    public static final String JOB_MAPPER_OUTPUT_KEY = "mapred.mapoutput.key.class";
    public static final String JOB_MAPPER_OUTPUT_VALUE = "mapred.mapoutput.value.class";

    public static final String JOB_INPUT_FORMAT = "mapreduce.inputformat.class";
    public static final String JOB_OUTPUT_FORMAT = "mapreduce.outputformat.class";

    public static final String JOB_OUTPUT_KEY = "mapred.mapoutput.key.class";
    public static final String JOB_OUTPUT_VALUE = "mapred.mapoutput.value.class";
    
    public static final String MIN_INPUT_SPLIT_SIZE = "mapred.min.split.size";
    public static final String MAX_INPUT_SPLIT_SIZE = "mapred.max.split.size";

    public static final String INPUT_DIR = "mapred.input.dir";
    public static final String INPUT_PATH_FILTER = "mapred.input.pathFilter.class";
    public static final String OUTPUT_DIR = "mapred.output.dir";
    public static final String BASE_OUTPUT_NAME = "mapreduce.output.basename";
    
    public static final String TEXT_OUTPUT_FORMAT_SEPARATOR = "mapred.textoutputformat.separator";
    
    public static final String TEXT_INPUT_MAX_LENGTH = "mapred.linerecordreader.maxlength";
    
    public static final String TEXT_RECORD_DELIMITER = "textinputformat.record.delimiter";
    public static final String TEXT_LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";
    public static final String TEXT_KEY_VALUE_SEPARATOR = "key.value.separator.in.input.line";
    
    
    public static final long MEGABYTE = 1024 * 1024;
    
    public synchronized static JSFSConfiguration getJSFSConfigurationInstance(Configuration conf) throws InstantiationException {
        if(instance == null) {
            Backend backend = getBackend(conf);
            if(backend == null) {
                LOG.error("null backend");
                throw new InstantiationException("null backend");
            }
            if(backend.equals(Backend.IPC)) {
                IPCConfiguration ipc_conf = new IPCConfiguration();
                try {
                    ipc_conf.setUGName(getIPC_UGName(conf));
                    ipc_conf.setMaxMetadataCacheSize(getMaxMetadataCacheNum(conf));
                    ipc_conf.setCacheTimeoutSecond(getMetadataCacheTimeout(conf));
                } catch (IllegalAccessException ex) {
                    LOG.error(ex);
                    throw new InstantiationException(ex.getMessage());
                }
                
                instance = ipc_conf;
                
                initJSFSConfiguration(conf, instance);
                
            } else if(backend.equals(Backend.SHARED_FS)) {
                SharedFSConfiguration sfs_conf = new SharedFSConfiguration();
                try {
                    String mountPath = getSFS_MountPath(conf);
                    File mountFile = new File(mountPath);
                    if(!(mountFile.exists() && mountFile.isDirectory())) {
                        LOG.error("mount point not exist");
                        throw new InstantiationException("mount point not exist");
                    }
                    sfs_conf.setMountPoint(mountFile);
                } catch (IllegalAccessException ex) {
                    LOG.error(ex);
                    throw new InstantiationException(ex.getMessage());
                }
                
                instance = sfs_conf;
                
                initJSFSConfiguration(conf, instance);
            } else {
                LOG.error("invalid backend");
                throw new InstantiationException("invalid backend");
            }
        }
        
        return instance;
    }
    
    private synchronized static void initJSFSConfiguration(Configuration conf, JSFSConfiguration jsfs_config) throws InstantiationException {
        int readBufferSize = getFileReadBufferSize(conf);
        try {
            jsfs_config.setReadBufferSize(readBufferSize);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
        
        int writeBufferSize = getFileWriteBufferSize(conf);
        try {
            jsfs_config.setWriteBufferSize(writeBufferSize);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
    }
    
    public static void setBackend(Configuration conf, Backend backend) {
        conf.setInt(BACKEND, backend.getCode());
    }
    
    public static Backend getBackend(Configuration conf) {
        int code = conf.getInt(BACKEND, 0);
        return Backend.getFrom(code);
    }
    
    public static void setIPC_Port(Configuration conf, int port) {
        conf.setInt(IPC_PORT, port);
    }
    
    public static int getIPC_Port(Configuration conf) {
        return conf.getInt(IPC_PORT, IPCConfiguration.DEFAULT_IPC_PORT);
    }
    
    public static void setIPC_UGName(Configuration conf, String ug_name) {
        conf.set(IPC_UG_NAME, ug_name);
    }
    
    public static String getIPC_UGName(Configuration conf) {
        return conf.get(IPC_UG_NAME);
    }
    
    public static void setSFS_MountPath(Configuration conf, String path) {
        conf.set(SFS_MOUNT_PATH, path);
    }
    
    public static String getSFS_MountPath(Configuration conf) {
        return conf.get(SFS_MOUNT_PATH);
    }
    
    public static void setMaxMetadataCacheNum(Configuration conf, int maxCache) {
        conf.setInt(MAX_METADATA_CACHE, maxCache);
    }
    
    public static int getMaxMetadataCacheNum(Configuration conf) {
        return conf.getInt(MAX_METADATA_CACHE, 0);
    }
    
    public static void setMetadataCacheTimeout(Configuration conf, int timeoutSec) {
        conf.setInt(TIMEOUT_METADATA_CACHE, timeoutSec);
    }
    
    public static int getMetadataCacheTimeout(Configuration conf) {
        return conf.getInt(TIMEOUT_METADATA_CACHE, 0);
    }
    
    public static void setFileReadBufferSize(Configuration conf, int bufferSize) {
        conf.setInt(FILE_READ_BUFFER_SIZE, bufferSize);
    }
    
    public static int getFileReadBufferSize(Configuration conf) {
        return conf.getInt(FILE_READ_BUFFER_SIZE, 0);
    }
    
    public static void setFileWriteBufferSize(Configuration conf, int bufferSize) {
        conf.setInt(FILE_WRITE_BUFFER_SIZE, bufferSize);
    }
    
    public static int getFileWriteBufferSize(Configuration conf) {
        return conf.getInt(FILE_WRITE_BUFFER_SIZE, 0);
    }
    
    public static Class<? extends Mapper> getMapper(Configuration conf) {
        return conf.getClass(JOB_MAPPER, null, Mapper.class);
    }

    public static void setMapper(Configuration conf, Class<? extends Mapper> val) {
        conf.setClass(JOB_MAPPER, val, Mapper.class);
    }

    public static Class<?> getMapperOutputKey(Configuration conf) {
        return conf.getClass(JOB_MAPPER_OUTPUT_KEY, null);
    }

    public static void setMapperOutputKey(Configuration conf, Class<?> val) {
        conf.setClass(JOB_MAPPER_OUTPUT_KEY, val, Object.class);
    }

    public static Class<?> getMapperOutputValue(Configuration conf) {
        return conf.getClass(JOB_MAPPER_OUTPUT_VALUE, null);
    }

    public static void setMapperOutputValue(Configuration conf, Class<?> val) {
        conf.setClass(JOB_MAPPER_OUTPUT_VALUE, val, Object.class);
    }

    public static Class<? extends Reducer> getCombiner(Configuration conf) {
        return conf.getClass(JOB_COMBINER, null, Reducer.class);
    }

    public static void setCombiner(Configuration conf, Class<? extends Reducer> val) {
        conf.setClass(JOB_COMBINER, val, Reducer.class);
    }

    public static Class<? extends Reducer> getReducer(Configuration conf) {
        return conf.getClass(JOB_REDUCER, null, Reducer.class);
    }

    public static void setReducer(Configuration conf, Class<? extends Reducer> val) {
        conf.setClass(JOB_REDUCER, val, Reducer.class);
    }

    public static Class<? extends Partitioner> getPartitioner(Configuration conf) {
        return conf.getClass(JOB_PARTITIONER, null, Partitioner.class);
    }

    public static void setPartitioner(Configuration conf, Class<? extends Partitioner> val) {
        conf.setClass(JOB_PARTITIONER, val, Partitioner.class);
    }

    public static Class<? extends RawComparator> getSortComparator(Configuration conf) {
        return conf.getClass(JOB_SORT_COMPARATOR, null, RawComparator.class);
    }

    public static void setSortComparator(Configuration conf, Class<? extends RawComparator> val) {
        conf.setClass(JOB_SORT_COMPARATOR, val, RawComparator.class);
    }

    public static Class<? extends OutputFormat> getOutputFormat(Configuration conf) {
        return conf.getClass(JOB_OUTPUT_FORMAT, null, OutputFormat.class);
    }

    public static void setOutputFormat(Configuration conf, Class<? extends OutputFormat> val) {
        conf.setClass(JOB_OUTPUT_FORMAT, val, OutputFormat.class);
    }

    public static Class<?> getOutputKey(Configuration conf) {
        return conf.getClass(JOB_OUTPUT_KEY, null);
    }

    public static void setOutputKey(Configuration conf, Class<?> val) {
        conf.setClass(JOB_OUTPUT_KEY, val, Object.class);
    }

    public static Class<?> getOutputValue(Configuration conf) {
        return conf.getClass(JOB_OUTPUT_VALUE, null);
    }

    public static void setOutputValue(Configuration conf, Class<?> val) {
        conf.setClass(JOB_OUTPUT_VALUE, val, Object.class);
    }

    public static Class<? extends InputFormat> getInputFormat(Configuration conf) {
        return conf.getClass(JOB_INPUT_FORMAT, null, InputFormat.class);
    }

    public static void setInputFormat(Configuration conf, Class<? extends InputFormat> val) {
        conf.setClass(JOB_INPUT_FORMAT, val, InputFormat.class);
    }
    
    public static void setMinInputSplitSize(Configuration conf, long minSize) {
        conf.setLong(MIN_INPUT_SPLIT_SIZE, minSize);
    }
    
    public static long getMinInputSplitSize(Configuration conf) {
        return conf.getLong(MIN_INPUT_SPLIT_SIZE, MEGABYTE);
    }
    
    public static void setMaxInputSplitSize(Configuration conf, long maxSize) {
        conf.setLong(MAX_INPUT_SPLIT_SIZE, maxSize);
    }
    
    public static long getMaxInputSplitSize(Configuration conf) {
        return conf.getLong(MAX_INPUT_SPLIT_SIZE, Long.MAX_VALUE);
    }

    public static void setInputPaths(Configuration conf, String pathstrings) throws IOException {
        String[] pathstr = StringUtil.getPathStrings(pathstrings);
        JSFSPath[] patharr = StringUtil.stringToPath(pathstr);
        setInputPaths(conf, patharr);
    }
    
    public static void addInputPaths(Configuration conf, String pathstrings) throws IOException {
        String[] pathstr = StringUtil.getPathStrings(pathstrings);
        for(String str : pathstr) {
            addInputPath(conf, StringUtil.stringToPath(str));
        }
    }
    
    public static void setInputPaths(Configuration conf, JSFSPath... inputPaths) throws IOException {
        String path = StringUtil.generatePathString(inputPaths);
        conf.set(INPUT_DIR, path);
    }
    
    public static void addInputPath(Configuration conf, JSFSPath inputPath) throws IOException {
        String dirs = conf.get(INPUT_DIR);
        
        String newDirs = StringUtil.addPathString(dirs, inputPath);
        conf.set(INPUT_DIR, newDirs);
    }
    
    public static JSFSPath[] getInputPaths(Configuration conf) {
        String dirs = conf.get(INPUT_DIR, "");
        
        String[] list = StringUtil.getPathStrings(dirs);
        return StringUtil.stringToPath(list);
    }
    
    public static void setInputPathFilter(Configuration conf, Class<? extends JSFSFilenameFilter> filter) {
        conf.setClass(INPUT_PATH_FILTER, filter, JSFSFilenameFilter.class);
    }
    
    public static Class<? extends JSFSFilenameFilter> getInputPathFilter(Configuration conf) {
        return conf.getClass(INPUT_PATH_FILTER, null, JSFSFilenameFilter.class);
    }
    
    public static void setOutputPath(Configuration conf, String outputPath) {
        conf.set(OUTPUT_DIR, outputPath);
    }
    
    public static void setOutputPath(Configuration conf, JSFSPath outputPath) {
        conf.set(OUTPUT_DIR, outputPath.getPath());
    }

    public static JSFSPath getOutputPath(Configuration conf) {
        return StringUtil.stringToPath(conf.get(OUTPUT_DIR));
    }
    
    public static void setOutputBaseName(Configuration conf, String basename) {
        conf.set(BASE_OUTPUT_NAME, basename);
    }
    
    public static String getOutputBaseName(Configuration conf) {
        return conf.get(BASE_OUTPUT_NAME, "part");
    }
    
    public static void setTextOutputFormatSeparator(Configuration conf, String separator) {
        conf.set(TEXT_OUTPUT_FORMAT_SEPARATOR, separator);
    }
    
    public static String getTextOutputFormatSeparator(Configuration conf) {
        return conf.get(TEXT_OUTPUT_FORMAT_SEPARATOR, "\t");
    }
    
    public static void setTextInputMaxLength(Configuration conf, int maxlength) {
        conf.setInt(TEXT_INPUT_MAX_LENGTH, maxlength);
    }
    
    public static int getTextInputMaxLength(Configuration conf) {
        return conf.getInt(TEXT_INPUT_MAX_LENGTH, Integer.MAX_VALUE);
    }
    
    public static void setTextRecordDelimiter(Configuration conf, String delimiter) {
        conf.set(TEXT_RECORD_DELIMITER, delimiter);
    }
    
    public static String getTextRecordDelimiter(Configuration conf) {
        return conf.get(TEXT_RECORD_DELIMITER, null);
    }
    
    public static void setTextLinesPerMap(Configuration conf, int lines) {
        conf.setInt(TEXT_LINES_PER_MAP, lines);
    }
    
    public static int getTextLinesPerMap(Configuration conf) {
        return conf.getInt(TEXT_LINES_PER_MAP, 1);
    }
    
    public static void setTextKeyValueSeparator(Configuration conf, byte separator) {
        String separatorString = new String(new byte[]{separator});
        conf.set(TEXT_KEY_VALUE_SEPARATOR, separatorString);
    }
    
    public static byte getTextKeyValueSeparator(Configuration conf) {
        String separatorString = conf.get(TEXT_KEY_VALUE_SEPARATOR, "\t");
        return (byte)separatorString.charAt(0);
    }
}
