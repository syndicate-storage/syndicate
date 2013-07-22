/*
 * Configuration helper tool for Syndicate
 */
package SyndicateHadoop.util;

import JSyndicateFS.FilenameFilter;
import JSyndicateFS.Path;
import java.io.IOException;
import java.net.URL;
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
    
    public static final String CONFIG_FILE = "syndicate.conf.config_file";
    public static final String UG_PASSWORD = "syndicate.conf.ug.password";
    public static final String UG_NAME_PREFIX = "syndicate.conf.ug.name.prefix";
    public static final String VOLUME_NAME = "syndicate.conf.volume.name";
    public static final String VOLUME_SECRET = "syndicate.conf.volume.secret";
    public static final String MSURL = "syndicate.conf.ms_url";
    public static final String PORT = "syndicate.conf.port";
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
    
    public static void setConfigFile(Configuration conf, String path) {
        conf.set(CONFIG_FILE, path);
    }
    
    public static String getConfigFile(Configuration conf) {
        return conf.get(CONFIG_FILE);
    }
    
    public static void setUGPassword(Configuration conf, String ug_password) {
        conf.set(UG_PASSWORD, ug_password);
    }
    
    public static String getUGPassword(Configuration conf) {
        return conf.get(UG_PASSWORD);
    }
    
    public static void setUGNamePrefix(Configuration conf, String ug_name_prefix) {
        conf.set(UG_NAME_PREFIX, ug_name_prefix);
    }
    
    public static String getUGNamePrefix(Configuration conf) {
        return conf.get(UG_NAME_PREFIX);
    }
    
    public static String getUGName(Configuration conf) {
        String prefix = getUGNamePrefix(conf);
        return UGNameUtil.getUGName(prefix);
    }
    
    public static void setVolumeName(Configuration conf, String volume_name) {
        conf.set(VOLUME_NAME, volume_name);
    }
    
    public static String getVolumeName(Configuration conf) {
        return conf.get(VOLUME_NAME);
    }
    
    public static void setVolumeSecret(Configuration conf, String volume_secret) {
        conf.set(VOLUME_SECRET, volume_secret);
    }
    
    public static String getVolumeSecret(Configuration conf) {
        return conf.get(VOLUME_SECRET);
    }
    
    public static void setMSUrl(Configuration conf, String msurl) {
        try {
            URL url = new URL(msurl);
            setMSUrl(conf, url);
        } catch(Exception ex) {
            throw new IllegalArgumentException("Given msurl is not a valid format");
        }
    }
    
    public static void setMSUrl(Configuration conf, URL msurl) {
        if(msurl == null)
            throw new IllegalArgumentException("Can not set url from null parameter");
        
        conf.set(MSURL, msurl.toString());
    }
    
    public static String getMSUrl(Configuration conf) {
        return conf.get(MSURL);
    }
    
    public static void setPort(Configuration conf, int port) {
        conf.setInt(PORT, port);
    }
    
    public static int getPort(Configuration conf) {
        return conf.getInt(PORT, 0);
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
        return conf.getLong(MIN_INPUT_SPLIT_SIZE, 1);
    }
    
    public static void setMaxInputSplitSize(Configuration conf, long maxSize) {
        conf.setLong(MAX_INPUT_SPLIT_SIZE, maxSize);
    }
    
    public static long getMaxInputSplitSize(Configuration conf) {
        return conf.getLong(MAX_INPUT_SPLIT_SIZE, Long.MAX_VALUE);
    }

    public static void setInputPaths(Configuration conf, String pathstrings) throws IOException {
        String[] pathstr = StringUtils.getPathStrings(pathstrings);
        Path[] patharr = StringUtils.stringToPath(pathstr);
        setInputPaths(conf, patharr);
    }
    
    public static void addInputPaths(Configuration conf, String pathstrings) throws IOException {
        String[] pathstr = StringUtils.getPathStrings(pathstrings);
        for(String str : pathstr) {
            addInputPath(conf, StringUtils.stringToPath(str));
        }
    }
    
    public static void setInputPaths(Configuration conf, Path... inputPaths) throws IOException {
        String path = StringUtils.generatePathString(inputPaths);
        conf.set(INPUT_DIR, path);
    }
    
    public static void addInputPath(Configuration conf, Path inputPath) throws IOException {
        String dirs = conf.get(INPUT_DIR);
        
        String newDirs = StringUtils.addPathString(dirs, inputPath);
        conf.set(INPUT_DIR, newDirs);
    }
    
    public static Path[] getInputPaths(Configuration conf) {
        String dirs = conf.get(INPUT_DIR, "");
        
        String[] list = StringUtils.getPathStrings(dirs);
        return StringUtils.stringToPath(list);
    }
    
    public static void setInputPathFilter(Configuration conf, Class<? extends FilenameFilter> filter) {
        conf.setClass(INPUT_PATH_FILTER, filter, FilenameFilter.class);
    }
    
    public static Class<? extends FilenameFilter> getInputPathFilter(Configuration conf) {
        return conf.getClass(INPUT_PATH_FILTER, null, FilenameFilter.class);
    }
    
    public static void setOutputPath(Configuration conf, String outputPath) {
        conf.set(OUTPUT_DIR, outputPath);
    }
    
    public static void setOutputPath(Configuration conf, Path outputPath) {
        conf.set(OUTPUT_DIR, outputPath.getPath());
    }

    public static Path getOutputPath(Configuration conf) {
        return StringUtils.stringToPath(conf.get(OUTPUT_DIR));
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
}
