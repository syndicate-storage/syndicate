/*
 * Configuration helper tool for Syndicate
 */
package SyndicateHadoop.util;

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
    
    public static final String CONFIG_FILE_KEY = "syndicate.conf.config_file";
    public static final String PASSWORD_KEY = "syndicate.conf.password";
    public static final String USERNAME_KEY = "syndicate.conf.username";
    public static final String VOLUME_NAME_KEY = "syndicate.conf.volume_name";
    public static final String VOLUME_SECRET_KEY = "syndicate.conf.volume_secret";
    public static final String MSURL_KEY = "syndicate.conf.ms_url";
    public static final String PORT_KEY = "syndicate.conf.port";
    public static final String MAX_METADATA_CACHE_KEY = "syndicate.conf.max_metadata_cache";
    public static final String TIMEOUT_METADATA_CACHE_KEY = "syndicate.conf.timeout_metadata_cache";
    public static final String FILE_READ_BUFFER_SIZE_KEY = "syndicate.conf.file_read_buffer_size";
    public static final String FILE_WRITE_BUFFER_SIZE_KEY = "syndicate.conf.file_write_buffer_size";
    
    public static final String JOB_MAPPER = "syndicate.job.mapper";
    public static final String JOB_COMBINER = "syndicate.job.combiner";
    public static final String JOB_PARTITIONER = "syndicate.job.partitioner";
    public static final String JOB_REDUCER = "syndicate.job.reducer";
    public static final String JOB_SORT_COMPARATOR = "syndicate.job.sort_comparator";

    public static final String JOB_MAPPER_OUTPUT_KEY = "syndicate.job.mapper.output.key";
    public static final String JOB_MAPPER_OUTPUT_VALUE = "syndicate.job.mapper.output.value";

    public static final String JOB_INPUT_FORMAT = "syndicate.job.input.format";
    public static final String JOB_OUTPUT_FORMAT = "syndicate.job.output.format";

    public static final String JOB_OUTPUT_KEY = "syndicate.job.output.key";
    public static final String JOB_OUTPUT_VALUE = "syndicate.job.output.value";

    
    public static void setConfigFile(Configuration conf, String path) {
        conf.set(CONFIG_FILE_KEY, path);
    }
    
    public static String getConfigFile(Configuration conf) {
        return conf.get(CONFIG_FILE_KEY);
    }
    
    public static void setPassword(Configuration conf, String password) {
        conf.set(PASSWORD_KEY, password);
    }
    
    public static String getPassword(Configuration conf) {
        return conf.get(PASSWORD_KEY);
    }
    
    public static void setUserName(Configuration conf, String username) {
        conf.set(USERNAME_KEY, username);
    }
    
    public static String getUserName(Configuration conf) {
        return conf.get(USERNAME_KEY);
    }
    
    public static void setVolumeName(Configuration conf, String volumename) {
        conf.set(VOLUME_NAME_KEY, volumename);
    }
    
    public static String getVolumeName(Configuration conf) {
        return conf.get(VOLUME_NAME_KEY);
    }
    
    public static void setVolumeSecret(Configuration conf, String volumesecret) {
        conf.set(VOLUME_SECRET_KEY, volumesecret);
    }
    
    public static String getVolumeSecret(Configuration conf) {
        return conf.get(VOLUME_SECRET_KEY);
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
        
        conf.set(MSURL_KEY, msurl.toString());
    }
    
    public static String getMSUrl(Configuration conf) {
        return conf.get(MSURL_KEY);
    }
    
    public static void setPort(Configuration conf, int port) {
        conf.setInt(PORT_KEY, port);
    }
    
    public static int getPort(Configuration conf) {
        return conf.getInt(PORT_KEY, 0);
    }
    
    public static void setMaxMetadataCacheNum(Configuration conf, int maxCache) {
        conf.setInt(MAX_METADATA_CACHE_KEY, maxCache);
    }
    
    public static int getMaxMetadataCacheNum(Configuration conf) {
        return conf.getInt(MAX_METADATA_CACHE_KEY, 0);
    }
    
    public static void setMetadataCacheTimeout(Configuration conf, int timeoutSec) {
        conf.setInt(TIMEOUT_METADATA_CACHE_KEY, timeoutSec);
    }
    
    public static int getMetadataCacheTimeout(Configuration conf) {
        return conf.getInt(TIMEOUT_METADATA_CACHE_KEY, 0);
    }
    
    public static void setFileReadBufferSize(Configuration conf, int bufferSize) {
        conf.setInt(FILE_READ_BUFFER_SIZE_KEY, bufferSize);
    }
    
    public static int getFileReadBufferSize(Configuration conf) {
        return conf.getInt(FILE_READ_BUFFER_SIZE_KEY, 0);
    }
    
    public static void setFileWriteBufferSize(Configuration conf, int bufferSize) {
        conf.setInt(FILE_WRITE_BUFFER_SIZE_KEY, bufferSize);
    }
    
    public static int getFileWriteBufferSize(Configuration conf) {
        return conf.getInt(FILE_WRITE_BUFFER_SIZE_KEY, 0);
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
}
