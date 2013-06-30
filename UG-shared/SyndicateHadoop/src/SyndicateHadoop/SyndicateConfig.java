/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SyndicateHadoop;

import JSyndicateFS.FilenameFilter;
import JSyndicateFS.Path;
import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
public class SyndicateConfig {

    public static final Log LOG = LogFactory.getLog(SyndicateConfig.class);
    
    private Configuration config;
    
    public SyndicateConfig(Configuration config) {
        this.config = config;
    }
    
    public SyndicateConfig(DataInput in) throws IOException {
        this.config = new Configuration();
        this.config.readFields(in);
    }
    
    public String getConfigFile() {
        return SyndicateConfigUtil.getConfigFile(this.config);
    }
    
    public void setConfigFile(String val) {
        SyndicateConfigUtil.setConfigFile(this.config, val);
    }
    
    public String getPassword() {
        return SyndicateConfigUtil.getPassword(this.config);
    }
    
    public void setPassword(String val) {
        SyndicateConfigUtil.setPassword(this.config, val);
    }
    
    public String getUserName() {
        return SyndicateConfigUtil.getUserName(this.config);
    }
    
    public void setUserName(String val) {
        SyndicateConfigUtil.setUserName(this.config, val);
    }
    
    public String getVolumeName() {
        return SyndicateConfigUtil.getVolumeName(this.config);
    }
    
    public void setVolumeName(String val) {
        SyndicateConfigUtil.setVolumeName(this.config, val);
    }
    
    public String getVolumeSecret() {
        return SyndicateConfigUtil.getVolumeSecret(this.config);
    }
    
    public void setVolumeSecret(String val) {
        SyndicateConfigUtil.setVolumeSecret(this.config, val);
    }
    
    public String getMSUrl() {
        return SyndicateConfigUtil.getMSUrl(this.config);
    }
    
    public void setMSUrl(String val) {
        SyndicateConfigUtil.setMSUrl(this.config, val);
    }
    
    public void setMSUrl(URL val) {
        SyndicateConfigUtil.setMSUrl(this.config, val);
    }
    
    public int getPort() {
        return SyndicateConfigUtil.getPort(this.config);
    }
    
    public void setPort(int val) {
        SyndicateConfigUtil.setPort(this.config, val);
    }
    
    public int getMaxMetadataCacheNum() {
        return SyndicateConfigUtil.getMaxMetadataCacheNum(this.config);
    }
    
    public void setMaxMetadataCacheNum(int val) {
        SyndicateConfigUtil.setMaxMetadataCacheNum(this.config, val);
    }
    
    public int getMetadataCacheTimeout() {
        return SyndicateConfigUtil.getMetadataCacheTimeout(this.config);
    }
    
    public void setMetadataCacheTimeout(int val) {
        SyndicateConfigUtil.setMetadataCacheTimeout(this.config, val);
    }
    
    public int getFileReadBufferSize() {
        return SyndicateConfigUtil.getFileReadBufferSize(this.config);
    }
    
    public void setFileReadBufferSize(int val) {
        SyndicateConfigUtil.setFileReadBufferSize(this.config, val);
    }
    
    public int getFileWriteBufferSize() {
        return SyndicateConfigUtil.getFileWriteBufferSize(this.config);
    }
    
    public void setFileWriteBufferSize(int val) {
        SyndicateConfigUtil.setFileWriteBufferSize(this.config, val);
    }
    
    public JSyndicateFS.Configuration getJSFSConfiguration() {
        JSyndicateFS.Configuration jsfsConfig = new JSyndicateFS.Configuration();
        
        String configFile = getConfigFile();
        if(configFile != null) {
            File configFileObj = new File(configFile);
            try {
                jsfsConfig.setConfigFile(configFileObj);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
            }
        }
        
        String username = getUserName();
        if(username != null) {
            try {
                jsfsConfig.setUsername(username);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
            }
        }
        
        String password = getPassword();
        if(password != null) {
            try {
                jsfsConfig.setPassword(password);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
            }
        }
        
        String volumeName = getVolumeName();
        if(volumeName != null) {
            try {
                jsfsConfig.setVolumeName(volumeName);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
            }
        }
        
        String volumeSecret = getVolumeSecret();
        if(volumeSecret != null) {
            try {
                jsfsConfig.setVolumeSecret(volumeSecret);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
            }
        }
        
        String msUrl = getMSUrl();
        if(msUrl != null) {
            try {
                URI msurlObj = new URI(msUrl);
                jsfsConfig.setMSUrl(msurlObj);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
            } catch (URISyntaxException ex) {
                LOG.error(ex);
            }
        }
        
        int port = getPort();
        try {
            jsfsConfig.setPort(port);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
        
        int maxMetadataCacheSize = getMaxMetadataCacheNum();
        try {
            jsfsConfig.setMaxMetadataCacheSize(maxMetadataCacheSize);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
        
        int metadataCacheTimeout = getMetadataCacheTimeout();
        try {
            jsfsConfig.setCacheTimeoutSecond(metadataCacheTimeout);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
        
        int readBufferSize = this.getFileReadBufferSize();
        try {
            jsfsConfig.setReadBufferSize(readBufferSize);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
        
        int writeBufferSize = this.getFileWriteBufferSize();
        try {
            jsfsConfig.setWriteBufferSize(writeBufferSize);
        } catch (IllegalAccessException ex) {
            LOG.error(ex);
        }
        
        return jsfsConfig;
    }
    
    public Class<? extends Mapper> getMapper() {
        return SyndicateConfigUtil.getMapper(this.config);
    }

    public void setMapper(Class<? extends Mapper> val) {
        SyndicateConfigUtil.setMapper(this.config, val);
    }

    public Class<?> getMapperOutputKey() {
        return SyndicateConfigUtil.getMapperOutputKey(this.config);
    }

    public void setMapperOutputKey(Class<?> val) {
        SyndicateConfigUtil.setMapperOutputKey(this.config, val);
    }

    public Class<?> getMapperOutputValue() {
        return SyndicateConfigUtil.getMapperOutputValue(this.config);
    }

    public void setMapperOutputValue(Class<?> val) {
        SyndicateConfigUtil.setMapperOutputKey(this.config, val);
    }

    public Class<? extends Reducer> getCombiner() {
        return SyndicateConfigUtil.getCombiner(this.config);
    }

    public void setCombiner(Class<? extends Reducer> val) {
        SyndicateConfigUtil.setCombiner(this.config, val);
    }

    public Class<? extends Reducer> getReducer() {
        return SyndicateConfigUtil.getReducer(this.config);
    }

    public void setReducer(Class<? extends Reducer> val) {
        SyndicateConfigUtil.setReducer(this.config, val);
    }

    public Class<? extends Partitioner> getPartitioner() {
        return SyndicateConfigUtil.getPartitioner(this.config);
    }

    public void setPartitioner(Class<? extends Partitioner> val) {
        SyndicateConfigUtil.setPartitioner(this.config, val);
    }

    public Class<? extends RawComparator> getSortComparator() {
        return SyndicateConfigUtil.getSortComparator(this.config);
    }

    public void setSortComparator(Class<? extends RawComparator> val) {
        SyndicateConfigUtil.setSortComparator(this.config, val);
    }

    public Class<? extends OutputFormat> getOutputFormat() {
        return SyndicateConfigUtil.getOutputFormat(this.config);
    }

    public void setOutputFormat(Class<? extends OutputFormat> val) {
        SyndicateConfigUtil.setOutputFormat(this.config, val);
    }

    public Class<?> getOutputKey() {
        return SyndicateConfigUtil.getOutputKey(this.config);
    }

    public void setOutputKey(Class<?> val) {
        SyndicateConfigUtil.setOutputKey(this.config, val);
    }

    public Class<?> getOutputValue() {
        return SyndicateConfigUtil.getOutputValue(this.config);
    }

    public void setOutputValue(Class<?> val) {
        SyndicateConfigUtil.setOutputValue(this.config, val);
    }

    public Class<? extends InputFormat> getInputFormat() {
        return SyndicateConfigUtil.getInputFormat(this.config);
    }

    public void setInputFormat(Class<? extends InputFormat> val) {
        SyndicateConfigUtil.setInputFormat(this.config, val);
    }
    
    public void setMinInputSplitSize(long val) {
        SyndicateConfigUtil.setMinInputSplitSize(this.config, val);
    }
    
    public long getMinInputSplitSize() {
        return SyndicateConfigUtil.getMinInputSplitSize(this.config);
    }
    
    public void setMaxInputSplitSize(long val) {
        SyndicateConfigUtil.setMaxInputSplitSize(this.config, val);
    }
    
    public long getMaxInputSplitSize() {
        return SyndicateConfigUtil.getMaxInputSplitSize(this.config);
    }
    
    public void setInputPaths(String commaSeparatedPaths) throws IOException {
        SyndicateConfigUtil.setInputPaths(this.config, commaSeparatedPaths);
    }

    public void addInputPaths(String commaSeparatedPaths) throws IOException {
        SyndicateConfigUtil.addInputPaths(this.config, commaSeparatedPaths);
    }
    
    public void setInputPaths(Path... inputPaths) throws IOException {
        SyndicateConfigUtil.setInputPaths(this.config, inputPaths);
    }
    
    public Path[] getInputPaths() throws IOException {
        return SyndicateConfigUtil.getInputPaths(this.config);
    }
    
    public Class<? extends FilenameFilter> getInputPathFilter() {
        return SyndicateConfigUtil.getInputPathFilter(this.config);
    }
    
    public void setInputPathFilter(Class<? extends FilenameFilter> val) {
        SyndicateConfigUtil.setInputPathFilter(this.config, val);
    }
}
