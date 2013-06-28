/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SyndicateHadoop;

import SyndicateHadoop.util.SyndicateConfigUtil;
import java.io.DataInput;
import java.io.IOException;
import java.net.URL;
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
}
