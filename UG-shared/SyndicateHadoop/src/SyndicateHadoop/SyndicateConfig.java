/*
 * JSFSConfiguration class for Syndicate
 */
package SyndicateHadoop;

import JSyndicateFS.JSFSFilenameFilter;
import JSyndicateFS.JSFSPath;
import SyndicateHadoop.util.SyndicateConfigUtil;
import SyndicateHadoop.util.SyndicateConfigUtil.Backend;
import java.io.DataInput;
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
    
    public Backend getBackend() {
        return SyndicateConfigUtil.getBackend(this.config);
    }
    
    public void setBackend(Backend backend) {
        SyndicateConfigUtil.setBackend(this.config, backend);
    }
    
    public int getIPC_Port() {
        return SyndicateConfigUtil.getIPC_Port(this.config);
    }
    
    public void setIPC_Port(int port) {
        SyndicateConfigUtil.setIPC_Port(this.config, port);
    }
    
    public String getIPC_UGName() {
        return SyndicateConfigUtil.getIPC_UGName(this.config);
    }
    
    public void setIPC_UGName(String ug_name) {
        SyndicateConfigUtil.setIPC_UGName(this.config, ug_name);
    }
    
    public String getSFS_MountPath() {
        return SyndicateConfigUtil.getSFS_MountPath(this.config);
    }
    
    public void setSFS_MountPath(String path) {
        SyndicateConfigUtil.setSFS_MountPath(this.config, path);
    }
    
    public int getMaxMetadataCacheNum() {
        return SyndicateConfigUtil.getMaxMetadataCacheNum(this.config);
    }
    
    public void setMaxMetadataCacheNum(int maxCache) {
        SyndicateConfigUtil.setMaxMetadataCacheNum(this.config, maxCache);
    }
    
    public int getMetadataCacheTimeout() {
        return SyndicateConfigUtil.getMetadataCacheTimeout(this.config);
    }
    
    public void setMetadataCacheTimeout(int timeoutSec) {
        SyndicateConfigUtil.setMetadataCacheTimeout(this.config, timeoutSec);
    }
    
    public int getFileReadBufferSize() {
        return SyndicateConfigUtil.getFileReadBufferSize(this.config);
    }
    
    public void setFileReadBufferSize(int bufferSize) {
        SyndicateConfigUtil.setFileReadBufferSize(this.config, bufferSize);
    }
    
    public int getFileWriteBufferSize() {
        return SyndicateConfigUtil.getFileWriteBufferSize(this.config);
    }
    
    public void setFileWriteBufferSize(int bufferSize) {
        SyndicateConfigUtil.setFileWriteBufferSize(this.config, bufferSize);
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
    
    public void setMinInputSplitSize(long minSize) {
        SyndicateConfigUtil.setMinInputSplitSize(this.config, minSize);
    }
    
    public long getMinInputSplitSize() {
        return SyndicateConfigUtil.getMinInputSplitSize(this.config);
    }
    
    public void setMaxInputSplitSize(long maxSize) {
        SyndicateConfigUtil.setMaxInputSplitSize(this.config, maxSize);
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
    
    public void setInputPaths(JSFSPath... inputPaths) throws IOException {
        SyndicateConfigUtil.setInputPaths(this.config, inputPaths);
    }
    
    public JSFSPath[] getInputPaths() throws IOException {
        return SyndicateConfigUtil.getInputPaths(this.config);
    }
    
    public Class<? extends JSFSFilenameFilter> getInputPathFilter() {
        return SyndicateConfigUtil.getInputPathFilter(this.config);
    }
    
    public void setInputPathFilter(Class<? extends JSFSFilenameFilter> val) {
        SyndicateConfigUtil.setInputPathFilter(this.config, val);
    }
    
    public void setOutputPath(JSFSPath outputPath) {
        SyndicateConfigUtil.setOutputPath(this.config, outputPath);
    }
    
    public void setOutputPath(String outputPath) {
        SyndicateConfigUtil.setOutputPath(this.config, outputPath);
    }

    public JSFSPath getOutputPath() {
        return SyndicateConfigUtil.getOutputPath(this.config);
    }

    public void setOutputBaseName(String basename) {
        SyndicateConfigUtil.setOutputBaseName(this.config, basename);
    }
    
    public String getOutputBaseName() {
        return SyndicateConfigUtil.getOutputBaseName(this.config);
    }
    
    public void setTextOutputFormatSeparator(String separator) {
        SyndicateConfigUtil.setTextOutputFormatSeparator(this.config, separator);
    }
    
    public String getTextOutputFormatSeparator() {
        return SyndicateConfigUtil.getTextOutputFormatSeparator(this.config);
    }
    
    public void setTextInputMaxLength(int maxlength) {
        SyndicateConfigUtil.setTextInputMaxLength(this.config, maxlength);
    }
    
    public int getTextInputMaxLength() {
        return SyndicateConfigUtil.getTextInputMaxLength(this.config);
    }
    
    public void setTextRecordDelimiter(String delimiter) {
        SyndicateConfigUtil.setTextRecordDelimiter(this.config, delimiter);
    }
    
    public String getTextRecordDelimiter() {
        return SyndicateConfigUtil.getTextRecordDelimiter(this.config);
    }
    
    public void setTextLinesPerMap(int lines) {
        SyndicateConfigUtil.setTextLinesPerMap(this.config, lines);
    }
    
    public int getTextLinesPerMap() {
        return SyndicateConfigUtil.getTextLinesPerMap(this.config);
    }
}
