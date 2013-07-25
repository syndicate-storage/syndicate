/*
 * FileSystem Util for Syndicate
 */
package SyndicateHadoop.util;

import JSyndicateFS.FileSystem;
import SyndicateHadoop.SyndicateConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class FileSystemUtil {

    public static final Log LOG = LogFactory.getLog(FileSystemUtil.class);
    
    public static final FileSystem getFileSystem(Configuration conf) throws InstantiationException {
        if(!FileSystem.isInitialized()) {
            SyndicateConfig syndicateConfig = new SyndicateConfig(conf);
            JSyndicateFS.Configuration jsfsConfig = syndicateConfig.getJSFSConfiguration();
            
            FileSystem.init(jsfsConfig);
        }

        return FileSystem.getInstance();
    }
}
