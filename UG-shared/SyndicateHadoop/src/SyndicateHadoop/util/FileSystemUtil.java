/*
 * FileSystem Util for Syndicate
 */
package SyndicateHadoop.util;

import JSyndicateFS.JSFSConfiguration;
import JSyndicateFS.JSFSFileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class FileSystemUtil {

    public static final Log LOG = LogFactory.getLog(FileSystemUtil.class);
    
    private static JSFSFileSystem instance;
    
    public synchronized static final JSFSFileSystem getFileSystem(Configuration conf) throws InstantiationException {
        if(instance == null) {
            JSFSConfiguration configuration = SyndicateConfigUtil.getJSFSConfigurationInstance(conf);
            instance = JSFSFileSystem.createInstance(configuration);
        }
        
        return instance;
    }
}
