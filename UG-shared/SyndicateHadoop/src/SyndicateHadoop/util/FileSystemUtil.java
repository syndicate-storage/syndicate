/*
 * FileSystem Util for Syndicate
 */
package SyndicateHadoop.util;

import JSyndicateFS.FileSystem;
import SyndicateHadoop.SyndicateConfig;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class FileSystemUtil {
    public static final FileSystem getFileSystem(Configuration conf) throws InstantiationException {
        SyndicateConfig syndicateConfig = new SyndicateConfig(conf);
        JSyndicateFS.Configuration jsfsConfig = syndicateConfig.getJSFSConfiguration();
        return FileSystem.getInstance(jsfsConfig);
    }
}
