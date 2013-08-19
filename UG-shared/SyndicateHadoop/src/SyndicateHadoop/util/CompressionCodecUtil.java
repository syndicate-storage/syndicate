/*
 * Compression Codec Util for Syndicate
 */
package SyndicateHadoop.util;

import JSyndicateFS.JSFSPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 *
 * @author iychoi
 */
public class CompressionCodecUtil {
    public static CompressionCodec getCompressionCodec(Configuration conf, JSFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
        return codec;
    }
    
    public static CompressionCodec getCompressionCodec(CompressionCodecFactory factory, JSFSPath path) {
        // caution : file variable contains fake path 
        Path file = new Path(path.getPath());
        CompressionCodec codec = factory.getCodec(file);
        return codec;
    }
}
