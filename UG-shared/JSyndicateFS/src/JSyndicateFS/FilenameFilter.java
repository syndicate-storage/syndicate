/*
 * FilenameFilter class for JSyndicateFS
 */
package JSyndicateFS;

/**
 *
 * @author iychoi
 */
public interface FilenameFilter {
    
    boolean accept(File dir, String name);
}
