/*
 * JSFSFilenameFilter class for JSyndicateFS
 */
package JSyndicateFS;

/**
 *
 * @author iychoi
 */
public interface JSFSFilenameFilter {
    boolean accept(JSFSPath dir, String name);
}
