/*
 * FileSystem Event Handler class for JSyndicateFS
 */
package JSyndicateFS;

/**
 *
 * @author iychoi
 */
public interface JSFSFileSystemEventHandler {
    void onBeforeCreate(JSFSConfiguration conf);
    void onAfterCreate(JSFSFileSystem fs);
    
    void onBeforeDestroy(JSFSFileSystem fs);
    void onAfterDestroy(JSFSConfiguration conf);
}
