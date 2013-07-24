/*
 * FileSystem Event Handler class for JSyndicateFS
 */
package JSyndicateFS;

/**
 *
 * @author iychoi
 */
public interface FileSystemEventHandler {
    void onBeforeCreate(Configuration conf);
    void onAfterCreate(Configuration conf);
    
    void onBeforeDestroy(Configuration conf);
    void onAfterDestroy(Configuration conf);
}
