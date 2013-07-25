/*
 * FileSystem Event Handler class for JSyndicateFS
 */
package JSyndicateFS;

/**
 *
 * @author iychoi
 */
public interface FileSystemEventHandler {
    void onBeforeCreate(FileSystem fs);
    void onAfterCreate(FileSystem fs);
    
    void onBeforeDestroy(FileSystem fs);
    void onAfterDestroy(FileSystem fs);
}
