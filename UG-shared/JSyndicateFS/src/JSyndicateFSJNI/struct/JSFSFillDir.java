/*
 * FillDir (callback) class for JSyndicateFS
 * - This class is used between JNI layers
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public abstract class JSFSFillDir {
    public abstract void fill(String name, JSFSStat stbuf, long off);
}
