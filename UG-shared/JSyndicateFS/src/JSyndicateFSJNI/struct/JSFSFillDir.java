/*
 * FillDir (callback) class for JSyndicateFS
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public abstract class JSFSFillDir {
    public abstract void fill(String name, JSFSStat stbuf, long off);
}
