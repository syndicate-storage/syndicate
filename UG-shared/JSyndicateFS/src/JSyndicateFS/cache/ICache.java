/*
 * Cache interface
 */
package JSyndicateFS.cache;

/**
 *
 * @author iychoi
 */
public interface ICache<tk, tv> {
    /*
     * Return True if has cached object
     */
    public boolean containsKey(tk key);
    
    /*
     * Return Value cached
     */
    public tv get(tk key);
    
    /*
     * Set value into the cache
     */
    public void insert(tk key, tv value);
    
    /*
     * Invalidate cache
     */
    public void invalidate(tk key);
    
    /*
     * Remove all cache
     */
    public void clear();
    
    /*
     * Return the size of cached objects
     */
    public int size();
}
