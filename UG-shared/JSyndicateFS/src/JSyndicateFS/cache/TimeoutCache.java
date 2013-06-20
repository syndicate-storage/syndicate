/*
 * Timeout based Cache class
 */
package JSyndicateFS.cache;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;


/**
 *
 * @author iychoi
 */
public class TimeoutCache<tk, tv> implements ICache<tk, tv> {
    
    private int maxCacheNumber;
    private int timeoutSeconds;
    
    private Map<tk, CacheObject<tk, tv>> cache = new Hashtable<tk, CacheObject<tk, tv>>();
    
    public TimeoutCache(int maxCacheNumber, int timeoutSeconds) {
        if(maxCacheNumber < 0)
            maxCacheNumber = 0;
        if(timeoutSeconds < 0)
            timeoutSeconds = 0;
        
        this.maxCacheNumber = maxCacheNumber;
        this.timeoutSeconds = timeoutSeconds;
    }
    
    private void removeOld() {
        Collection<CacheObject<tk, tv>> values = this.cache.values();
        long now = CacheObject.currentTime();
        
        for(CacheObject<tk, tv> value : values) {
            if(!value.isFresh(this.timeoutSeconds, now)) {
                // old cache
                this.cache.remove(value.getKey());
            }
        }
    }
    
    @Override
    public synchronized boolean containsKey(tk key) {
        CacheObject<tk, tv> co = this.cache.get(key);
        if(co == null)
            return false;
        
        if(!co.isFresh(this.timeoutSeconds)) {
            // old
            this.cache.remove(co.getKey());
            return false;
        }
        return true;
    }

    @Override
    public synchronized tv get(tk key) {
        CacheObject<tk, tv> co = this.cache.get(key);
        if(co == null)
            return null;
        
        if(!co.isFresh(this.timeoutSeconds)) {
            // old
            this.cache.remove(co.getKey());
            return null;
        }
        
        return co.getValue();
    }
    
    private void makeEmptySlot() {
        Collection<CacheObject<tk, tv>> values = this.cache.values();
        long now = CacheObject.currentTime();
        CacheObject<tk, tv> oldest = null;
        
        for(CacheObject<tk, tv> value : values) {
            if(!value.isFresh(this.timeoutSeconds, now)) {
                // old cache
                this.cache.remove(value.getKey());
                return;
            } else {
                if(oldest == null) {
                    oldest = value;
                }
                else {
                    if(oldest.getTimestamp() > value.getTimestamp())
                        oldest = value;
                }
            }
        }
        
        this.cache.remove(oldest.getKey());
    }

    @Override
    public synchronized void insert(tk key, tv value) {
        CacheObject<tk, tv> pair = new CacheObject<tk, tv>(key, value);
        
        if(this.maxCacheNumber > 0 &&
                this.cache.size() >= this.maxCacheNumber) {
            makeEmptySlot();
        }
        
        this.cache.put(key, pair);
    }

    @Override
    public synchronized void invalidate(tk key) {
        this.cache.remove(key);
    }

    @Override
    public synchronized void clear() {
        this.cache.clear();
    }

    @Override
    public synchronized int size() {
        removeOld();
        return this.cache.size();
    }
}
