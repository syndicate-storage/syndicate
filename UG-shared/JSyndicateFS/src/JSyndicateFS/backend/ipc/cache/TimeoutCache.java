/*
 * Timeout based Cache class
 */
package JSyndicateFS.backend.ipc.cache;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 *
 * @author iychoi
 */
public class TimeoutCache<tk, tv> implements ICache<tk, tv> {
    
    private static final Log LOG = LogFactory.getLog(TimeoutCache.class);
    
    private int maxCacheSize;
    private int timeoutSeconds;
    
    private Map<tk, CacheObject<tk, tv>> cache = new Hashtable<tk, CacheObject<tk, tv>>();
    
    public TimeoutCache(int maxCacheSize, int timeoutSeconds) {
        LOG.info("TimeoutCache");
        
        if(maxCacheSize < 0)
            maxCacheSize = 0;
        if(timeoutSeconds < 0)
            timeoutSeconds = 0;
        
        LOG.debug("maxCacheSize : " + maxCacheSize);
        LOG.debug("timeoutSeconds : " + timeoutSeconds);
        
        this.maxCacheSize = maxCacheSize;
        this.timeoutSeconds = timeoutSeconds;
    }
    
    private void removeOld() {
        LOG.info("removeOld");
        
        Collection<CacheObject<tk, tv>> values = this.cache.values();
        long now = CacheObject.currentTime();
        
        for(CacheObject<tk, tv> value : values) {
            if(!value.isFresh(this.timeoutSeconds, now)) {
                // old cache
                LOG.debug("remove - key : " + value.getKey().toString());
                this.cache.remove(value.getKey());
            }
        }
    }
    
    @Override
    public synchronized boolean containsKey(tk key) {
        LOG.info("containsKey");
        LOG.debug("key : " + key.toString());
        
        CacheObject<tk, tv> co = this.cache.get(key);
        if(co == null)
            return false;
        
        if(!co.isFresh(this.timeoutSeconds)) {
            // old
            LOG.debug("found dirty - key : " + co.getKey().toString());
            this.cache.remove(co.getKey());
            return false;
        }
        return true;
    }

    @Override
    public synchronized tv get(tk key) {
        LOG.info("get");
        LOG.debug("key : " + key.toString());
        
        CacheObject<tk, tv> co = this.cache.get(key);
        if(co == null)
            return null;
        
        if(!co.isFresh(this.timeoutSeconds)) {
            // old
            LOG.debug("found dirty - key : " + co.getKey().toString());
            this.cache.remove(co.getKey());
            return null;
        }
        
        return co.getValue();
    }
    
    private void makeEmptySlot() {
        LOG.info("makeEmptySlot");
        
        Collection<CacheObject<tk, tv>> values = this.cache.values();
        long now = CacheObject.currentTime();
        CacheObject<tk, tv> oldest = null;
        
        for(CacheObject<tk, tv> value : values) {
            if(!value.isFresh(this.timeoutSeconds, now)) {
                // old cache
                LOG.debug("found dirty - key : " + value.getKey().toString());
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
        
        LOG.debug("found oldest - key : " + oldest.getKey().toString());
        this.cache.remove(oldest.getKey());
    }

    @Override
    public synchronized void insert(tk key, tv value) {
        LOG.info("insert");
        LOG.debug("key : " + key.toString());
        
        CacheObject<tk, tv> pair = new CacheObject<tk, tv>(key, value);
        
        CacheObject co = this.cache.get(key);
        if(co == null) {
            if(this.maxCacheSize > 0 &&
                    this.cache.size() >= this.maxCacheSize) {
                makeEmptySlot();
            }
        } else {
            LOG.debug("remove existing key : " + key.toString());
            this.cache.remove(key);
        }
        
        LOG.debug("put key : " + key.toString());
        this.cache.put(key, pair);
    }

    @Override
    public synchronized void invalidate(tk key) {
        LOG.info("invalidate");
        LOG.debug("key : " + key.toString());
        
        this.cache.remove(key);
    }

    @Override
    public synchronized void clear() {
        LOG.info("clear");
        
        this.cache.clear();
    }

    @Override
    public synchronized int size() {
        removeOld();
        return this.cache.size();
    }
}
