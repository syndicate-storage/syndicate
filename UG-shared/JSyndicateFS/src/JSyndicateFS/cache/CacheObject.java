/*
 * CacheObject class
 */
package JSyndicateFS.cache;

import java.util.Calendar;

/**
 *
 * @author iychoi
 */
class CacheObject<tk, tv> {
    private tk key;
    private tv value;
    private long timestamp;
    
    public CacheObject() {
        this.timestamp = getCurrentTime();
    }
    
    public CacheObject(tk key, tv value) {
        if(key == null)
            throw new IllegalArgumentException("Can not create key from null object");
        
        this.key = key;
        this.value = value;
        this.timestamp = getCurrentTime();
    }
    
    public static long currentTime() {
        return Calendar.getInstance().getTimeInMillis();
    }
    
    private long getCurrentTime() {
        return Calendar.getInstance().getTimeInMillis();
    }
    
    public void setKey(tk key) {
        if(key == null)
            throw new IllegalArgumentException("Can not create key from null object");
        
        this.key = key;
    }
    
    public tk getKey() {
        return this.key;
    }
    
    public void setValue(tv value) {
        this.value = value;
    }
    
    public tv getValue() {
        return this.value;
    }
    
    public boolean compareKey(CacheObject<tk, tv> o) {
        return this.key.equals(o.key);
    }
    
    public void renewTimestamp() {
        this.timestamp = getCurrentTime();
    }
    
    public long getTimestamp() {
        return this.timestamp;
    }
    
    public boolean isFresh(int timeoutSecond) {
        if(timeoutSecond <= 0) {
            return true;
        }
        
        long now = getCurrentTime();
        return isFresh(timeoutSecond, now);
    }
    
    public boolean isFresh(int timeoutSecond, long now) {
        if(timeoutSecond <= 0 || now <= 0) {
            return true;
        }
        
        if(this.timestamp < (now - (timeoutSecond * 1000)))
            return false;
        return true;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CacheObject))
            return false;
        
        CacheObject<tk, tv> other = (CacheObject<tk, tv>) o;
        if(!this.key.equals((other.key)))
            return false;
        
        return this.value.equals(other.value);
    }
    
    @Override
    public int hashCode() {
        return this.key.hashCode() ^ this.value.hashCode();
    }
}
