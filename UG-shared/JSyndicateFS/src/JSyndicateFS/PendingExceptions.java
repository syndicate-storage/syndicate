/*
 * Pending Multiple Exception class
 */
package JSyndicateFS;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public class PendingExceptions extends Exception {

    private ArrayList<Exception> exceptions = new ArrayList<Exception>();

    public PendingExceptions() {
        // do nothing
    }
    
    public void add(Exception e) {
        this.exceptions.add(e);
    }
    
    public void addAll(PendingExceptions ex) {
        this.exceptions.addAll(ex.exceptions);
    }

    public Collection<Exception> getExceptions() {
        return this.exceptions;
    }
    
    public boolean isEmpty() {
        return this.exceptions.isEmpty();
    }
    
    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        
        for(Exception ex : this.exceptions) {
            sb.append(ex.getMessage());
        }
        
        return sb.toString();
    }
}
