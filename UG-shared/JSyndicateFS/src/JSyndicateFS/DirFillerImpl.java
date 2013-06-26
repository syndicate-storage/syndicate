/*
 * Directory Entry Filler Implementation Class of JSFSFillDir for JSyndicateFS
 */
package JSyndicateFS;

import JSyndicateFSJNI.struct.JSFSFillDir;
import JSyndicateFSJNI.struct.JSFSStat;
import java.util.ArrayList;

/**
 *
 * @author iychoi
 */
public class DirFillerImpl extends JSFSFillDir {
    
    private ArrayList<String> entries = new ArrayList<String>();
    
    DirFillerImpl() {
        
    }
            
    @Override
    public void fill(String name, JSFSStat stbuf, long off) {
        entries.add(name);
    }

    public String[] getEntryNames() {
        String[] arr = new String[this.entries.size()];
        arr = entries.toArray(arr);
        return arr;
    }
}
