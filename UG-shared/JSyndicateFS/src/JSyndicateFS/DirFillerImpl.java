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
    
    private static final String[] SKIP_FILES = {".", ".."};
    
    private ArrayList<String> entries = new ArrayList<String>();
    
    
    DirFillerImpl() {
        
    }
            
    @Override
    public void fill(String name, JSFSStat stbuf, long off) {
        boolean skip = false;
        for(String skip_file : SKIP_FILES) {
            if(name.equals(skip_file)) {
                skip = true;
            }
        }
        
        if(!skip) {
            entries.add(name);
        }
    }

    public String[] getEntryNames() {
        String[] arr = new String[this.entries.size()];
        arr = entries.toArray(arr);
        return arr;
    }
}
