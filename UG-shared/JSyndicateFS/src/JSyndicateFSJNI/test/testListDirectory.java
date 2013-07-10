/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFSJNI.test;

import JSyndicateFSJNI.JSyndicateFS;
import JSyndicateFSJNI.struct.JSFSFillDir;
import JSyndicateFSJNI.struct.JSFSStat;

/**
 *
 * @author iychoi
 */
public class testListDirectory {
    
    public static void listDir(String path) {
        
        System.out.println("list dir : " + path);
        
        JSyndicateFS.jsyndicatefs_readdir(path, new JSFSFillDir() {

            @Override
            public void fill(String name, JSFSStat stbuf, long off) {
                System.out.println(name);
            }
        
        }, 0, null);
    }
    
    public static void main(String[] args) {
        testLibraryLoading.load(false);
        
        if(args.length > 0)
            listDir(args[0]);
        
        testLibraryLoading.unload();
    }
}
