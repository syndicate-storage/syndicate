/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFSJNI.test;

import JSyndicateFSJNI.JSyndicateFS;
import JSyndicateFSJNI.struct.JSFSConfig;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author iychoi
 */
public class testLibraryLoading {
    
    public static void load() {
        System.out.println("testLibraryLoading");
        System.out.println("loading : " + System.getProperty(JSyndicateFS.LIBRARY_FILE_PATH_KEY));
        
        if(System.getProperty(JSyndicateFS.LIBRARY_FILE_PATH_KEY, null) == null) {
           System.setProperty(JSyndicateFS.LIBRARY_FILE_PATH_KEY, "./libjsyndicatefs.so");
        }
        
        JSFSConfig cfg = new JSFSConfig();
        cfg.setUGName("Hadoop");
        
        int result = 0;
        
        System.out.println("UG name : " + cfg.getUGName());
        
        result = JSyndicateFS.jsyndicatefs_init(cfg);
        if(result != 0) {
            System.out.println("JSyndicateFS.jsyndicatefs_init : " + result);
        }
    }
    
    public static void unload() {
        
        int result = 0;
        
        result = JSyndicateFS.jsyndicatefs_destroy();
        if(result != 0) {
            System.out.println("JSyndicateFS.jsyndicatefs_destroy : " + result);
        }
    }
    
    public static void main(String[] args) {
        
        load();
        
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            Logger.getLogger(testLibraryLoading.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        unload();
    }
}
