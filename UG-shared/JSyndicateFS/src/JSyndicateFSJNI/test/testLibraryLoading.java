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
    
    public static void load(boolean local) {
        System.out.println("testLibraryLoading");
        System.out.println("loading : " + System.getProperty(JSyndicateFS.LIBRARY_FILE_PATH_KEY));
        
        if(System.getProperty(JSyndicateFS.LIBRARY_FILE_PATH_KEY, null) == null) {
           System.setProperty(JSyndicateFS.LIBRARY_FILE_PATH_KEY, "./libjsyndicatefs.so");
        }
        
        JSFSConfig cfg = new JSFSConfig();
        if(local)
            cfg.setMs_url("http://localhost:8080");
        else
            cfg.setMs_url("https://syndicate-metadata.appspot.com");
        
        cfg.setUGName("Hadoop");
        cfg.setUGPassword("sniff");
        cfg.setVolume_name("SyndicateHadoop");
        cfg.setVolume_secret("sniff");
        cfg.setPortnum(32780);
        
        int result = 0;
        
        System.out.println("MS : " + cfg.getMs_url());
        System.out.println("UG name : " + cfg.getUGName());
        System.out.println("UG password : " + cfg.getUGPassword());
        System.out.println("Volume name : " + cfg.getVolume_name());
        System.out.println("Volume secret : " + cfg.getVolume_secret());
        System.out.println("Port : " + cfg.getPortnum());
        
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
        
        boolean local = false;
        
        if(args.length > 0) {
            local = Boolean.parseBoolean(args[0]);
        }
        
        load(local);
        
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            Logger.getLogger(testLibraryLoading.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        unload();
    }
}
