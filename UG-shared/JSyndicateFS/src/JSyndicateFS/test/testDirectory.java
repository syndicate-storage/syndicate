/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS.test;

import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSPath;
import JSyndicateFS.backend.sharedfs.SharedFSConfiguration;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author iychoi
 */
public class testDirectory {
    private static JSFSFileSystem filesystem;
    
    public static void initFS() throws IllegalAccessException, InstantiationException {
        SharedFSConfiguration conf = new SharedFSConfiguration();
        File mountpoint = new File("/mnt/syndicatefs");
        conf.setMountPoint(mountpoint);
        
        System.out.println("JSyndicateFS is Opening");
        filesystem = JSFSFileSystem.createInstance(conf);
    }
    
    public static void uninitFS() throws IOException {
        System.out.println("JSyndicateFS is Closing");
        filesystem.close();
    }
    
    public static void createNewDirs() throws IOException {
        JSFSPath complex = new JSFSPath("a/b/c");
        
        System.out.println("create complex dir");
        filesystem.mkdirs(complex);
    }
    
    public static void createNewFile() throws IOException {
        JSFSPath complexpath = new JSFSPath("a/b/c/complex.txt");
        
        System.out.println("start file check");
        
        String msg = "hello world!";
        OutputStream out = filesystem.getFileOutputStream(complexpath);
        out.write(msg.getBytes());
        out.close();
        System.out.println("msg written");

        InputStream in = filesystem.getFileInputStream(complexpath);

        byte[] buffer = new byte[256];
        int read = in.read(buffer);
        if(read > 0) {
            String readmsg = new String(buffer, 0, read);
            System.out.println("msg read : " + readmsg);
        }
        in.close();

        System.out.println("filename : " + complexpath.getPath() + ", size : " + read);
    }
    
    public static void listAllFiles() throws FileNotFoundException, IOException {
        JSFSPath path = new JSFSPath("/");
        
        JSFSPath[] entries = filesystem.listAllFiles(path);
        if(entries != null) {
            System.out.println("number of entries : " + entries.length);
            for(JSFSPath entry : entries) {
                System.out.println("file : " + entry.getPath());
            }
        }
    }
    
    public static void deleteAll() throws IOException {
        JSFSPath path = new JSFSPath("/a");
        
        filesystem.deleteAll(path);
    }
    
    public static void main(String[] args) {
        try {
            initFS();
            
            createNewDirs();
            createNewFile();

            listAllFiles();
            
            deleteAll();
            
            Thread.sleep(3000);
            uninitFS();
        } catch (InstantiationException ex) {
            System.out.println(ex.toString());
        } catch (IOException ex) {
            System.out.println(ex.toString());
        } catch (IllegalAccessException ex) {
            System.out.println(ex.toString());
        } catch (InterruptedException ex) {
            System.out.println(ex.toString());
        }
    }
}
