/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS.test;

import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSPath;
import JSyndicateFS.backend.sharedfs.SharedFSConfiguration;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author iychoi
 */
public class testBigFileIO {
    private static JSFSFileSystem filesystem;
    private static final long KILOBYTE = 1024;
    private static final long MEGABYTE = 1024*1024;
    
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
    
    public static void createNewFile() throws IOException {
        JSFSPath path = new JSFSPath("testBigFileIO.txt");
        
        System.out.println("start file check");
        if(filesystem.exists(path)) {
            System.out.println("file already exists");
            
            filesystem.delete(path);
            System.out.println("file deleted");
        }
        
        long size = MEGABYTE * 64;
        
        OutputStream outSource = filesystem.getFileOutputStream(path);
        BufferedOutputStream out = new BufferedOutputStream(outSource, (int)MEGABYTE);
        byte[] wbuff = new byte[1];

        byte data = 0;
        for(long i=0;i<size;i++) {
            // fill buffer
            wbuff[0] = data;
            out.write(wbuff);

            data++;
        }

        out.close();
                
        if(filesystem.getSize(path) != size) {
            System.out.println("msg written failed");
        } else {
            System.out.println("msg written");
        }

        InputStream inSource = filesystem.getFileInputStream(path);
        BufferedInputStream in = new BufferedInputStream(inSource, (int)MEGABYTE);
                    
        data = 0;
        byte[] rbuff = new byte[1];
        boolean success = true;

        for(long i=0;i<size;i++) {
            int read = in.read(rbuff);
            if(read > 0) {
                if(data != rbuff[0]) {
                    System.out.println("data not matching");
                    success = false;
                    break;
                }
            }

            data++;
        }
        in.close();

        if(success) {
            System.out.println("msg read success");
        }
        
        System.out.println("filename : " + path.getPath() + ", size : " + size);

        filesystem.delete(path);
    }
    
    public static void main(String[] args) {
        try {
            initFS();
            
            createNewFile();
            
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
