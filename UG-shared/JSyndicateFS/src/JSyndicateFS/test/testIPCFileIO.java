/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS.test;

import JSyndicateFS.JSFSFileSystem;
import JSyndicateFS.JSFSPath;
import JSyndicateFS.backend.ipc.IPCConfiguration;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author iychoi
 */
public class testIPCFileIO {
    private static JSFSFileSystem filesystem;
    
    public static void initFS() throws IllegalAccessException, InstantiationException {
        IPCConfiguration conf = new IPCConfiguration();
        conf.setPort(18888);
        
        System.out.println("JSyndicateFS is Opening");
        filesystem = JSFSFileSystem.createInstance(conf);
    }
    
    public static void uninitFS() throws IOException {
        System.out.println("JSyndicateFS is Closing");
        filesystem.close();
    }
    
    public static void createNewFile() throws IOException {
        JSFSPath path = new JSFSPath("testFileIO.txt");
        
        System.out.println("start file check");
        String msg = "hello world!";
        
        OutputStream out = filesystem.getFileOutputStream(path);
        out.write(msg.getBytes());
        out.close();
        System.out.println("msg written");
                
        InputStream in = filesystem.getFileInputStream(path);
                
        byte[] buffer = new byte[256];
        int read = in.read(buffer);
        if(read > 0) {
            String readmsg = new String(buffer, 0, read);
            System.out.println("msg read : " + readmsg);
        }
        in.close();
                
        System.out.println("filename : " + path.getPath() + ", size : " + read);

        filesystem.delete(path);
    }
    
    public static void listRootFiles() throws FileNotFoundException, IOException {
        JSFSPath path = new JSFSPath("/");
        
        String[] entries = filesystem.readDirectoryEntryNames(path);
        if(entries != null) {
            System.out.println("number of entries : " + entries.length);
            for(String entry : entries) {
                System.out.println("file : " + entry);
            }
        }
    }
    
    public static void main(String[] args) {
        try {
            initFS();
            
            listRootFiles();
            
            //createNewFile();
            
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
