/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS.test;

import JSyndicateFS.Configuration;
import JSyndicateFS.FSInputStream;
import JSyndicateFS.FSOutputStream;
import JSyndicateFS.File;
import JSyndicateFS.FileSystem;
import JSyndicateFS.Path;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 *
 * @author iychoi
 */
public class testFileIO {
    
    private static FileSystem filesystem;
    
    public static void initFS() throws IllegalAccessException, URISyntaxException, InstantiationException {
        Configuration conf = new Configuration();
        conf.setUGName("Hadoop");

        System.out.println("JSyndicateFS is Opening");
        FileSystem.init(conf);
        filesystem = FileSystem.getInstance();
    }
    
    public static void uninitFS() throws IOException {
        System.out.println("JSyndicateFS is Closing");
        filesystem.close();
    }
    
    public static void createNewFile() throws IOException {
        Path path = new Path("testFileIO.txt");
        
        System.out.println("start file check");
        if(filesystem.exists(path)) {
            System.out.println("file already exists");
            
            filesystem.delete(path);
            System.out.println("file deleted");
        }
        
        boolean result = filesystem.createNewFile(path);
        if(result) {
            File file = new File(filesystem, path);
            if(file.isFile() && file.exist()) {
                System.out.println("file created");
                
                String msg = "hello world!";
                FSOutputStream out = new FSOutputStream(file);
                out.write(msg.getBytes());
                out.close();
                System.out.println("msg written");
                
                FSInputStream in = new FSInputStream(file);
                
                byte[] buffer = new byte[256];
                int read = in.read(buffer);
                if(read > 0) {
                    String readmsg = new String(buffer, 0, read);
                    System.out.println("msg read : " + readmsg);
                }
                in.close();
                
                System.out.println("filename : " + file.getName() + ", size : " + file.getSize() + ", blocks : " + file.getBlocks() + ", blockSize : " + file.getBlockSize());
                
                if(file.delete()) {
                    System.out.println("file deleted");
                }
            }
        } else {
            System.out.println("file creation failed");
        }
    }
    
    public static void listRootFiles() throws FileNotFoundException, IOException {
        Path path = new Path("/");
        
        String[] entries = filesystem.readDirectoryEntries(path);
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
            
            //listRootFiles();
            
            createNewFile();
            
            Thread.sleep(3000);
            uninitFS();
        } catch (InstantiationException ex) {
            System.out.println(ex.toString());
        } catch (IOException ex) {
            System.out.println(ex.toString());
        } catch (URISyntaxException ex) {
            System.out.println(ex.toString());
        } catch (IllegalAccessException ex) {
            System.out.println(ex.toString());
        } catch (InterruptedException ex) {
            System.out.println(ex.toString());
        }
    }
}
