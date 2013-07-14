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
import static JSyndicateFS.test.testFileIO.createNewFile;
import static JSyndicateFS.test.testFileIO.initFS;
import static JSyndicateFS.test.testFileIO.uninitFS;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author iychoi
 */
public class testDirectory {
    private static FileSystem filesystem;
    
    public static void initFS() throws IllegalAccessException, URISyntaxException, InstantiationException {
        Configuration conf = new Configuration();
        conf.setMSUrl(new URI("http://localhost:8080"));
        conf.setUGName("Hadoop");
        conf.setUGPassword("sniff");
        conf.setVolumeName("testvolume-iychoi-email.arizona.edu");
        conf.setVolumeSecret("sniff");
        conf.setPort(32780);

        System.out.println("JSyndicateFS is Opening");
        filesystem = FileSystem.getInstance(conf);
    }
    
    public static void uninitFS() throws IOException {
        System.out.println("JSyndicateFS is Closing");
        filesystem.close();
    }
    
    public static void createNewDirs() throws IOException {
        Path complex = new Path("a/b/c");
        
        System.out.println("create complex dir");
        if(filesystem.exists(complex) && filesystem.isDirectory(complex)) {
            System.out.println("dir already exists");
        } else {
            filesystem.mkdirs(complex);
            
            if(filesystem.exists(complex) && filesystem.isDirectory(complex)) {
                System.out.println("dir created");
            }
        }
    }
    
    public static void createNewFile() throws IOException {
        Path complexpath = new Path("a/b/c/complex.txt");
        
        System.out.println("start file check");
        if(filesystem.exists(complexpath)) {
            System.out.println("file already exists");
            
            filesystem.delete(complexpath);
            System.out.println("file deleted");
        }
        
        if(filesystem.createNewFile(complexpath)) {
            File file = new File(filesystem, complexpath);
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
                
                //file.renameTo(new Path("a/b/c/d/complexNew.txt"));
                
                //System.out.println("file renamed : " + file.getPath().getPath());
            }
        } else {
            System.out.println("file creation failed");
        }
    }
    
    public static void listAllFiles() throws FileNotFoundException, IOException {
        Path path = new Path("/");
        
        Path[] entries = filesystem.listAllFiles(path);
        if(entries != null) {
            System.out.println("number of entries : " + entries.length);
            for(Path entry : entries) {
                System.out.println("file : " + entry.getPath());
            }
        }
    }
    
    public static void deleteAll() throws IOException {
        Path path = new Path("/a");
        
        filesystem.deleteAll(path);
    }
    
    public static void main(String[] args) {
        try {
            initFS();
            
            createNewDirs();
            createNewFile();

            //listAllFiles();
            
            deleteAll();
            
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
