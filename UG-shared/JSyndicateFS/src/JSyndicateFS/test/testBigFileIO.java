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
import static JSyndicateFS.test.testFileIO.listRootFiles;
import static JSyndicateFS.test.testFileIO.uninitFS;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author iychoi
 */
public class testBigFileIO {
    private static FileSystem filesystem;
    private static final long KILOBYTE = 1024;
    private static final long MEGABYTE = 1024*1024;
    
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
    
    public static void createNewFile() throws IOException {
        Path path = new Path("testBigFileIO.txt");
        
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
                
                long size = MEGABYTE * 64;
                
                FSOutputStream outSource = new FSOutputStream(file);
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
                
                if(file.getSize() != size) {
                    System.out.println("msg written failed");
                } else {
                    System.out.println("msg written");
                    
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {}

                    FSInputStream inSource = new FSInputStream(file);
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
                }
                
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
