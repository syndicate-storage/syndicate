/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS.test;

import JSyndicateFS.Configuration;
import JSyndicateFS.FileSystem;
import JSyndicateFS.Path;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author iychoi
 */
public class testFileIO {
    
    private static FileSystem filesystem;
    
    public static void initFS() throws IllegalAccessException, URISyntaxException, InstantiationException {
        Configuration conf = new Configuration();
        conf.setMSUrl(new URI("http://localhost:8080"));
        conf.setUGName("Hadoop");
        conf.setUGPassword("sniff");
        conf.setVolumeName("SyndicateHadoop");
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
        Path path = new Path("test1.txt");
        
        boolean result = filesystem.createNewFile(path);
        if(result)
            System.out.println("file created");
        else
            System.out.println("file creation failed");
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
            
            listRootFiles();
            
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
