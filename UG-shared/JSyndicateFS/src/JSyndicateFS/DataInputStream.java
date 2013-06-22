/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFS;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author iychoi
 */
public class DataInputStream extends InputStream {

    private FileHandle filehandle;
    
    public DataInputStream(FileHandle filehandle) {
        if(filehandle == null)
            throw new IllegalArgumentException("Can not create input stream from null filehandle");
        
        if(!filehandle.isOpen())
            throw new IllegalArgumentException("Can not create input stream from closed filehandle");
        
        this.filehandle = filehandle;
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
