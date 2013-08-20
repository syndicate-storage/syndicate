/*
 * MessageBuilder class for JSyndicateFS with IPC backend
 */
package JSyndicateFS.backend.ipc.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPCMessageBuilder {

    private static final Log LOG = LogFactory.getLog(IPCMessageBuilder.class);

    public enum IPCMessageOperations {
        OP_GET_STAT(0), OP_DELETE(1), OP_REMOVE_DIRECTORY(2), OP_RENAME(3), OP_MKDIR(4), 
        OP_READ_DIRECTORY(5), OP_GET_FILE_HANDLE(6), OP_CREATE_NEW_FILE(7), OP_READ_FILEDATA(8), 
        OP_WRITE_FILEDATA(9), OP_FLUSH(10), OP_CLOSE_FILE_HANDLE(11), OP_TRUNCATE_FILE(12);
        
        private int code = -1;
        
        private IPCMessageOperations(int c) {
            this.code = c;
        }

        public int getCode() {
            return this.code;
        }

        public static IPCMessageOperations getFrom(int code) {
            IPCMessageOperations[] operations = IPCMessageOperations.values();
            for(IPCMessageOperations op : operations) {
                if(op.getCode() == code)
                    return op;
            }
            return null;
        }
    }
    
    private static byte[] getBytesOf(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        buffer.putInt(value);
        return buffer.array();
    }
    
    private static byte[] getBytesOf(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        buffer.putLong(value);
        return buffer.array();
    }
    
    private static void sendBytesMessage(DataOutputStream dos, IPCMessageOperations op, List<byte[]> messages) throws IOException {
        // use big endian
        dos.writeInt(op.getCode());
        
        int sum = 0;
        for(byte[] msg : messages) {
            sum += msg.length;
        }
        
        // total message size
        // use big endian
        dos.writeInt(sum + (messages.size() * 4));
        // number of messages
        dos.writeInt(messages.size());
        
        for(byte[] msg : messages) {
            dos.writeInt(msg.length);
            dos.write(msg);
        }
        
        dos.flush();
    }
    
    private static List<byte[]> recvBytesMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        int opcode = dis.readInt();
        if(opcode != op.getCode()) {
            throw new IOException("OPCode is not matching : request (" + op.getCode() + "), found (" + opcode + ")");
        }
        
        // must have 3 fields
        int returncode = dis.readInt();
        int totalMessageSize = dis.readInt();
        int totalNumberOfMessages = dis.readInt();
        
        LOG.debug("returncode : " + returncode);
        LOG.debug("totalMessageSize : " + totalMessageSize);
        LOG.debug("totalNumberOfMessages : " + totalNumberOfMessages);
        
        if(opcode == IPCMessageOperations.OP_READ_FILEDATA.getCode() ||
                opcode == IPCMessageOperations.OP_WRITE_FILEDATA.getCode()) {
            // returncode can be positive
            if(returncode < 0) {
                throw new IOException(ErrorUtils.generateErrorMessage(returncode));
            }
        } else {
            if(returncode != 0) {
                throw new IOException(ErrorUtils.generateErrorMessage(returncode));
            }    
        }
        
        int readSum = 0;
        
        List<byte[]> arr = new ArrayList<byte[]>();
        
        for(int i=0;i<totalNumberOfMessages;i++) {
            int size = dis.readInt();
            readSum += 4;
            LOG.debug("message " + i + " size : " + size);
            readSum += size;
            byte[] message = new byte[size];
            dis.readFully(message);
            arr.add(message);
        }
        
        if(readSum != totalMessageSize) {
            throw new IOException("read message size is not matching with totalMessageSize in header");
        }
        
        return arr;
    }
    
    public static void sendStringsMessage(DataOutputStream dos, IPCMessageOperations op, String... messages) throws IOException {
        List<byte[]> arr = new ArrayList<byte[]>();
        
        for(String str : messages) {
            arr.add(str.getBytes());
        }
        sendBytesMessage(dos, op, arr);
        
        arr.clear();
    }
    
    public static void sendFileReadMessage(DataOutputStream dos, IPCMessageOperations op, IPCFileInfo fi, long fileoffset, int size) throws IOException {
        List<byte[]> arr = new ArrayList<byte[]>();
        
        arr.add(fi.toBytes());
        arr.add(getBytesOf(fileoffset));
        arr.add(getBytesOf(size));
        
        sendBytesMessage(dos, op, arr);
        
        arr.clear();
    }
    
    public static void sendFileWriteMessage(DataOutputStream dos, IPCMessageOperations op, IPCFileInfo fi, long fileoffset, byte[] buffer, int bufferoffset, int size) throws IOException {
        byte[] newarr = new byte[size];
        System.arraycopy(buffer, bufferoffset, newarr, 0, size);
        
        List<byte[]> arr = new ArrayList<byte[]>();
        
        arr.add(fi.toBytes());
        arr.add(getBytesOf(fileoffset));
        arr.add(newarr);
        
        sendBytesMessage(dos, op, arr);
        
        arr.clear();
    }
    
    public static void sendFileInfoMessage(DataOutputStream dos, IPCMessageOperations op, IPCFileInfo fi) throws IOException {
        List<byte[]> arr = new ArrayList<byte[]>();
        
        arr.add(fi.toBytes());
        
        sendBytesMessage(dos, op, arr);
        
        arr.clear();
    }
    
    public static void sendFileTruncateMessage(DataOutputStream dos, IPCMessageOperations op, IPCFileInfo fi, long fileoffset) throws IOException {
        List<byte[]> arr = new ArrayList<byte[]>();
        
        arr.add(fi.toBytes());
        arr.add(getBytesOf(fileoffset));
        
        sendBytesMessage(dos, op, arr);
        
        arr.clear();
    }
    
    public static IPCStat readStatMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        List<byte[]> arr = recvBytesMessage(dis, op);
        
        if(arr.size() != 1) {
            throw new IOException("The number of message is not 1");
        }
        
        byte[] msg = arr.get(0);
        
        IPCStat stat = new IPCStat();
        stat.fromBytes(msg, 0, msg.length);
        return stat;
    }
    
    public static void readResultMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        List<byte[]> arr = recvBytesMessage(dis, op);
        
        if(arr.size() != 0) {
            throw new IOException("The number of message is not 0");
        }
    }
    
    public static String[] readDirectoryMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        List<byte[]> arr = recvBytesMessage(dis, op);
        
        String[] entries = new String[arr.size()];
        for(int i=0;i<arr.size();i++) {
            entries[i] = new String(arr.get(i));
        }
        return entries;
    }
    
    public static IPCFileInfo readFileInfoMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        List<byte[]> arr = recvBytesMessage(dis, op);
        
        if(arr.size() != 1) {
            throw new IOException("The number of message is not 1");
        }
        
        byte[] msg = arr.get(0);
        
        IPCFileInfo fileinfo = new IPCFileInfo();
        fileinfo.fromBytes(msg, 0, msg.length);
        return fileinfo;
    }
    
    public static int readFileData(DataInputStream dis, IPCMessageOperations op, byte[] buffer, int offset) throws IOException {
        List<byte[]> arr = recvBytesMessage(dis, op);
        
        if(arr.size() != 1) {
            throw new IOException("The number of message is not 1");
        }
        
        byte[] msg = arr.get(0);
        
        System.arraycopy(msg, 0, buffer, offset, msg.length);
        
        return msg.length;
    }
}
