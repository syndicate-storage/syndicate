/*
 * MessageBuilder class for JSyndicateFS with IPC backend
 */
package JSyndicateFS.backend.ipc.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author iychoi
 */
public class IPCMessageBuilder {

    public enum IPCMessageOperations {
        OP_GET_STAT(0), OP_DELETE(1), OP_REMOVE_DIRECTORY(2), OP_RENAME(3), OP_MKDIR(4), 
        OP_READ_DIRECTORY(5), OP_GET_FILE_HANDLE(6), OP_CREATE_NEW_FILE(7), OP_READ_FILEDATA(8), 
        OP_WRITE_FILE_DATA(9), OP_FLUSH(10), OP_CLOSE_FILE_HANDLE(11);
        
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
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, byte[] message) throws IOException {
        dos.writeInt(op.getCode()); // op
        dos.writeInt(message.length + 8); // message body (4*2 int)
        dos.writeInt(1); // num of message
        dos.writeInt(message.length); // message 1 length
        dos.write(message);
        dos.flush();
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, byte[] message, int message2, long message3) throws IOException {
        dos.writeInt(op.getCode()); // op
        dos.writeInt(message.length + 8 + 20); // message body (4*2 int)
        dos.writeInt(3); // num of message
        dos.writeInt(message.length); // message 1 length
        dos.write(message);
        dos.writeInt(4); // int
        dos.writeInt(message2);
        dos.writeInt(8); // long
        dos.writeLong(message3);
        dos.flush();
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, String message) throws IOException {
        sendMessage(dos, op, message.getBytes());
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, ArrayList<byte[]> messages) throws IOException {
        dos.writeInt(op.getCode());
        
        int sum = 0;
        for(byte[] msg : messages) {
            sum += msg.length;
        }
        
        dos.writeInt(sum + 4 + (messages.size() * 4));
        dos.writeInt(messages.size());
        
        for(byte[] msg : messages) {
            dos.writeInt(msg.length);
            dos.write(msg);
        }
        
        dos.flush();
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, byte[] message, byte[] message2, int size, int offset, long message3) throws IOException {
        dos.writeInt(op.getCode()); // op
        dos.writeInt(message.length + 8 + 4 + size + 4 + 8); // message body (4*2 int)
        dos.writeInt(3); // num of message
        dos.writeInt(message.length); // message 1 length
        dos.write(message);
        dos.writeInt(size); // size
        dos.write(message2, offset, size);
        dos.writeInt(8); // long
        dos.writeLong(message3);
        dos.flush();
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, String... messages) throws IOException {
        ArrayList<byte[]> arr = new ArrayList<byte[]>();
        
        for(String str : messages) {
            arr.add(str.getBytes());
        }
        sendMessage(dos, op, arr);
        
        arr.clear();
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, IPCFileInfo fi) throws IOException {
        sendMessage(dos, op, fi.toBytes());
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, IPCFileInfo fi, int size, long fileoffset) throws IOException {
        sendMessage(dos, op, fi.toBytes(), size, fileoffset);
    }
    
    public static void sendMessage(DataOutputStream dos, IPCMessageOperations op, IPCFileInfo fi, byte[] buffer, int size, int bufferoffset, long fileoffset) throws IOException {
        sendMessage(dos, op, fi.toBytes(), buffer, size, bufferoffset, fileoffset);
    }
    
    private static void checkOpCode(DataInputStream dis, IPCMessageOperations op) throws IOException {
        int opcode = dis.readInt();
        
        if(opcode != op.getCode()) {
            throw new IOException("OPCode is not matching : request (" + op.getCode() + "), found (" + opcode + ")");
        }
    }
    
    public static IPCStat readStatMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        checkOpCode(dis, op);
        int returncode = dis.readInt();
        int messageCount = dis.readInt(); // 1
        int messageLen = dis.readInt();
        
        if(returncode != 0) {
            throw new IOException(ErrorUtils.generateErrorMessage(returncode));
        }
        
        byte[] message = new byte[messageLen];
        int read = dis.read(message, 0, messageLen);
        if(read != messageLen) {
            throw new IOException("read message size is different with message len");
        }
        
        IPCStat stat = new IPCStat();
        stat.fromBytes(message, 0, messageLen);
        return stat;
    }
    
    public static void readResultMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        checkOpCode(dis, op);
        int returncode = dis.readInt();
        int messageCount = dis.readInt(); // 0
        
        if(returncode != 0) {
            throw new IOException(ErrorUtils.generateErrorMessage(returncode));
        }
    }
    
    public static String[] readDirectoryMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        checkOpCode(dis, op);
        int returncode = dis.readInt();
        int messageCount = dis.readInt();
        
        if(returncode != 0) {
            throw new IOException(ErrorUtils.generateErrorMessage(returncode));
        }
        
        ArrayList<String> arr_entries = new ArrayList<String>();
        
        for(int i=0;i<messageCount;i++) {
            int messageLen = dis.readInt();
            byte[] message = new byte[messageLen];
            
            int read = dis.read(message, 0, messageLen);
            if (read != messageLen) {
                throw new IOException("read message size is different with message len");
            }
            
            arr_entries.add(new String(message));
        }
        
        String[] entries = new String[arr_entries.size()];
        entries = arr_entries.toArray(entries);
        return entries;
    }
    
    public static IPCFileInfo readFileInfoMessage(DataInputStream dis, IPCMessageOperations op) throws IOException {
        checkOpCode(dis, op);
        int returncode = dis.readInt();
        int messageCount = dis.readInt(); // 1
        int messageLen = dis.readInt();
        
        if(returncode != 0) {
            throw new IOException(ErrorUtils.generateErrorMessage(returncode));
        }
        
        byte[] message = new byte[messageLen];
        int read = dis.read(message, 0, messageLen);
        if(read != messageLen) {
            throw new IOException("read message size is different with message len");
        }
        
        IPCFileInfo fileinfo = new IPCFileInfo();
        fileinfo.fromBytes(message, 0, messageLen);
        return fileinfo;
    }
    
    public static int readFileData(DataInputStream dis, IPCMessageOperations op, byte[] buffer, int offset) throws IOException {
        checkOpCode(dis, op);
        int returncode = dis.readInt();
        int messageCount = dis.readInt(); // 1
        int messageLen = dis.readInt();
        
        if(returncode != 0) {
            throw new IOException(ErrorUtils.generateErrorMessage(returncode));
        }
        
        dis.readFully(buffer, offset, messageLen);
        
        return messageLen;
    }
}
