/*
 * Stat class for JSyndicateFS with IPC daemon backend
 */
package JSyndicateFS.backend.ipc.message;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 * @author iychoi
 */
public class IPCStat {
    
    /*
     * Bit mask of mode
     */
    public static final int S_IFMT = 0170000; //bit mask for the file type bit fields
    public static final int S_IFSOCK = 0140000; //socket
    public static final int S_IFLNK = 0120000; //symbolic link
    public static final int S_IFREG = 0100000; //regular file
    public static final int S_IFBLK = 0060000; //block device
    public static final int S_IFDIR = 0040000; //directory
    public static final int S_IFCHR = 0020000; //character device
    public static final int S_IFIFO = 0010000; //FIFO
    public static final int S_ISUID = 0004000; //set-user-ID bit
    public static final int S_ISGID = 0002000; //set-group-ID bit (see below)
    public static final int S_ISVTX = 0001000; //sticky bit (see below)
    public static final int S_IRWXU = 00700; //mask for file owner permissions
    public static final int S_IRUSR = 00400; //owner has read permission
    public static final int S_IWUSR = 00200; //owner has write permission
    public static final int S_IXUSR = 00100; //owner has execute permission
    public static final int S_IRWXG = 00070; //mask for group permissions
    public static final int S_IRGRP = 00040; //group has read permission
    public static final int S_IWGRP = 00020; //group has write permission
    public static final int S_IXGRP = 00010; //group has execute permission
    public static final int S_IRWXO = 00007; //mask for permissions for others (not in group)
    public static final int S_IROTH = 00004; //others have read permission
    public static final int S_IWOTH = 00002; //others have write permission
    public static final int S_IXOTH = 00001; //others have execute permission
    
    private /* __mode_t */ int st_mode;		/* File mode.  */
    private /* __uid_t */ int st_uid;		/* User ID of the file's owner.	*/
    private /* __gid_t */ int st_gid;		/* Group ID of the file's group.*/
    private /* __off_t */ long st_size;		/* Size of file, in bytes.  */
    private /* __blksize_t */ long st_blksize;	/* Optimal block size for I/O.  */
    private /* __blkcnt_t */ long st_blocks;	/* Number 512-byte blocks allocated. */
    private /* __time_t */ long st_atim;	/* Time of last access.  */
    private /* __time_t */ long st_mtim;	/* Time of last modification.  */
    
    public IPCStat() {
        this.st_mode = -1;
        this.st_uid = -1;
        this.st_gid = -1;
        this.st_size = -1;
        this.st_blksize = -1;
        this.st_blocks = -1;
        this.st_atim = -1;
        this.st_mtim = -1;
    }
    
    public IPCStat(int st_mode, int st_uid, int st_gid, long st_size, long st_blksize, long st_blocks, long st_atim, long st_mtim) {
        this.st_mode = st_mode;
        this.st_uid = st_uid;
        this.st_gid = st_gid;
        this.st_size = st_size;
        this.st_blksize = st_blksize;
        this.st_blocks = st_blocks;
        this.st_atim = st_atim;
        this.st_mtim = st_mtim;
    }

    public int getMode() {
        return st_mode;
    }

    public void setMode(int mode) {
        this.st_mode = mode;
    }

    public int getUid() {
        return st_uid;
    }

    public void setUid(int uid) {
        this.st_uid = uid;
    }

    public int getGid() {
        return st_gid;
    }

    public void setGid(int gid) {
        this.st_gid = gid;
    }

    public long getSize() {
        return st_size;
    }

    public void setSize(long size) {
        this.st_size = size;
    }

    public long getBlksize() {
        return st_blksize;
    }

    public void setBlksize(long blksize) {
        this.st_blksize = blksize;
    }

    public long getBlocks() {
        return st_blocks;
    }

    public void setBlocks(long blocks) {
        this.st_blocks = blocks;
    }

    public long getAtim() {
        return st_atim;
    }

    public void setAtim(long atim) {
        this.st_atim = atim;
    }

    public long getMtim() {
        return st_mtim;
    }

    public void setMtim(long mtim) {
        this.st_mtim = mtim;
    }
    
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(getFieldSize());
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        buffer.putInt(this.st_mode);
        buffer.putInt(this.st_uid);
        buffer.putInt(this.st_gid);
        
        buffer.putLong(this.st_size);
        buffer.putLong(this.st_blksize);
        buffer.putLong(this.st_blocks);
        buffer.putLong(this.st_atim);
        buffer.putLong(this.st_mtim);

        return buffer.array();
    }
    
    public void fromBytes(byte[] bytes, int offset, int len) {
        ByteBuffer buffer = ByteBuffer.allocate(getFieldSize());
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        buffer.put(bytes, offset, len);
        
        buffer.flip();
        
        this.st_mode = buffer.getInt();
        this.st_uid = buffer.getInt();
        this.st_gid = buffer.getInt();
        
        this.st_size = buffer.getLong();
        this.st_blksize = buffer.getLong();
        this.st_blocks = buffer.getLong();
        this.st_atim = buffer.getLong();
        this.st_mtim = buffer.getLong();
    }
    
    public int getFieldSize() {
        return (5*8) + (3*4);
    }
}
