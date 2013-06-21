/*
 * Stat class for JSyndicateFS
 * - This class is used between JNI layers
 */
package JSyndicateFSJNI.struct;

/**
 *
 * @author iychoi
 */
public class JSFSStat {
    
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
    
    /*
    * Type Information
    * __dev_t = __DEV_T_TYPE = __UQUAD_TYPE = unsigned long int
    * __ino_t = __INO_T_TYPE = __ULONGWORD_TYPE = unsigned long int
    * __mode_t = __MODE_T_TYPE = __U32_TYPE = unsigned int
    * __nlink_t = __NLINK_T_TYPE = __UWORD_TYPE = unsigned long int
    * __uid_t = __UID_T_TYPE = __U32_TYPE = unsigned int
    * __gid_t = __GID_T_TYPE = __U32_TYPE = unsigned int
    * __off_t = __OFF_T_TYPE = __SLONGWORD_TYPE = long int
    * __blksize_t = __BLKSIZE_T_TYPE = __SLONGWORD_TYPE = long int
    * __blkcnt_t = __BLKCNT_T_TYPE = __SLONGWORD_TYPE = long int
    * __time_t = __TIME_T_TYPE = __SLONGWORD_TYPE = long int
    */
    private /* __dev_t */ long st_dev;		/* Device.  */
    private /* __ino_t */ long st_ino;		/* File serial number.	*/
    private /* __mode_t */ int st_mode;		/* File mode.  */
    private /* __nlink_t */ long st_nlink;	/* Link count.  */
    private /* __uid_t */ int st_uid;		/* User ID of the file's owner.	*/
    private /* __gid_t */ int st_gid;		/* Group ID of the file's group.*/
    private /* __dev_t */ long st_rdev;		/* Device number, if device.  */
    private /* __off_t */ long st_size;		/* Size of file, in bytes.  */
    private /* __blksize_t */ long st_blksize;	/* Optimal block size for I/O.  */
    private /* __blkcnt_t */ long st_blocks;	/* Number 512-byte blocks allocated. */
    private /* __time_t */ long st_atim;	/* Time of last access.  */
    private /* __time_t */ long st_mtim;	/* Time of last modification.  */
    private /* __time_t */ long st_ctim;	/* Time of last status change.  */
    
    public JSFSStat() {
        this.st_dev = -1;
        this.st_ino = -1;
        this.st_mode = -1;
        this.st_nlink = -1;
        this.st_uid = -1;
        this.st_gid = -1;
        this.st_rdev = -1;
        this.st_size = -1;
        this.st_blksize = -1;
        this.st_blocks = -1;
        this.st_atim = -1;
        this.st_mtim = -1;
        this.st_ctim = -1;
    }
    
    public JSFSStat(long st_dev, long st_ino, int st_mode, long st_nlink, int st_uid, int st_gid, long st_rdev, long st_size, long st_blksize, long st_blocks, long st_atim, long st_mtim, long st_ctim) {
        this.st_dev = st_dev;
        this.st_ino = st_ino;
        this.st_mode = st_mode;
        this.st_nlink = st_nlink;
        this.st_uid = st_uid;
        this.st_gid = st_gid;
        this.st_rdev = st_rdev;
        this.st_size = st_size;
        this.st_blksize = st_blksize;
        this.st_blocks = st_blocks;
        this.st_atim = st_atim;
        this.st_mtim = st_mtim;
        this.st_ctim = st_ctim;
    }

    /**
     * @return the st_dev
     */
    public long getSt_dev() {
        return st_dev;
    }

    /**
     * @param st_dev the st_dev to set
     */
    public void setSt_dev(long st_dev) {
        this.st_dev = st_dev;
    }

    /**
     * @return the st_ino
     */
    public long getSt_ino() {
        return st_ino;
    }

    /**
     * @param st_ino the st_ino to set
     */
    public void setSt_ino(long st_ino) {
        this.st_ino = st_ino;
    }

    /**
     * @return the st_mode
     */
    public int getSt_mode() {
        return st_mode;
    }

    /**
     * @param st_mode the st_mode to set
     */
    public void setSt_mode(int st_mode) {
        this.st_mode = st_mode;
    }

    /**
     * @return the st_nlink
     */
    public long getSt_nlink() {
        return st_nlink;
    }

    /**
     * @param st_nlink the st_nlink to set
     */
    public void setSt_nlink(long st_nlink) {
        this.st_nlink = st_nlink;
    }

    /**
     * @return the st_uid
     */
    public int getSt_uid() {
        return st_uid;
    }

    /**
     * @param st_uid the st_uid to set
     */
    public void setSt_uid(int st_uid) {
        this.st_uid = st_uid;
    }

    /**
     * @return the st_gid
     */
    public int getSt_gid() {
        return st_gid;
    }

    /**
     * @param st_gid the st_gid to set
     */
    public void setSt_gid(int st_gid) {
        this.st_gid = st_gid;
    }

    /**
     * @return the st_rdev
     */
    public long getSt_rdev() {
        return st_rdev;
    }

    /**
     * @param st_rdev the st_rdev to set
     */
    public void setSt_rdev(long st_rdev) {
        this.st_rdev = st_rdev;
    }

    /**
     * @return the st_size
     */
    public long getSt_size() {
        return st_size;
    }

    /**
     * @param st_size the st_size to set
     */
    public void setSt_size(long st_size) {
        this.st_size = st_size;
    }

    /**
     * @return the st_blksize
     */
    public long getSt_blksize() {
        return st_blksize;
    }

    /**
     * @param st_blksize the st_blksize to set
     */
    public void setSt_blksize(long st_blksize) {
        this.st_blksize = st_blksize;
    }

    /**
     * @return the st_blocks
     */
    public long getSt_blocks() {
        return st_blocks;
    }

    /**
     * @param st_blocks the st_blocks to set
     */
    public void setSt_blocks(long st_blocks) {
        this.st_blocks = st_blocks;
    }

    /**
     * @return the st_atim
     */
    public long getSt_atim() {
        return st_atim;
    }

    /**
     * @param st_atim the st_atim to set
     */
    public void setSt_atim(long st_atim) {
        this.st_atim = st_atim;
    }

    /**
     * @return the st_mtim
     */
    public long getSt_mtim() {
        return st_mtim;
    }

    /**
     * @param st_mtim the st_mtim to set
     */
    public void setSt_mtim(long st_mtim) {
        this.st_mtim = st_mtim;
    }

    /**
     * @return the st_ctim
     */
    public long getSt_ctim() {
        return st_ctim;
    }

    /**
     * @param st_ctim the st_ctim to set
     */
    public void setSt_ctim(long st_ctim) {
        this.st_ctim = st_ctim;
    }
}
