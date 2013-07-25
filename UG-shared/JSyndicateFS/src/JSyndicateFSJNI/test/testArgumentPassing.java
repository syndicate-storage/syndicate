/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JSyndicateFSJNI.test;

import JSyndicateFSJNI.JSyndicateFS;
import JSyndicateFSJNI.struct.JSFSFileInfo;
import JSyndicateFSJNI.struct.JSFSFillDir;
import JSyndicateFSJNI.struct.JSFSStat;
import JSyndicateFSJNI.struct.JSFSStatvfs;
import JSyndicateFSJNI.struct.JSFSUtimbuf;

/**
 *
 * @author iychoi
 */
public class testArgumentPassing {
    public static void test_getattr() {
        
        System.out.println("test_getattr start!");
        
        JSFSStat statbuf = new JSFSStat();
        statbuf.setSt_dev(301);
        statbuf.setSt_ino(302);
        statbuf.setSt_mode(303);
        statbuf.setSt_nlink(304);
        statbuf.setSt_uid(305);
        statbuf.setSt_gid(306);
        statbuf.setSt_rdev(307);
        statbuf.setSt_size(308);
        statbuf.setSt_blksize(309);
        statbuf.setSt_blocks(310);
        statbuf.setSt_atim(311);
        statbuf.setSt_mtim(312);
        statbuf.setSt_ctim(313);
        
        JSyndicateFS.jsyndicatefs_getattr("/path/sample.txt", statbuf);
        
        System.out.println("st_dev : " + statbuf.getSt_dev());
        System.out.println("st_ino : " + statbuf.getSt_ino());
        System.out.println("st_mode : " + statbuf.getSt_mode());
        System.out.println("st_nlink : " + statbuf.getSt_nlink());
        System.out.println("st_uid : " + statbuf.getSt_uid());
        System.out.println("st_gid : " + statbuf.getSt_gid());
        System.out.println("st_rdev : " + statbuf.getSt_rdev());
        System.out.println("st_size : " + statbuf.getSt_size());
        System.out.println("st_blksize : " + statbuf.getSt_blksize());
        System.out.println("st_blocks : " + statbuf.getSt_blocks());
        System.out.println("st_atim : " + statbuf.getSt_atim());
        System.out.println("st_mtim : " + statbuf.getSt_mtim());
        System.out.println("st_ctim : " + statbuf.getSt_ctim());
        
        System.out.println("test_getattr end!");
    }
    
    public static void test_mknod() {
        
        System.out.println("test_mknod start!");
        
        JSyndicateFS.jsyndicatefs_mknod("/path/sample.txt", 300, 301);
        
        System.out.println("test_mknod end!");
    }
    
    public static void test_mkdir() {
        
        System.out.println("test_mkdir start!");
        
        JSyndicateFS.jsyndicatefs_mkdir("/path/sample.txt", 300);
        
        System.out.println("test_mkdir end!");
    }
    
    public static void test_unlink() {
        
        System.out.println("test_unlink start!");
        
        JSyndicateFS.jsyndicatefs_unlink("/path/sample.txt");
        
        System.out.println("test_unlink end!");
    }
    
    public static void test_rmdir() {
        
        System.out.println("test_rmdir start!");
        
        JSyndicateFS.jsyndicatefs_rmdir("/path/sample.txt");
        
        System.out.println("test_rmdir end!");
    }
    
    public static void test_rename() {
        
        System.out.println("test_rename start!");
        
        JSyndicateFS.jsyndicatefs_rename("/path/sample1.txt", "/path/sample2.txt");
        
        System.out.println("test_rename end!");
    }
    
    public static void test_chmod() {
        
        System.out.println("test_chmod start!");
        
        JSyndicateFS.jsyndicatefs_chmod("/path/sample.txt", 300);
        
        System.out.println("test_chmod end!");
    }
    
    public static void test_truncate() {
        
        System.out.println("test_truncate start!");
        
        JSyndicateFS.jsyndicatefs_truncate("/path/sample.txt", 300);
        
        System.out.println("test_truncate end!");
    }
    
    public static void test_utime() {
        
        System.out.println("test_utime start!");
        
        JSFSUtimbuf utim = new JSFSUtimbuf();
        utim.setActime(301);
        utim.setModtime(302);
        
        JSyndicateFS.jsyndicatefs_utime("/path/sample.txt", utim);
        
        System.out.println("actime : " + utim.getActime());
        System.out.println("modtime : " + utim.getModtime());
        
        System.out.println("test_utime end!");
    }
    
    public static void test_open() {
        
        System.out.println("test_open start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_open("/path/sample.txt", fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_open end!");
    }
    
    public static void test_read() {
        
        System.out.println("test_read start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        byte[] buf = new byte[10];
        for(int i=0;i<10;i++) {
            buf[i] = 0;
        }
        
        JSyndicateFS.jsyndicatefs_read("/path/sample.txt", buf, 10, 0, fi);
        
        System.out.print("buf : ");
        for(int i=0;i<10;i++) {
            System.out.print(buf[i]);
        }
        System.out.println("");
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_read end!");
    }
    
    public static void test_write() {
        
        System.out.println("test_write start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        byte[] buf = new byte[10];
        for(int i=0;i<10;i++) {
            buf[i] = 0;
        }
        
        JSyndicateFS.jsyndicatefs_write("/path/sample.txt", buf, 10, 0, fi);
        
        System.out.print("buf : ");
        for(int i=0;i<10;i++) {
            System.out.print(buf[i]);
        }
        System.out.println("");
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_write end!");
    }
    
    public static void test_statfs() {
        
        System.out.println("test_statfs start!");
        
        JSFSStatvfs statv = new JSFSStatvfs();
        statv.setF_bsize(301);
        statv.setF_frsize(302);
        statv.setF_blocks(303);
        statv.setF_bfree(304);
        statv.setF_bavail(305);
        statv.setF_files(306);
        statv.setF_ffree(307);
        statv.setF_favail(308);
        statv.setF_fsid(309);
        statv.setF_flag(310);
        statv.setF_namemax(311);
        
        JSyndicateFS.jsyndicatefs_statfs("/path/sample.txt", statv);
        
        System.out.println("f_bsize : " + statv.getF_bsize());
        System.out.println("f_frsize : " + statv.getF_frsize());
        System.out.println("f_blocks : " + statv.getF_blocks());
        System.out.println("f_bfree : " + statv.getF_bfree());
        System.out.println("f_bavail : " + statv.getF_bavail());
        System.out.println("f_files : " + statv.getF_files());
        System.out.println("f_ffree : " + statv.getF_ffree());
        System.out.println("f_favail : " + statv.getF_favail());
        System.out.println("f_fsid : " + statv.getF_fsid());
        System.out.println("f_flag : " + statv.getF_flag());
        System.out.println("f_namemax : " + statv.getF_namemax());
        
        System.out.println("test_statfs end!");
    }
    
    public static void test_flush() {
        
        System.out.println("test_flush start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_flush("/path/sample.txt", fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_flush end!");
    }
    
    public static void test_release() {
        
        System.out.println("test_release start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_release("/path/sample.txt", fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_release end!");
    }
    
    public static void test_fsync() {
        
        System.out.println("test_fsync start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_fsync("/path/sample.txt", 300, fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_fsync end!");
    }
    
    public static void test_setxattr() {
        
        System.out.println("test_setxattr start!");
        
        byte[] buf = new byte[10];
        for(int i=0;i<10;i++) {
            buf[i] = 10;
        }
        
        JSyndicateFS.jsyndicatefs_setxattr("/path/sample.txt", "some_name", buf, 10, 300);
        
        System.out.print("buf : ");
        for(int i=0;i<10;i++) {
            System.out.print(buf[i]);
        }
        System.out.println("");
        
        System.out.println("test_setxattr end!");
    }
    
    public static void test_getxattr() {
        
        System.out.println("test_getxattr start!");
        
        byte[] buf = new byte[10];
        for(int i=0;i<10;i++) {
            buf[i] = 10;
        }
        
        JSyndicateFS.jsyndicatefs_getxattr("/path/sample.txt", "some_name", buf, 10);
        
        System.out.print("buf : ");
        for(int i=0;i<10;i++) {
            System.out.print(buf[i]);
        }
        System.out.println("");
        
        System.out.println("test_getxattr end!");
    }
    
    public static void test_listxattr() {
        
        System.out.println("test_listxattr start!");
        
        byte[] buf = new byte[10];
        for(int i=0;i<10;i++) {
            buf[i] = 10;
        }
        
        JSyndicateFS.jsyndicatefs_listxattr("/path/sample.txt", buf, 10);
        
        System.out.print("buf : ");
        for(int i=0;i<10;i++) {
            System.out.print(buf[i]);
        }
        System.out.println("");
        
        System.out.println("test_listxattr end!");
    }
    
    public static void test_removexattr() {
        
        System.out.println("test_removexattr start!");
        
        JSyndicateFS.jsyndicatefs_removexattr("/path/sample.txt", "some_attr_name");
        
        System.out.println("test_removexattr end!");
    }
    
    public static void test_opendir() {
        
        System.out.println("test_opendir start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_opendir("/path/sample.txt", fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_opendir end!");
    }
    
    public static void test_readdir() {
        
        System.out.println("test_readdir start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_readdir("/path/sample.txt", new JSFSFillDir(){

            @Override
            public void fill(String name, JSFSStat stbuf, long off) {
                System.out.println("cb - name : " + name);
                System.out.println("cb - stbuf : " + stbuf);
                System.out.println("cb - off : " + off);
            }
        }, 0, fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_readdir end!");
    }
    
    public static void test_releasedir() {
        
        System.out.println("test_releasedir start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_releasedir("/path/sample.txt", fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_releasedir end!");
    }
    
    public static void test_fsyncdir() {
        
        System.out.println("test_fsyncdir start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_fsync("/path/sample.txt", 300, fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_fsyncdir end!");
    }
    
    public static void test_access() {
        
        System.out.println("test_access start!");
        
        JSyndicateFS.jsyndicatefs_access("/path/sample.txt", 300);
        
        System.out.println("test_access end!");
    }
    
    public static void test_create() {
        
        System.out.println("test_create start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_create("/path/sample.txt", 300, fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_create end!");
    }
    
    public static void test_ftruncate() {
        
        System.out.println("test_ftruncate start!");
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_ftruncate("/path/sample.txt", 300, fi);
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_ftruncate end!");
    }
    
    public static void test_fgetattr() {
        
        System.out.println("test_fgetattr start!");
        
        JSFSStat statbuf = new JSFSStat();
        statbuf.setSt_dev(301);
        statbuf.setSt_ino(302);
        statbuf.setSt_mode(303);
        statbuf.setSt_nlink(304);
        statbuf.setSt_uid(305);
        statbuf.setSt_gid(306);
        statbuf.setSt_rdev(307);
        statbuf.setSt_size(308);
        statbuf.setSt_blksize(309);
        statbuf.setSt_blocks(310);
        statbuf.setSt_atim(311);
        statbuf.setSt_mtim(312);
        statbuf.setSt_ctim(313);
        
        JSFSFileInfo fi = new JSFSFileInfo();
        fi.setDirect_io(301);
        fi.setFlags(302);
        fi.setFh(303);
        
        JSyndicateFS.jsyndicatefs_fgetattr("/path/sample.txt", statbuf, fi);
        
        System.out.println("st_dev : " + statbuf.getSt_dev());
        System.out.println("st_ino : " + statbuf.getSt_ino());
        System.out.println("st_mode : " + statbuf.getSt_mode());
        System.out.println("st_nlink : " + statbuf.getSt_nlink());
        System.out.println("st_uid : " + statbuf.getSt_uid());
        System.out.println("st_gid : " + statbuf.getSt_gid());
        System.out.println("st_rdev : " + statbuf.getSt_rdev());
        System.out.println("st_size : " + statbuf.getSt_size());
        System.out.println("st_blksize : " + statbuf.getSt_blksize());
        System.out.println("st_blocks : " + statbuf.getSt_blocks());
        System.out.println("st_atim : " + statbuf.getSt_atim());
        System.out.println("st_mtim : " + statbuf.getSt_mtim());
        System.out.println("st_ctim : " + statbuf.getSt_ctim());
        
        System.out.println("direct_io : " + fi.getDirect_io());
        System.out.println("flags : " + fi.getFlags());
        System.out.println("fh : " + fi.getFh());
        
        System.out.println("test_fgetattr end!");
    }
    
    public static void main(String[] args) {
        testLibraryLoading.load();
        
        System.out.println("test start!");
        
        test_getattr();
        test_mknod();
        test_mkdir();
        test_unlink();
        test_rmdir();
        test_rename();
        test_chmod();
        test_truncate();
        test_utime();
        test_open();
        test_read();
        test_write();
        test_statfs();
        test_flush();
        test_release();
        test_fsync();
        test_setxattr();
        test_getxattr();
        test_listxattr();
        test_removexattr();
        test_opendir();
        test_readdir();
        test_releasedir();
        test_fsyncdir();
        test_access();
        test_create();
        test_ftruncate();
        test_fgetattr();
        
        System.out.println("test end!");
        
        testLibraryLoading.unload();
    }
}
