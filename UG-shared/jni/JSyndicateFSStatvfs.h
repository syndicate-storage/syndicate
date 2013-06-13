/* 
 * File:   JSyndicateFSStatvfs.h
 * Author: iychoi
 *
 * Created on June 11, 2013, 11:20 PM
 */

#ifndef JSYNDICATEFSSTATVFS_H
#define	JSYNDICATEFSSTATVFS_H

#include <libsyndicate.h>

#include <jni.h>
#include <string.h>
#include <sys/statvfs.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define JSFSStatvfs_Class_Identifier "JSyndicateFSJNI/struct/JSFSStatvfs"
    
struct JSFSStatvfs_Class_Structure {
    jclass      clazz;
    jclass      ref_clazz;
    jfieldID    f_bsize_id;
    jfieldID    f_frsize_id;
    jfieldID    f_blocks_id;
    jfieldID    f_bfree_id;
    jfieldID    f_bavail_id;
    jfieldID    f_files_id;
    jfieldID    f_ffree_id;
    jfieldID    f_favail_id;
    jfieldID    f_fsid_id;
    jfieldID    f_flag_id;
    jfieldID    f_namemax_id;
};

int jsyndicatefs_init_statvfs(struct statvfs *statv);
int jsyndicatefs_uninit_statvfs(struct statvfs *statv);

int jsyndicatefs_init_JSFSStatvfs_Structure(JNIEnv *jenv);
int jsyndicatefs_uninit_JSFSStatvfs_Structure(JNIEnv *jenv);
int jsyndicatefs_copy_JSFSStatvfs_to_Native(JNIEnv *jenv, jobject jobj, struct statvfs *statv);
int jsyndicatefs_copy_Native_to_JSFSStatvfs(JNIEnv *jenv, jobject jobj, struct statvfs *statv);

#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFSSTATVFS_H */

