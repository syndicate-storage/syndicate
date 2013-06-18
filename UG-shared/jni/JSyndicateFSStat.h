/* 
 * File:   JSyndicateFSStat.h
 * Author: iychoi
 *
 * Created on June 11, 2013, 4:17 AM
 */

#ifndef JSYNDICATEFSSTAT_H
#define	JSYNDICATEFSSTAT_H

#include <libsyndicate.h>

#include <jni.h>
#include <string.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define JSFSStat_Class_Identifier "JSyndicateFSJNI/struct/JSFSStat"
    
struct JSFSStat_Class_Structure {
    jclass      clazz;
    jclass      ref_clazz;
    jmethodID   constructor_id;
    jfieldID    st_dev_id;
    jfieldID    st_ino_id;
    jfieldID    st_mode_id;
    jfieldID    st_nlink_id;
    jfieldID    st_uid_id;
    jfieldID    st_gid_id;
    jfieldID    st_rdev_id;
    jfieldID    st_size_id;
    jfieldID    st_blksize_id;
    jfieldID    st_blocks_id;
    jfieldID    st_atim_id;
    jfieldID    st_mtim_id;
    jfieldID    st_ctim_id;
};

int jsyndicatefs_init_stat(struct stat *statbuf);
int jsyndicatefs_uninit_stat(struct stat *statbuf);

int jsyndicatefs_init_JSFSStat_Structure(JNIEnv *jenv);
int jsyndicatefs_uninit_JSFSStat_Structure(JNIEnv *jenv);
int jsyndicatefs_copy_JSFSStat_to_Native(JNIEnv *jenv, jobject jobj, struct stat *statbuf);
int jsyndicatefs_copy_Native_to_JSFSStat(JNIEnv *jenv, jobject jobj, struct stat *statbuf);
jobject jsyndicatefs_create_JSFSStat(JNIEnv *jenv);

#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFSSTAT_H */

