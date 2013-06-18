/* 
 * File:   JSyndicateFSFileInfo.h
 * Author: iychoi
 *
 * Created on June 6, 2013, 12:43 PM
 */

#ifndef JSYNDICATEFSFILEINFO_H
#define	JSYNDICATEFSFILEINFO_H

#include "fs_entry.h"

#include <jni.h>
#include <string.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define JSFSFileInfo_Class_Identifier "JSyndicateFSJNI/struct/JSFSFileInfo"
    
struct JSyndicateFS_FileInfo {
    int         flags;
    int         direct_io;
    void*       fh; // struct fs_file_handle* | struct fs_dir_handle*
};

struct JSFSFileInfo_Class_Structure {
    jclass      clazz;
    jclass      ref_clazz;
    jfieldID    flags_id;
    jfieldID    direct_io_id;
    jfieldID    fh_id;
};

int jsyndicatefs_init_fileinfo(struct JSyndicateFS_FileInfo* fi);
int jsyndicatefs_uninit_fileinfo(struct JSyndicateFS_FileInfo *fi);

int jsyndicatefs_init_JSFSFileInfo_Structure(JNIEnv *jenv);
int jsyndicatefs_uninit_JSFSFileInfo_Structure(JNIEnv *jenv);
int jsyndicatefs_copy_JSFSFileInfo_to_Native(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_copy_Native_to_JSFSFileInfo(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_FileInfo *fi);


#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFSFILEINFO_H */

