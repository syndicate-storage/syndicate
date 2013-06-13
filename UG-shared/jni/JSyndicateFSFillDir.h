/* 
 * File:   JSyndicateFSFillDir.h
 * Author: iychoi
 *
 * Created on June 7, 2013, 12:22 PM
 */

#ifndef JSYNDICATEFSFILLDIR_H
#define	JSYNDICATEFSFILLDIR_H

#include "JSyndicateFSStat.h"

#include <jni.h>
#include <string.h>
#include <stat.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define JSFSFillDir_Class_Identifier "JSyndicateFSJNI/struct/JSFSFillDir"
    
/** Function to add an entry in a readdir() operation
 *
 * @param name the file name of the directory entry
 * @param stat file attributes, can be NULL
 * @param off offset of the next entry or zero
 * @return 1 if buffer is full, zero otherwise
 */
typedef int (*JSyndicateFS_Fill_Dir_t) (void *pjenv, void *pjobj, const char *name,
				const struct stat *stbuf, off_t off);
    
struct JSFSFillDir_Class_Structure {
    jclass      clazz;
    jclass      ref_clazz;
    jmethodID   fill_id;
};

int jsyndicatefs_init_JSFSFillDir_Structure(JNIEnv *jenv);
int jsyndicatefs_uninit_JSFSFillDir_Structure(JNIEnv *jenv);
int jsyndicatefs_call_JSFSFillDir(JNIEnv *jenv, jobject jobj, const char *name, const struct stat *stbuf, off_t off);


#ifdef	__cplusplus
}
#endif
    
#endif	/* JSYNDICATEFSFILLDIR_H */

