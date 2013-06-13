/* 
 * File:   JSyndicateFSUtimbuf.h
 * Author: iychoi
 *
 * Created on June 11, 2013, 9:44 PM
 */

#ifndef JSYNDICATEFSUTIMBUF_H
#define	JSYNDICATEFSUTIMBUF_H

#include <libsyndicate.h>

#include <jni.h>
#include <string.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define JSFSUtimbuf_Class_Identifier "JSyndicateFSJNI/struct/JSFSUtimbuf"
    
struct JSFSUtimbuf_Class_Structure {
    jclass      clazz;
    jclass      ref_clazz;
    jfieldID    actime_id;
    jfieldID    modtime_id;
};

int jsyndicatefs_init_utimbuf(struct utimbuf *utim);
int jsyndicatefs_uninit_utimbuf(struct utimbuf *utim);

int jsyndicatefs_init_JSFSUtimbuf_Structure(JNIEnv *jenv);
int jsyndicatefs_uninit_JSFSUtimbuf_Structure(JNIEnv *jenv);
int jsyndicatefs_copy_JSFSUtimbuf_to_Native(JNIEnv *jenv, jobject jobj, struct utimbuf *utim);
int jsyndicatefs_copy_Native_to_JSFSUtimbuf(JNIEnv *jenv, jobject jobj, struct utimbuf *utim);

#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFSUTIMBUF_H */

