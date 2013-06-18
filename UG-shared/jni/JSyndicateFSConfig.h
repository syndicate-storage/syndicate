/* 
 * File:   JSyndicateFSConfig.h
 * Author: iychoi
 *
 * Created on June 7, 2013, 1:06 PM
 */

#ifndef JSYNDICATEFSCONFIG_H
#define	JSYNDICATEFSCONFIG_H

#include <libsyndicate.h>

#include <jni.h>
#include <string.h>

#ifdef	__cplusplus
extern "C" {
#endif
    
struct JSyndicateFS_Config {
    char* config_file;
    char* username;
    char* password;
    char* volume_name;
    char* volume_secret;
    char* ms_url;
    int portnum;
};

#define JSFSConfig_Class_Identifier "JSyndicateFSJNI/struct/JSFSConfig"

struct JSFSConfig_Class_Structure {
    jclass      clazz;
    jclass      ref_clazz;
    jfieldID    config_file_id;
    jfieldID    username_id;
    jfieldID    password_id;
    jfieldID    volume_name_id;
    jfieldID    volume_secret_id;
    jfieldID    ms_url_id;
    jfieldID    portnum_id;
};

int jsyndicatefs_init_config(struct JSyndicateFS_Config *cfg);
int jsyndicatefs_uninit_config(struct JSyndicateFS_Config *cfg);

int jsyndicatefs_init_JSFSConfig_Structure(JNIEnv *jenv);
int jsyndicatefs_uninit_JSFSConfig_Structure(JNIEnv *jenv);
int jsyndicatefs_copy_JSFSConfig_to_Native(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_Config *cfg);
int jsyndicatefs_copy_Native_to_JSFSConfig(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_Config *cfg);

#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFSCONFIG_H */

