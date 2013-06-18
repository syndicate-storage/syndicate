#include "JSyndicateFSFillDir.h"

/*
 * Global Variable
 */
struct JSFSFillDir_Class_Structure jsfsfilldir_class_structure;

/*
 * Public Functions
 */
int jsyndicatefs_init_JSFSFillDir_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    jsfsfilldir_class_structure.clazz = jenv->FindClass(JSFSFillDir_Class_Identifier);
    if(!jsfsfilldir_class_structure.clazz) return -1;
    
    // allocate global reference
    jsfsfilldir_class_structure.ref_clazz = (jclass)jenv->NewGlobalRef(jsfsfilldir_class_structure.clazz);
    if(!jsfsfilldir_class_structure.ref_clazz) return -1;
    
    // release local reference
    jenv->DeleteLocalRef(jsfsfilldir_class_structure.clazz);
    
    jsfsfilldir_class_structure.fill_id = jenv->GetMethodID(jsfsfilldir_class_structure.ref_clazz, "fill", "(Ljava/lang/String;LJSyndicateFSJNI/struct/JSFSStat;J)V");
    if(!jsfsfilldir_class_structure.fill_id) return -1;
    
    return 0;
}

int jsyndicatefs_uninit_JSFSFillDir_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    if(!jsfsfilldir_class_structure.ref_clazz) {
        jenv->DeleteGlobalRef(jsfsfilldir_class_structure.ref_clazz);
        memset(&jsfsfilldir_class_structure, 0, sizeof(struct JSFSFillDir_Class_Structure));
    }
    return 0;
}

int jsyndicatefs_call_JSFSFillDir(JNIEnv *jenv, jobject jobj, const char *name, const struct stat *stbuf, off_t off) {
    if(!jenv) return -1;
    if(!jobj) return -1;

    if(!jsfsfilldir_class_structure.ref_clazz) return -1;

    jstring jstr_name = NULL;
    if(name) {
        jstr_name = jenv->NewStringUTF(name);
        if(!jstr_name) return -1;
    }
    
    jobject jobj_stat = NULL;
    if(stbuf) {
        
        jobj_stat = jsyndicatefs_create_JSFSStat(jenv);
        if(!jobj_stat) return -1;
        
        int result = jsyndicatefs_copy_Native_to_JSFSStat(jenv, jobj_stat, (struct stat *)stbuf);
        if(result != 0) return result;
    }
    
    jlong jlong_offset = (jlong) off;
    
    // call
    jenv->CallVoidMethod(jobj, jsfsfilldir_class_structure.fill_id, jstr_name, jobj_stat, jlong_offset);
    
    return 0;
}
