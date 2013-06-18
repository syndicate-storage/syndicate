#include "JSyndicateFSUtimbuf.h"

/*
 * Global Variable
 */
struct JSFSUtimbuf_Class_Structure jsfsutimbuf_class_structure;

/*
 * Public Functions
 */
int jsyndicatefs_init_utimbuf(struct utimbuf *utim) {
    if(utim != NULL) {
        memset(utim, 0, sizeof(struct utimbuf));
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_uninit_utimbuf(struct utimbuf *utim) {
    if(utim != NULL) {
        memset(utim, 0, sizeof(struct utimbuf));
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_init_JSFSUtimbuf_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    jsfsutimbuf_class_structure.clazz = jenv->FindClass(JSFSUtimbuf_Class_Identifier);
    if(!jsfsutimbuf_class_structure.clazz) return -1;
    
    // allocate global reference
    jsfsutimbuf_class_structure.ref_clazz = (jclass)jenv->NewGlobalRef(jsfsutimbuf_class_structure.clazz);
    if(!jsfsutimbuf_class_structure.ref_clazz) return -1;
    
    // release local reference
    jenv->DeleteLocalRef(jsfsutimbuf_class_structure.clazz);
    
    jsfsutimbuf_class_structure.actime_id = jenv->GetFieldID(jsfsutimbuf_class_structure.ref_clazz, "actime", "J");
    if(!jsfsutimbuf_class_structure.actime_id) return -1;
    
    jsfsutimbuf_class_structure.modtime_id = jenv->GetFieldID(jsfsutimbuf_class_structure.ref_clazz, "modtime", "J");
    if(!jsfsutimbuf_class_structure.modtime_id) return -1;
    
    return 0;
}

int jsyndicatefs_uninit_JSFSUtimbuf_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    if(!jsfsutimbuf_class_structure.ref_clazz) {
        jenv->DeleteGlobalRef(jsfsutimbuf_class_structure.ref_clazz);
        memset(&jsfsutimbuf_class_structure, 0, sizeof(struct JSFSUtimbuf_Class_Structure));
    }
    return 0;
}

int jsyndicatefs_copy_JSFSUtimbuf_to_Native(JNIEnv *jenv, jobject jobj, struct utimbuf *utim) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!utim) return -1;
    
    if(!jsfsutimbuf_class_structure.ref_clazz) return -1;
    
    jlong jlong_actime = jenv->GetLongField(jobj, jsfsutimbuf_class_structure.actime_id);
    utim->actime = (time_t)jlong_actime;
    
    jlong jlong_modtime = jenv->GetLongField(jobj, jsfsutimbuf_class_structure.modtime_id);
    utim->modtime = (time_t)jlong_modtime;
    
    return 0;
}

int jsyndicatefs_copy_Native_to_JSFSUtimbuf(JNIEnv *jenv, jobject jobj, struct utimbuf *utim) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!utim) return -1;
    
    if(!jsfsutimbuf_class_structure.ref_clazz) return -1;
    
    jlong jlong_actime = (jlong)utim->actime;
    jenv->SetLongField(jobj, jsfsutimbuf_class_structure.actime_id, jlong_actime);
    
    jlong jlong_modtime = (jlong)utim->modtime;
    jenv->SetLongField(jobj, jsfsutimbuf_class_structure.modtime_id, jlong_modtime);
    
    return 0;
}

