#include "JSyndicateFSFileInfo.h"

/*
 * Global Variable
 */
struct JSFSFileInfo_Class_Structure jsfsfileinfo_class_structure;

/*
 * Public Functions
 */
int jsyndicatefs_init_fileinfo(struct JSyndicateFS_FileInfo* fi) {
    if(fi != NULL) {
        fi->direct_io = 1;
        fi->fh = NULL;
        fi->flags = 0;
        
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_uninit_fileinfo(struct JSyndicateFS_FileInfo *fi) {
    if(fi != NULL) {
        memset(fi, 0, sizeof(struct JSyndicateFS_FileInfo));
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_init_JSFSFileInfo_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    jsfsfileinfo_class_structure.clazz = jenv->FindClass(JSFSFileInfo_Class_Identifier);
    if(!jsfsfileinfo_class_structure.clazz) return -1;
    
    // allocate global reference
    jsfsfileinfo_class_structure.ref_clazz = (jclass)jenv->NewGlobalRef(jsfsfileinfo_class_structure.clazz);
    if(!jsfsfileinfo_class_structure.ref_clazz) return -1;
    
    // release local reference
    jenv->DeleteLocalRef(jsfsfileinfo_class_structure.clazz);
    
    jsfsfileinfo_class_structure.flags_id = jenv->GetFieldID(jsfsfileinfo_class_structure.ref_clazz, "flags", "I");
    if(!jsfsfileinfo_class_structure.flags_id) return -1;
    
    jsfsfileinfo_class_structure.direct_io_id = jenv->GetFieldID(jsfsfileinfo_class_structure.ref_clazz, "direct_io", "I");
    if(!jsfsfileinfo_class_structure.direct_io_id) return -1;
    
    jsfsfileinfo_class_structure.fh_id = jenv->GetFieldID(jsfsfileinfo_class_structure.ref_clazz, "fh", "J");
    if(!jsfsfileinfo_class_structure.fh_id) return -1;
    
    return 0;
}

int jsyndicatefs_uninit_JSFSFileInfo_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    if(!jsfsfileinfo_class_structure.ref_clazz) {
        jenv->DeleteGlobalRef(jsfsfileinfo_class_structure.ref_clazz);
        memset(&jsfsfileinfo_class_structure, 0, sizeof(struct JSFSFileInfo_Class_Structure));
    }
    return 0;
}

int jsyndicatefs_copy_JSFSFileInfo_to_Native(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_FileInfo *fi) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!fi) return -1;
    
    if(!jsfsfileinfo_class_structure.ref_clazz) return -1;
    
    jint jint_flags = jenv->GetIntField(jobj, jsfsfileinfo_class_structure.flags_id);
    fi->flags = (int)jint_flags;
    
    jint jint_direct_io = jenv->GetIntField(jobj, jsfsfileinfo_class_structure.direct_io_id);
    fi->direct_io = (int)jint_direct_io;
    
    jlong jlong_fh = jenv->GetLongField(jobj, jsfsfileinfo_class_structure.fh_id);
    fi->fh = (void*)jlong_fh;
    
    return 0;
}

int jsyndicatefs_copy_Native_to_JSFSFileInfo(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_FileInfo *fi) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!fi) return -1;
    
    if(!jsfsfileinfo_class_structure.ref_clazz) return -1;

    jint jint_flags = (jint)fi->flags;
    jenv->SetIntField(jobj, jsfsfileinfo_class_structure.flags_id, jint_flags);
    
    jint jint_direct_io = (jint)fi->direct_io;
    jenv->SetIntField(jobj, jsfsfileinfo_class_structure.direct_io_id, jint_direct_io);
    
    jlong jlong_fh = (jlong)fi->fh;
    jenv->SetLongField(jobj, jsfsfileinfo_class_structure.fh_id, jlong_fh);
    
    return 0;
}