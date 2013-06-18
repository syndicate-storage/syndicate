#include "JSyndicateFSStatvfs.h"

/*
 * Global Variable
 */
struct JSFSStatvfs_Class_Structure jsfsstatvfs_class_structure;

/*
 * Public Functions
 */
int jsyndicatefs_init_statvfs(struct statvfs *statv) {
    if(statv != NULL) {
        memset(statv, 0, sizeof(struct statvfs));
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_uninit_statvfs(struct statvfs *statv) {
    if(statv != NULL) {
        memset(statv, 0, sizeof(struct statvfs));
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_init_JSFSStatvfs_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    jsfsstatvfs_class_structure.clazz = jenv->FindClass(JSFSStatvfs_Class_Identifier);
    if(!jsfsstatvfs_class_structure.clazz) return -1;
    
    // allocate global reference
    jsfsstatvfs_class_structure.ref_clazz = (jclass)jenv->NewGlobalRef(jsfsstatvfs_class_structure.clazz);
    if(!jsfsstatvfs_class_structure.ref_clazz) return -1;
    
    // release local reference
    jenv->DeleteLocalRef(jsfsstatvfs_class_structure.clazz);
    
    jsfsstatvfs_class_structure.f_bsize_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_bsize", "J");
    if(!jsfsstatvfs_class_structure.f_bsize_id) return -1;
    
    jsfsstatvfs_class_structure.f_frsize_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_frsize", "J");
    if(!jsfsstatvfs_class_structure.f_frsize_id) return -1;
    
    jsfsstatvfs_class_structure.f_blocks_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_blocks", "J");
    if(!jsfsstatvfs_class_structure.f_blocks_id) return -1;
    
    jsfsstatvfs_class_structure.f_bfree_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_bfree", "J");
    if(!jsfsstatvfs_class_structure.f_bfree_id) return -1;
    
    jsfsstatvfs_class_structure.f_bavail_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_bavail", "J");
    if(!jsfsstatvfs_class_structure.f_bavail_id) return -1;
    
    jsfsstatvfs_class_structure.f_files_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_files", "J");
    if(!jsfsstatvfs_class_structure.f_files_id) return -1;
    
    jsfsstatvfs_class_structure.f_ffree_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_ffree", "J");
    if(!jsfsstatvfs_class_structure.f_ffree_id) return -1;
    
    jsfsstatvfs_class_structure.f_favail_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_favail", "J");
    if(!jsfsstatvfs_class_structure.f_favail_id) return -1;
    
    jsfsstatvfs_class_structure.f_fsid_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_fsid", "J");
    if(!jsfsstatvfs_class_structure.f_fsid_id) return -1;
    
    jsfsstatvfs_class_structure.f_flag_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_flag", "J");
    if(!jsfsstatvfs_class_structure.f_flag_id) return -1;
    
    jsfsstatvfs_class_structure.f_namemax_id = jenv->GetFieldID(jsfsstatvfs_class_structure.ref_clazz, "f_namemax", "J");
    if(!jsfsstatvfs_class_structure.f_namemax_id) return -1;
    
    return 0;
}

int jsyndicatefs_uninit_JSFSStatvfs_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    if(!jsfsstatvfs_class_structure.ref_clazz) {
        jenv->DeleteGlobalRef(jsfsstatvfs_class_structure.ref_clazz);
        memset(&jsfsstatvfs_class_structure, 0, sizeof(struct JSFSStatvfs_Class_Structure));
    }
    return 0;
}

int jsyndicatefs_copy_JSFSStatvfs_to_Native(JNIEnv *jenv, jobject jobj, struct statvfs *statv) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!statv) return -1;
    
    if(!jsfsstatvfs_class_structure.ref_clazz) return -1;
    
    jlong jlong_f_bsize = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_bsize_id);
    statv->f_bsize = (long)jlong_f_bsize;
    
    jlong jlong_f_frsize = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_frsize_id);
    statv->f_frsize = (long)jlong_f_frsize;
    
    jlong jlong_f_blocks = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_blocks_id);
    statv->f_blocks = (long)jlong_f_blocks;
    
    jlong jlong_f_bfree = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_bfree_id);
    statv->f_bfree = (long)jlong_f_bfree;
    
    jlong jlong_f_bavail = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_bavail_id);
    statv->f_bavail = (long)jlong_f_bavail;
    
    jlong jlong_f_files = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_files_id);
    statv->f_files = (long)jlong_f_files;
    
    jlong jlong_f_ffree = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_ffree_id);
    statv->f_ffree = (long)jlong_f_ffree;
    
    jlong jlong_f_favail = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_favail_id);
    statv->f_favail = (long)jlong_f_favail;
    
    jlong jlong_f_fsid = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_fsid_id);
    statv->f_fsid = (long)jlong_f_fsid;
    
    jlong jlong_f_flag = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_flag_id);
    statv->f_flag = (long)jlong_f_flag;
    
    jlong jlong_f_namemax = jenv->GetLongField(jobj, jsfsstatvfs_class_structure.f_namemax_id);
    statv->f_namemax = (long)jlong_f_namemax;
    
    return 0;
}

int jsyndicatefs_copy_Native_to_JSFSStatvfs(JNIEnv *jenv, jobject jobj, struct statvfs *statv) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!statv) return -1;
    
    if(!jsfsstatvfs_class_structure.ref_clazz) return -1;
    
    jlong jlong_f_bsize = (jlong)statv->f_bsize;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_bsize_id, jlong_f_bsize);
    
    jlong jlong_f_frsize = (jlong)statv->f_frsize;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_frsize_id, jlong_f_frsize);
    
    jlong jlong_f_blocks = (jlong)statv->f_blocks;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_blocks_id, jlong_f_blocks);
    
    jlong jlong_f_bfree = (jlong)statv->f_bfree;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_bfree_id, jlong_f_bfree);
    
    jlong jlong_f_bavail = (jlong)statv->f_bavail;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_bavail_id, jlong_f_bavail);
    
    jlong jlong_f_files = (jlong)statv->f_files;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_files_id, jlong_f_files);
    
    jlong jlong_f_ffree = (jlong)statv->f_ffree;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_ffree_id, jlong_f_ffree);
    
    jlong jlong_f_favail = (jlong)statv->f_favail;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_favail_id, jlong_f_favail);
    
    jlong jlong_f_fsid = (jlong)statv->f_fsid;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_fsid_id, jlong_f_fsid);
    
    jlong jlong_f_flag = (jlong)statv->f_flag;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_flag_id, jlong_f_flag);
    
    jlong jlong_f_namemax = (jlong)statv->f_namemax;
    jenv->SetLongField(jobj, jsfsstatvfs_class_structure.f_namemax_id, jlong_f_namemax);
    
    return 0;
}
