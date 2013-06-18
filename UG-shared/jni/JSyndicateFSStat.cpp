#include "JSyndicateFSStat.h"

/*
 * Global Variable
 */
struct JSFSStat_Class_Structure jsfsstat_class_structure;

/*
 * Public Functions
 */
int jsyndicatefs_init_stat(struct stat *statbuf) {
    if(statbuf != NULL) {
        memset(statbuf, 0, sizeof(struct stat));
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_uninit_stat(struct stat *statbuf) {
    if(statbuf != NULL) {
        memset(statbuf, 0, sizeof(struct stat));
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_init_JSFSStat_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    jsfsstat_class_structure.clazz = jenv->FindClass(JSFSStat_Class_Identifier);
    if(!jsfsstat_class_structure.clazz) return -1;
    
    // allocate global reference
    jsfsstat_class_structure.ref_clazz = (jclass)jenv->NewGlobalRef(jsfsstat_class_structure.clazz);
    if(!jsfsstat_class_structure.ref_clazz) return -1;
    
    // release local reference
    jenv->DeleteLocalRef(jsfsstat_class_structure.clazz);
    
    jsfsstat_class_structure.constructor_id = jenv->GetMethodID(jsfsstat_class_structure.ref_clazz, "<init>", "()V");
    if(!jsfsstat_class_structure.constructor_id) return -1;

    jsfsstat_class_structure.st_dev_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_dev", "J");
    if(!jsfsstat_class_structure.st_dev_id) return -1;
    
    jsfsstat_class_structure.st_ino_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_ino", "J");
    if(!jsfsstat_class_structure.st_ino_id) return -1;
    
    jsfsstat_class_structure.st_mode_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_mode", "I");
    if(!jsfsstat_class_structure.st_mode_id) return -1;
    
    jsfsstat_class_structure.st_nlink_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_nlink", "J");
    if(!jsfsstat_class_structure.st_nlink_id) return -1;
    
    jsfsstat_class_structure.st_uid_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_uid", "I");
    if(!jsfsstat_class_structure.st_uid_id) return -1;
    
    jsfsstat_class_structure.st_gid_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_gid", "I");
    if(!jsfsstat_class_structure.st_gid_id) return -1;
    
    jsfsstat_class_structure.st_rdev_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_rdev", "J");
    if(!jsfsstat_class_structure.st_rdev_id) return -1;
    
    jsfsstat_class_structure.st_size_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_size", "J");
    if(!jsfsstat_class_structure.st_size_id) return -1;
    
    jsfsstat_class_structure.st_blksize_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_blksize", "J");
    if(!jsfsstat_class_structure.st_blksize_id) return -1;
    
    jsfsstat_class_structure.st_blocks_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_blocks", "J");
    if(!jsfsstat_class_structure.st_blocks_id) return -1;
    
    jsfsstat_class_structure.st_atim_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_atim", "J");
    if(!jsfsstat_class_structure.st_atim_id) return -1;
    
    jsfsstat_class_structure.st_mtim_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_mtim", "J");
    if(!jsfsstat_class_structure.st_mtim_id) return -1;
    
    jsfsstat_class_structure.st_ctim_id = jenv->GetFieldID(jsfsstat_class_structure.ref_clazz, "st_ctim", "J");
    if(!jsfsstat_class_structure.st_ctim_id) return -1;
    
    return 0;
}

int jsyndicatefs_uninit_JSFSStat_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    if(!jsfsstat_class_structure.ref_clazz) {
        jenv->DeleteGlobalRef(jsfsstat_class_structure.ref_clazz);
        memset(&jsfsstat_class_structure, 0, sizeof(struct JSFSStat_Class_Structure));
    }
    return 0;
}

int jsyndicatefs_copy_JSFSStat_to_Native(JNIEnv *jenv, jobject jobj, struct stat *statbuf) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!statbuf) return -1;
    
    if(!jsfsstat_class_structure.ref_clazz) return -1;
    
    jlong jlong_st_dev = jenv->GetLongField(jobj, jsfsstat_class_structure.st_dev_id);
    statbuf->st_dev = (long)jlong_st_dev;
    
    jlong jlong_st_ino = jenv->GetLongField(jobj, jsfsstat_class_structure.st_ino_id);
    statbuf->st_ino = (long)jlong_st_ino;
    
    jint jint_st_mode = jenv->GetIntField(jobj, jsfsstat_class_structure.st_mode_id);
    statbuf->st_mode = (int)jint_st_mode;
    
    jlong jlong_st_nlink = jenv->GetLongField(jobj, jsfsstat_class_structure.st_nlink_id);
    statbuf->st_nlink = (long)jlong_st_nlink;
    
    jint jint_st_uid = jenv->GetIntField(jobj, jsfsstat_class_structure.st_uid_id);
    statbuf->st_uid = (int)jint_st_uid;
    
    jint jint_st_gid = jenv->GetIntField(jobj, jsfsstat_class_structure.st_gid_id);
    statbuf->st_gid = (int)jint_st_gid;
    
    jlong jlong_st_rdev = jenv->GetLongField(jobj, jsfsstat_class_structure.st_rdev_id);
    statbuf->st_rdev = (long)jlong_st_rdev;
    
    jlong jlong_st_size = jenv->GetLongField(jobj, jsfsstat_class_structure.st_size_id);
    statbuf->st_size = (long)jlong_st_size;
    
    jlong jlong_st_blksize = jenv->GetLongField(jobj, jsfsstat_class_structure.st_blksize_id);
    statbuf->st_blksize = (long)jlong_st_blksize;
    
    jlong jlong_st_blocks = jenv->GetLongField(jobj, jsfsstat_class_structure.st_blocks_id);
    statbuf->st_blocks = (long)jlong_st_blocks;
    
    jlong jlong_st_atim = jenv->GetLongField(jobj, jsfsstat_class_structure.st_atim_id);
    statbuf->st_atim.tv_sec = (long)jlong_st_atim;
    
    jlong jlong_st_mtim = jenv->GetLongField(jobj, jsfsstat_class_structure.st_mtim_id);
    statbuf->st_mtim.tv_sec = (long)jlong_st_mtim;
    
    jlong jlong_st_ctim = jenv->GetLongField(jobj, jsfsstat_class_structure.st_ctim_id);
    statbuf->st_ctim.tv_sec = (long)jlong_st_ctim;
    
    return 0;
}

int jsyndicatefs_copy_Native_to_JSFSStat(JNIEnv *jenv, jobject jobj, struct stat *statbuf) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!statbuf) return -1;
    
    if(!jsfsstat_class_structure.ref_clazz) return -1;
    
    jlong jlong_st_dev = (jlong)statbuf->st_dev;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_dev_id, jlong_st_dev);
    
    jlong jlong_st_ino = (jlong)statbuf->st_ino;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_ino_id, jlong_st_ino);
    
    jint jint_st_mode = (jint)statbuf->st_mode;
    jenv->SetIntField(jobj, jsfsstat_class_structure.st_mode_id, jint_st_mode);
    
    jlong jlong_st_nlink = (jlong)statbuf->st_nlink;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_nlink_id, jlong_st_nlink);
    
    jint jint_st_uid = (jint)statbuf->st_uid;
    jenv->SetIntField(jobj, jsfsstat_class_structure.st_uid_id, jint_st_uid);
    
    jint jint_st_gid = (jint)statbuf->st_gid;
    jenv->SetIntField(jobj, jsfsstat_class_structure.st_gid_id, jint_st_gid);
    
    jlong jlong_st_rdev = (jlong)statbuf->st_rdev;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_rdev_id, jlong_st_rdev);
    
    jlong jlong_st_size = (jlong)statbuf->st_size;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_size_id, jlong_st_size);
    
    jlong jlong_st_blksize = (jlong)statbuf->st_blksize;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_blksize_id, jlong_st_blksize);
    
    jlong jlong_st_blocks = (jlong)statbuf->st_blocks;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_blocks_id, jlong_st_blocks);
    
    jlong jlong_st_atim = (jlong)statbuf->st_atim.tv_sec;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_atim_id, jlong_st_atim);

    jlong jlong_st_mtim = (jlong)statbuf->st_mtim.tv_sec;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_mtim_id, jlong_st_mtim);
    
    jlong jlong_st_ctim = (jlong)statbuf->st_ctim.tv_sec;
    jenv->SetLongField(jobj, jsfsstat_class_structure.st_ctim_id, jlong_st_ctim);
    
    return 0;
}

jobject jsyndicatefs_create_JSFSStat(JNIEnv *jenv) {
    if(!jenv) return NULL;
    
    jobject jobj_stat = jenv->NewObject(jsfsstat_class_structure.ref_clazz, jsfsstat_class_structure.constructor_id);
    return jobj_stat;
}