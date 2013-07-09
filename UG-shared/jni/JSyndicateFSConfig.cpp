#include "JSyndicateFSConfig.h"

/*
 * Global Variable
 */
struct JSFSConfig_Class_Structure jsfsconfig_class_structure;

/*
 * Public Functions
 */
int jsyndicatefs_init_config(struct JSyndicateFS_Config *cfg) {
    if(cfg != NULL) {
        memset(cfg, 0, sizeof(struct JSyndicateFS_Config));
        cfg->portnum = -1;
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_uninit_config(struct JSyndicateFS_Config *cfg) {
    if(cfg != NULL) {
        if(cfg->config_file) free(cfg->config_file);
        if(cfg->ug_name) free(cfg->ug_name);
        if(cfg->ug_password) free(cfg->ug_password);
        if(cfg->volume_name) free(cfg->volume_name);
        if(cfg->volume_secret) free(cfg->volume_secret);
        if(cfg->ms_url) free(cfg->ms_url);
        
        memset(cfg, 0, sizeof(struct JSyndicateFS_Config));
        cfg->portnum = -1;
        return 0;
    }
    
    return -1;
}

int jsyndicatefs_init_JSFSConfig_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    jsfsconfig_class_structure.clazz = jenv->FindClass(JSFSConfig_Class_Identifier);
    if(!jsfsconfig_class_structure.clazz) return -1;
    
    // allocate global reference
    jsfsconfig_class_structure.ref_clazz = (jclass)jenv->NewGlobalRef(jsfsconfig_class_structure.clazz);
    if(!jsfsconfig_class_structure.ref_clazz) return -1;
    
    // release local reference
    jenv->DeleteLocalRef(jsfsconfig_class_structure.clazz);
    
    jsfsconfig_class_structure.config_file_id = jenv->GetFieldID(jsfsconfig_class_structure.ref_clazz, "config_file", "Ljava/lang/String;");
    if(!jsfsconfig_class_structure.config_file_id) return -1;
    
    jsfsconfig_class_structure.ug_name_id = jenv->GetFieldID(jsfsconfig_class_structure.ref_clazz, "ug_name", "Ljava/lang/String;");
    if(!jsfsconfig_class_structure.ug_name_id) return -1;
    
    jsfsconfig_class_structure.ug_password_id = jenv->GetFieldID(jsfsconfig_class_structure.ref_clazz, "ug_password", "Ljava/lang/String;");
    if(!jsfsconfig_class_structure.ug_password_id) return -1;
    
    jsfsconfig_class_structure.volume_name_id = jenv->GetFieldID(jsfsconfig_class_structure.ref_clazz, "volume_name", "Ljava/lang/String;");
    if(!jsfsconfig_class_structure.volume_name_id) return -1;
    
    jsfsconfig_class_structure.volume_secret_id = jenv->GetFieldID(jsfsconfig_class_structure.ref_clazz, "volume_secret", "Ljava/lang/String;");
    if(!jsfsconfig_class_structure.volume_secret_id) return -1;
    
    jsfsconfig_class_structure.ms_url_id = jenv->GetFieldID(jsfsconfig_class_structure.ref_clazz, "ms_url", "Ljava/lang/String;");
    if(!jsfsconfig_class_structure.ms_url_id) return -1;
    
    jsfsconfig_class_structure.portnum_id = jenv->GetFieldID(jsfsconfig_class_structure.ref_clazz, "portnum", "I");
    if(!jsfsconfig_class_structure.portnum_id) return -1;
    
    return 0;
}

int jsyndicatefs_uninit_JSFSConfig_Structure(JNIEnv *jenv) {
    if(!jenv) return -1;
    
    if(!jsfsconfig_class_structure.ref_clazz) {
        jenv->DeleteGlobalRef(jsfsconfig_class_structure.ref_clazz);
        memset(&jsfsconfig_class_structure, 0, sizeof(struct JSFSConfig_Class_Structure));
    }
    return 0;
}

int jsyndicatefs_copy_JSFSConfig_to_Native(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_Config *cfg) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!cfg) return -1;
    
    if(!jsfsconfig_class_structure.ref_clazz) return -1;
    
    jstring obj_config_file = (jstring)jenv->GetObjectField(jobj, jsfsconfig_class_structure.config_file_id);
    if(obj_config_file) {
        const char *pc = jenv->GetStringUTFChars(obj_config_file, 0);
        
        if(pc) {
            cfg->config_file = strdup(pc); // alloc & copy
        } else {
            cfg->config_file = NULL;
        }
        
        jenv->ReleaseStringUTFChars(obj_config_file, pc);
    }
    
    jstring obj_ug_name = (jstring)jenv->GetObjectField(jobj, jsfsconfig_class_structure.ug_name_id);
    if(obj_ug_name) {
        const char *pc = jenv->GetStringUTFChars(obj_ug_name, 0);
        
        if(pc) {
            cfg->ug_name = strdup(pc); // alloc & copy
        } else {
            cfg->ug_name = NULL;
        }
        
        jenv->ReleaseStringUTFChars(obj_ug_name, pc);
    }
    
    jstring obj_ug_password = (jstring)jenv->GetObjectField(jobj, jsfsconfig_class_structure.ug_password_id);
    if(obj_ug_password) {
        const char *pc = jenv->GetStringUTFChars(obj_ug_password, 0);
        
        if(pc) {
            cfg->ug_password = strdup(pc); // alloc & copy
        } else {
            cfg->ug_password = NULL;
        }
        
        jenv->ReleaseStringUTFChars(obj_ug_password, pc);
    }
    
    jstring obj_volume_name = (jstring)jenv->GetObjectField(jobj, jsfsconfig_class_structure.volume_name_id);
    if(obj_volume_name) {
        const char *pc = jenv->GetStringUTFChars(obj_volume_name, 0);
        
        if(pc) {
            cfg->volume_name = strdup(pc); // alloc & copy
        } else {
            cfg->volume_name = NULL;
        }
        
        jenv->ReleaseStringUTFChars(obj_volume_name, pc);
    }
    
    jstring obj_volume_secret = (jstring)jenv->GetObjectField(jobj, jsfsconfig_class_structure.volume_secret_id);
    if(obj_volume_secret) {
        const char *pc = jenv->GetStringUTFChars(obj_volume_secret, 0);
        
        if(pc) {
            cfg->volume_secret = strdup(pc); // alloc & copy
        } else {
            cfg->volume_secret = NULL;
        }
        
        jenv->ReleaseStringUTFChars(obj_volume_secret, pc);
    }
    
    jstring obj_ms_url = (jstring)jenv->GetObjectField(jobj, jsfsconfig_class_structure.ms_url_id);
    if(obj_ms_url) {
        const char *pc = jenv->GetStringUTFChars(obj_ms_url, 0);
        
        if(pc) {
            cfg->ms_url = strdup(pc); // alloc & copy
        } else {
            cfg->ms_url = NULL;
        }
        
        jenv->ReleaseStringUTFChars(obj_ms_url, pc);
    }
    
    jint jint_portnum = jenv->GetIntField(jobj, jsfsconfig_class_structure.portnum_id);
    cfg->portnum = (int)jint_portnum;
    
    return 0;
}

int jsyndicatefs_copy_Native_to_JSFSConfig(JNIEnv *jenv, jobject jobj, struct JSyndicateFS_Config *cfg) {
    if(!jenv) return -1;
    if(!jobj) return -1;
    if(!cfg) return -1;
    
    if(!jsfsconfig_class_structure.ref_clazz) return -1;

    if(cfg->config_file) {
        jstring obj_config_file = jenv->NewStringUTF(cfg->config_file);
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.config_file_id, (jobject)obj_config_file);
    } else {
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.config_file_id, (jobject)NULL);
    }
    
    if(cfg->ug_name) {
        jstring obj_ug_name = jenv->NewStringUTF(cfg->ug_name);
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.ug_name_id, (jobject)obj_ug_name);
    } else {
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.ug_name_id, (jobject)NULL);
    }
    
    if(cfg->ug_password) {
        jstring obj_ug_password = jenv->NewStringUTF(cfg->ug_password);
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.ug_password_id, (jobject)obj_ug_password);
    } else {
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.ug_password_id, (jobject)NULL);
    }
    
    if(cfg->volume_name) {
        jstring obj_volume_name = jenv->NewStringUTF(cfg->volume_name);
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.volume_name_id, (jobject)obj_volume_name);
    } else {
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.volume_name_id, (jobject)NULL);
    }
    
    if(cfg->volume_secret) {
        jstring obj_volume_secret = jenv->NewStringUTF(cfg->volume_secret);
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.volume_secret_id, (jobject)obj_volume_secret);
    } else {
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.volume_secret_id, (jobject)NULL);
    }

    if(cfg->ms_url) {
        jstring obj_ms_url = jenv->NewStringUTF(cfg->ms_url);
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.ms_url_id, (jobject)obj_ms_url);
    } else {
        jenv->SetObjectField(jobj, jsfsconfig_class_structure.ms_url_id, (jobject)NULL);
    }
    
    jint jint_portnum = (jint)cfg->portnum;
    jenv->SetIntField(jobj, jsfsconfig_class_structure.portnum_id, jint_portnum);
    
    return 0;
}
