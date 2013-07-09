// Implementation of the JSyndicateFSJni methods.

#include "JSyndicateFSJni.h"

/*
 * Global Variable
 */
struct JSyndicateFS_Config cfg;

/*
 * Private Functions
 */
static void JavaThrowException(JNIEnv *jenv, JavaExceptionCodes code, const char *msg) {
    jclass excep;
    static const struct JavaExceptions_t java_exceptions[] = {
        { JavaOutOfMemoryError, "java/lang/OutOfMemoryError"},
        { JavaIOException, "java/io/IOException"},
        { JavaRuntimeException, "java/lang/RuntimeException"},
        { JavaIndexOutOfBoundsException, "java/lang/IndexOutOfBoundsException"},
        { JavaArithmeticException, "java/lang/ArithmeticException"},
        { JavaIllegalArgumentException, "java/lang/IllegalArgumentException"},
        { JavaNullPointerException, "java/lang/NullPointerException"},
        { JavaDirectorPureVirtual, "java/lang/RuntimeException"},
        { JavaUnknownError, "java/lang/UnknownError"},
        { (JavaExceptionCodes) 0, "java/lang/UnknownError"}
    };
    const JavaExceptions_t *except_ptr = java_exceptions;

    while (except_ptr->code != code && except_ptr->code)
        except_ptr++;

    jenv->ExceptionClear();
    excep = jenv->FindClass(except_ptr->java_exception);
    if (excep)
        jenv->ThrowNew(excep, msg);
}

/*
 * Public Functions
 */

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_init
 * Signature: (LJSyndicateFS/struct/JSFSConfig;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1init(JNIEnv *jenv, jclass jcls, jobject jarg1) {
    jint jresult = 0;
    int result;

    //(void) jenv;
    (void) jcls;
    
    /*
     * Init internal structures for JNI
     */
    result = jsyndicatefs_init_JSFSConfig_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_init_JSFSStat_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_init_JSFSUtimbuf_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_init_JSFSFileInfo_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_init_JSFSStatvfs_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_init_JSFSFillDir_Structure(jenv);
    if(result) return -1;
    
    
    // read object - java to c++
    result = jsyndicatefs_init_config(&cfg);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSConfig_to_Native(jenv, jarg1, &cfg);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("cfg.config_file = %s\n", cfg.config_file);
    printf("cfg.ms_url = %s\n", cfg.ms_url);
    printf("cfg.ug_name = %s\n", cfg.ug_name);
    printf("cfg.ug_password = %s\n", cfg.ug_password);
    printf("cfg.volume_name = %s\n", cfg.volume_name);
    printf("cfg.volume_secret = %s\n", cfg.volume_secret);
    printf("cfg.portnum = %d\n", cfg.portnum);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_init(&cfg);
    jresult = (jint) result;
#endif
    
    // return object - c++ to java
    // do not overwrite value
    //result = jsyndicatefs_copy_Native_to_JSFSConfig(jenv, jarg1, &cfg);
    //if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_destroy
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1destroy(JNIEnv *jenv, jclass jcls) {
    jint jresult = 0;
    int result;

    //(void) jenv;
    (void) jcls;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_destroy();
    jresult = (jint) result;
#endif
    
    result = jsyndicatefs_uninit_config(&cfg);
    if(result) return -1;
    
    /*
     * Uninit internal structures for JNI
     */
    result = jsyndicatefs_uninit_JSFSConfig_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_uninit_JSFSStat_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_uninit_JSFSUtimbuf_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_uninit_JSFSFileInfo_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_uninit_JSFSStatvfs_Structure(jenv);
    if(result) return -1;
    
    result = jsyndicatefs_uninit_JSFSFillDir_Structure(jenv);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_getattr
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSStat;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1getattr(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct stat statbuf;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_stat(&statbuf);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSStat_to_Native(jenv, jarg2, &statbuf);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("statbuf.st_dev = %ld\n", statbuf.st_dev);
    printf("statbuf.st_ino = %ld\n", statbuf.st_ino);
    printf("statbuf.st_mode = %d\n", statbuf.st_mode);
    printf("statbuf.st_nlink = %ld\n", statbuf.st_nlink);
    printf("statbuf.st_uid = %d\n", statbuf.st_uid);
    printf("statbuf.st_gid = %d\n", statbuf.st_gid);
    printf("statbuf.st_rdev = %ld\n", statbuf.st_rdev);
    printf("statbuf.st_size = %ld\n", statbuf.st_size);
    printf("statbuf.st_blksize = %ld\n", statbuf.st_blksize);
    printf("statbuf.st_blocks = %ld\n", statbuf.st_blocks);
    printf("statbuf.st_atim = %ld\n", statbuf.st_atim.tv_sec);
    printf("statbuf.st_mtim = %ld\n", statbuf.st_mtim.tv_sec);
    printf("statbuf.st_ctim = %ld\n", statbuf.st_ctim.tv_sec);
    
    statbuf.st_dev++;
    statbuf.st_ino++;
    statbuf.st_mode++;
    statbuf.st_nlink++;
    statbuf.st_uid++;
    statbuf.st_gid++;
    statbuf.st_rdev++;
    statbuf.st_size++;
    statbuf.st_blksize++;
    statbuf.st_blocks++;
    statbuf.st_atim.tv_sec++;
    statbuf.st_mtim.tv_sec++;
    statbuf.st_ctim.tv_sec++;
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_getattr((char const *) arg1, &statbuf);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSStat(jenv, jarg2, &statbuf);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_mknod
 * Signature: (Ljava/lang/String;IJ)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1mknod(JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jlong jarg3) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    mode_t arg2;
    dev_t arg3;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (mode_t) jarg2;
    arg3 = (dev_t) jarg3;

#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("mode = %d\n", arg2);
    printf("dev = %ld\n", arg3);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_mknod((char const *) arg1, arg2, arg3);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_mkdir
 * Signature: (Ljava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1mkdir(JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    mode_t arg2;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (mode_t) jarg2;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("mode = %d\n", arg2);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_mkdir((char const *) arg1, arg2);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_unlink
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1unlink(JNIEnv *jenv, jclass jcls, jstring jarg1) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_unlink((char const *) arg1);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_rmdir
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1rmdir(JNIEnv *jenv, jclass jcls, jstring jarg1) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_rmdir((char const *) arg1);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_rename
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1rename(JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = 0;
    if (jarg2) {
        arg2 = (char *) jenv->GetStringUTFChars(jarg2, 0);
        if (!arg2) return -1;
    }
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path1 : %s\n", arg1);
    printf("path2 : %s\n", arg2);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_rename((char const *) arg1, (char const *) arg2);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    if (arg2) jenv->ReleaseStringUTFChars(jarg2, (const char *) arg2);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_chmod
 * Signature: (Ljava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1chmod(JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    mode_t arg2;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (mode_t) jarg2;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    printf("mode = %d\n", arg2);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_chmod((char const *) arg1, arg2);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_truncate
 * Signature: (Ljava/lang/String;J)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1truncate(JNIEnv *jenv, jclass jcls, jstring jarg1, jlong jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    off_t arg2;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (off_t) jarg2;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    printf("newsize = %ld\n", arg2);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_truncate((char const *) arg1, arg2);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_utime
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSUtimbuf;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1utime(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct utimbuf utim;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_utimbuf(&utim);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSUtimbuf_to_Native(jenv, jarg2, &utim);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("utim.actime = %ld\n", utim.actime);
    printf("utim.modtime = %ld\n", utim.modtime);
    
    utim.actime++;
    utim.modtime++;
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_utime((char const *) arg1, &utim);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSUtimbuf(jenv, jarg2, &utim);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_open
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1open(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg2, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_open((char const *) arg1, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg2, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_read
 * Signature: (Ljava/lang/String;[BJJLJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1read
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jbyteArray jarg2, jlong jarg3, jlong jarg4, jobject jarg5) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    jboolean arg2_isCopy;
    jbyte *arg2_barr;
    size_t arg3;
    off_t arg4;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = 0;
    if (jarg2) {
        arg2_barr = (jbyte*) jenv->GetByteArrayElements(jarg2, &arg2_isCopy);
        arg2 = (char *) arg2_barr;
        if (!arg2) return -1;
    }
    
    arg3 = (size_t) jarg3;
    arg4 = (off_t) jarg4;
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg5, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("buf : ");
    for(int i=0;i<arg3;i++) {
        printf("%d", arg2[i]);
    }
    printf("\n");
    printf("size : %ld\n", arg3);
    printf("offset : %ld\n", arg4);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    memset(arg2, 1, arg3);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_read((char const *) arg1, arg2, arg3, arg4, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    if (arg2) jenv->ReleaseByteArrayElements(jarg2, arg2_barr, 0);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg5, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_write
 * Signature: (Ljava/lang/String;[BJJLJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1write(JNIEnv *jenv, jclass jcls, jstring jarg1, jbyteArray jarg2, jlong jarg3, jlong jarg4, jobject jarg5) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    jboolean arg2_isCopy;
    jbyte *arg2_barr;
    size_t arg3;
    off_t arg4;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = 0;
    if (jarg2) {
        arg2_barr = (jbyte*) jenv->GetByteArrayElements(jarg2, &arg2_isCopy);
        arg2 = (char *) arg2_barr;
        if (!arg2) return -1;
    }
    
    arg3 = (size_t) jarg3;
    arg4 = (off_t) jarg4;
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg5, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("buf : ");
    for(int i=0;i<arg3;i++) {
        printf("%d", arg2[i]);
    }
    printf("\n");
    printf("size : %ld\n", arg3);
    printf("offset : %ld\n", arg4);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    memset(arg2, 1, arg3);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_write((char const *) arg1, (char const *) arg2, arg3, arg4, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    if (arg2) jenv->ReleaseByteArrayElements(jarg2, arg2_barr, JNI_ABORT);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg5, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_statfs
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSStatvfs;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1statfs(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct statvfs statv;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_statvfs(&statv);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSStatvfs_to_Native(jenv, jarg2, &statv);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("statv.f_bsize = %ld\n", statv.f_bsize);
    printf("statv.f_frsize = %ld\n", statv.f_frsize);
    printf("statv.f_blocks = %ld\n", statv.f_blocks);
    printf("statv.f_bfree = %ld\n", statv.f_bfree);
    printf("statv.f_bavail = %ld\n", statv.f_bavail);
    printf("statv.f_files = %ld\n", statv.f_files);
    printf("statv.f_ffree = %ld\n", statv.f_ffree);
    printf("statv.f_favail = %ld\n", statv.f_favail);
    printf("statv.f_fsid = %ld\n", statv.f_fsid);
    printf("statv.f_flag = %ld\n", statv.f_flag);
    printf("statv.f_namemax = %ld\n", statv.f_namemax);

    statv.f_bsize++;
    statv.f_frsize++;
    statv.f_blocks++;
    statv.f_bfree++;
    statv.f_bavail++;
    statv.f_files++;
    statv.f_ffree++;
    statv.f_favail++;
    statv.f_fsid++;
    statv.f_flag++;
    statv.f_namemax++;
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_statfs((char const *) arg1, &statv);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSStatvfs(jenv, jarg2, &statv);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_flush
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1flush(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg2, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_flush((char const *) arg1, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg2, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_release
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1release(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }

    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg2, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_release((char const *) arg1, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg2, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_fsync
 * Signature: (Ljava/lang/String;ILJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1fsync(JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jobject jarg3) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    int arg2;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (int) jarg2;
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg3, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("datasync : %d\n", arg2);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_fsync((char const *) arg1, arg2, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg3, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_setxattr
 * Signature: (Ljava/lang/String;Ljava/lang/String;[BJI)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1setxattr(JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2, jbyteArray jarg3, jlong jarg4, jint jarg5) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    char *arg3 = (char *) 0;
    jboolean arg3_isCopy;
    jbyte *arg3_barr;
    size_t arg4;
    int arg5;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = 0;
    if (jarg2) {
        arg2 = (char *) jenv->GetStringUTFChars(jarg2, 0);
        if (!arg2) return -1;
    }
    
    arg3 = 0;
    if (jarg3) {
        arg3_barr = (jbyte*) jenv->GetByteArrayElements(jarg3, &arg3_isCopy);
        arg3 = (char *) arg3_barr;
        if (!arg3) return -1;
    }
    
    arg4 = (size_t) jarg4;
    arg5 = (int) jarg5;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    printf("name : %s\n", arg2);
    
    printf("value : ");
    for(int i=0;i<arg4;i++) {
        printf("%d", arg3[i]);
    }
    printf("\n");
    printf("size : %ld\n", arg4);
    printf("flag : %d\n", arg5);
    
    memset(arg3, 1, arg4);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_setxattr((char const *) arg1, (char const *) arg2, (char const *) arg3, arg4, arg5);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    if (arg2) jenv->ReleaseStringUTFChars(jarg2, (const char *) arg2);
    
    if (arg3) jenv->ReleaseByteArrayElements(jarg3, arg3_barr, JNI_ABORT);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_getxattr
 * Signature: (Ljava/lang/String;Ljava/lang/String;[BJ)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1getxattr(JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2, jbyteArray jarg3, jlong jarg4) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    char *arg3 = (char *) 0;
    jboolean arg3_isCopy;
    jbyte *arg3_barr;
    size_t arg4;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = 0;
    if (jarg2) {
        arg2 = (char *) jenv->GetStringUTFChars(jarg2, 0);
        if (!arg2) return -1;
    }
    
    arg3 = 0;
    if (jarg3) {
        arg3_barr = (jbyte*) jenv->GetByteArrayElements(jarg3, &arg3_isCopy);
        arg3 = (char *) arg3_barr;
        if (!arg3) return -1;
    }
    
    arg4 = (size_t) jarg4;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    printf("name : %s\n", arg2);
    
    printf("value : ");
    for(int i=0;i<arg4;i++) {
        printf("%d", arg3[i]);
    }
    printf("\n");
    printf("size : %ld\n", arg4);
    
    memset(arg3, 1, arg4);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_getxattr((char const *) arg1, (char const *) arg2, arg3, arg4);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    if (arg2) jenv->ReleaseStringUTFChars(jarg2, (const char *) arg2);
    
    if (arg3) jenv->ReleaseByteArrayElements(jarg3, arg3_barr, 0);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_listxattr
 * Signature: (Ljava/lang/String;[BJ)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1listxattr(JNIEnv *jenv, jclass jcls, jstring jarg1, jbyteArray jarg2, jlong jarg3) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    jboolean arg2_isCopy;
    jbyte *arg2_barr;
    size_t arg3;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = 0;
    if (jarg2) {
        arg2_barr = (jbyte*) jenv->GetByteArrayElements(jarg2, &arg2_isCopy);
        arg2 = (char *) arg2_barr;
        if (!arg2) return -1;
    }
    
    arg3 = (size_t) jarg3;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("list : ");
    for(int i=0;i<arg3;i++) {
        printf("%d", arg2[i]);
    }
    printf("\n");
    printf("size : %ld\n", arg3);
    
    memset(arg2, 1, arg3);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_listxattr((char const *) arg1, arg2, arg3);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    if (arg2) jenv->ReleaseByteArrayElements(jarg2, arg2_barr, 0);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_removexattr
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1removexattr(JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = 0;
    if (jarg2) {
        arg2 = (char *) jenv->GetStringUTFChars(jarg2, 0);
        if (!arg2) return -1;
    }
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    printf("name : %s\n", arg2);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_removexattr((char const *) arg1, (char const *) arg2);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    if (arg2) jenv->ReleaseStringUTFChars(jarg2, (const char *) arg2);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_opendir
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1opendir(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg2, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_opendir((char const *) arg1, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg2, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * readdir call back
 */
int jsyndicatefs_readdir_filler_cb(void *pjenv, void *pjobj, const char *name, const struct stat *stbuf, off_t off) {
    JNIEnv *jenv = *(JNIEnv **)pjenv;
    jobject jobj = *(jobject *)pjobj;
    
    return jsyndicatefs_call_JSFSFillDir(jenv, jobj, name, stbuf, off);
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_readdir
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFillDir;JLJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1readdir(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2, jlong jarg3, jobject jarg4) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    off_t arg3;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg3 = (off_t) jarg3;
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg4, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("offset : %ld\n", arg3);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    // call callback
    jsyndicatefs_readdir_filler_cb(&jenv, &jarg2, "testname1", NULL, 1);
    jsyndicatefs_readdir_filler_cb(&jenv, &jarg2, "testname2", NULL, 2);
    jsyndicatefs_readdir_filler_cb(&jenv, &jarg2, "testname3", NULL, 3);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_readdir(&jenv, &jarg2, (char const *) arg1, &jsyndicatefs_readdir_filler_cb, arg3, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg4, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_releasedir
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1releasedir(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg2, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_releasedir((char const *) arg1, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg2, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_fsyncdir
 * Signature: (Ljava/lang/String;ILJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1fsyncdir(JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jobject jarg3) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    int arg2;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (int) jarg2;
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg3, &fi);
    if(result) return -1;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("datasync : %d\n", arg2);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_fsyncdir((char const *) arg1, arg2, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg3, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_access
 * Signature: (Ljava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1access(JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    int arg2;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (int) jarg2;
    
#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("mask : %d\n", arg2);
    
    jresult = (jint) 0;
#else
    result = (int) jsyndicatefs_access((char const *) arg1, arg2);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_create
 * Signature: (Ljava/lang/String;ILJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1create(JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jobject jarg3) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    mode_t arg2;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (mode_t) jarg2;
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg3, &fi);
    if(result) return -1;

#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("mode : %d\n", arg2);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else    
    result = (int) jsyndicatefs_create((char const *) arg1, arg2, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg3, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_ftruncate
 * Signature: (Ljava/lang/String;JLJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1ftruncate(JNIEnv *jenv, jclass jcls, jstring jarg1, jlong jarg2, jobject jarg3) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    off_t arg2;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    arg2 = (off_t) jarg2;
    
    // read object - java to c++
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg3, &fi);
    if(result) return -1;

#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("newsize : %ld\n", arg2);
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else    
    result = (int) jsyndicatefs_ftruncate((char const *) arg1, arg2, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg3, &fi);
    if(result) return -1;
    
    return jresult;
}

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_fgetattr
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSStat;LJSyndicateFS/struct/JSFSFileInfo;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1fgetattr(JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2, jobject jarg3) {
    jint jresult = 0;
    char *arg1 = (char *) 0;
    struct stat statbuf;
    struct JSyndicateFS_FileInfo fi;
    int result;

    //(void) jenv;
    (void) jcls;
    arg1 = 0;
    if (jarg1) {
        arg1 = (char *) jenv->GetStringUTFChars(jarg1, 0);
        if (!arg1) return -1;
    }
    
    // read object - java to c++
    result = jsyndicatefs_init_stat(&statbuf);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSStat_to_Native(jenv, jarg2, &statbuf);
    if(result) return -1;
    
    result = jsyndicatefs_init_fileinfo(&fi);
    if(result) return -1;
    
    result = jsyndicatefs_copy_JSFSFileInfo_to_Native(jenv, jarg3, &fi);
    if(result) return -1;

#ifdef JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    printf("path : %s\n", arg1);
    
    printf("statbuf.st_dev = %ld\n", statbuf.st_dev);
    printf("statbuf.st_ino = %ld\n", statbuf.st_ino);
    printf("statbuf.st_mode = %d\n", statbuf.st_mode);
    printf("statbuf.st_nlink = %ld\n", statbuf.st_nlink);
    printf("statbuf.st_uid = %d\n", statbuf.st_uid);
    printf("statbuf.st_gid = %d\n", statbuf.st_gid);
    printf("statbuf.st_rdev = %ld\n", statbuf.st_rdev);
    printf("statbuf.st_size = %ld\n", statbuf.st_size);
    printf("statbuf.st_blksize = %ld\n", statbuf.st_blksize);
    printf("statbuf.st_blocks = %ld\n", statbuf.st_blocks);
    printf("statbuf.st_atim = %ld\n", statbuf.st_atim.tv_sec);
    printf("statbuf.st_mtim = %ld\n", statbuf.st_mtim.tv_sec);
    printf("statbuf.st_ctim = %ld\n", statbuf.st_ctim.tv_sec);
    
    statbuf.st_dev++;
    statbuf.st_ino++;
    statbuf.st_mode++;
    statbuf.st_nlink++;
    statbuf.st_uid++;
    statbuf.st_gid++;
    statbuf.st_rdev++;
    statbuf.st_size++;
    statbuf.st_blksize++;
    statbuf.st_blocks++;
    statbuf.st_atim.tv_sec++;
    statbuf.st_mtim.tv_sec++;
    statbuf.st_ctim.tv_sec++;
    
    printf("fi.direct_io = %d\n", fi.direct_io);
    printf("fi.flags = %d\n", fi.flags);
    printf("fi.fh = %p\n", fi.fh);
    
    fi.direct_io++;
    fi.flags++;
    fi.fh = (void*)(((long)fi.fh)+1);
    
    jresult = (jint) 0;
#else    
    result = (int) jsyndicatefs_fgetattr((char const *) arg1, &statbuf, &fi);
    jresult = (jint) result;
#endif
    
    if (arg1) jenv->ReleaseStringUTFChars(jarg1, (const char *) arg1);
    
    // return object - c++ to java
    result = jsyndicatefs_copy_Native_to_JSFSStat(jenv, jarg2, &statbuf);
    if(result) return -1;
    
    result = jsyndicatefs_copy_Native_to_JSFSFileInfo(jenv, jarg3, &fi);
    if(result) return -1;
    
    return jresult;
}
