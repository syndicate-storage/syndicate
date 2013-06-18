/* 
 * File:   JSyndicateFSJni.h
 * Author: iychoi
 *
 * Created on June 10, 2013, 10:57 AM
 */

#ifndef JSYNDICATEFSJNI_H
#define	JSYNDICATEFSJNI_H

#include "JSyndicateFS.h"

#include <jni.h>
#include <stdlib.h>
#include <string.h>

#ifdef	__cplusplus
extern "C" {
#endif

/*
 *  make this macro defined if you want to test jni argument passing
 */
//#define JSYNDICATEFS_JNI_ARGUMENT_PASSING_TEST
    
/* Support for throwing Java exceptions */
typedef enum {
    JavaOutOfMemoryError = 1,
    JavaIOException,
    JavaRuntimeException,
    JavaIndexOutOfBoundsException,
    JavaArithmeticException,
    JavaIllegalArgumentException,
    JavaNullPointerException,
    JavaDirectorPureVirtual,
    JavaUnknownError
} JavaExceptionCodes;
    
struct JavaExceptions_t {
  JavaExceptionCodes code;
  const char *java_exception;
};


static void JavaThrowException(JNIEnv *jenv, JavaExceptionCodes code, const char *msg);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_init
 * Signature: (LJSyndicateFS/struct/JSFSConfig;)I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1init
  (JNIEnv *jenv, jclass jcls, jobject jarg1);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_destroy
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1destroy
  (JNIEnv *jenv, jclass jcls);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_getattr
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSStat;)I
 * 
 * Get file attributes (lstat)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1getattr
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_mknod
 * Signature: (Ljava/lang/String;IJ)I
 * 
 * Create a file node with open(), mkfifo(), or mknod(), depending on the mode.
 * Right now, only normal files are supported.
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1mknod
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jlong jarg3);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_mkdir
 * Signature: (Ljava/lang/String;I)I
 * 
 * Create a directory (mkdir)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1mkdir
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_unlink
 * Signature: (Ljava/lang/String;)I
 * 
 * Remove a file (unlink)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1unlink
  (JNIEnv *jenv, jclass jcls, jstring jarg1);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_rmdir
 * Signature: (Ljava/lang/String;)I
 * 
 * Remove a directory (rmdir)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1rmdir
  (JNIEnv *jenv, jclass jcls, jstring jarg1);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_rename
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 * 
 * Rename a file.  Paths are FS-relative! (rename)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1rename
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_chmod
 * Signature: (Ljava/lang/String;I)I
 * 
 * Change the permission bits of a file (chmod)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1chmod
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_truncate
 * Signature: (Ljava/lang/String;J)I
 * 
 * Change the size of a file (truncate)
 * only works on local files
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1truncate
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jlong jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_utime
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSUtimbuf;)I
 * 
 * Change the access and/or modification times of a file (utime)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1utime
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_open
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * File open operation (O_CREAT and O_EXCL will *not* be passed to this method, according to the documentation)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1open
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_read
 * Signature: (Ljava/lang/String;[BJJLJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Read data from an open file.  Return number of bytes read.
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1read
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jbyteArray jarg2, jlong jarg3, jlong jarg4, jobject jarg5);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_write
 * Signature: (Ljava/lang/String;[BJJLJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Write data to an open file (pwrite)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1write
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jbyteArray jarg2, jlong jarg3, jlong jarg4, jobject jarg5);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_statfs
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSStatvfs;)I
 * 
 * Get file system statistics
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1statfs
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_flush
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Possibly flush cached data (No-op)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1flush
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_release
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Release an open file (close)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1release
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_fsync
 * Signature: (Ljava/lang/String;ILJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Synchronize file contents (fdatasync, fsync)
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1fsync
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jobject jarg3);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_setxattr
 * Signature: (Ljava/lang/String;Ljava/lang/String;[BJI)I
 * 
 * Set extended attributes (lsetxattr)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1setxattr
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2, jbyteArray jarg3, jlong jarg4, jint jarg5);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_getxattr
 * Signature: (Ljava/lang/String;Ljava/lang/String;[BJ)I
 * 
 * Get extended attributes (lgetxattr)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1getxattr
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2, jbyteArray jarg3, jlong jarg4);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_listxattr
 * Signature: (Ljava/lang/String;[BJ)I
 * 
 * List extended attributes (llistxattr)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1listxattr
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jbyteArray jarg2, jlong jarg3);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_removexattr
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 * 
 * Remove extended attributes (lremovexattr)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1removexattr
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jstring jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_opendir
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Open directory (opendir)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1opendir
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_readdir
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFillDir;JLJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Read directory (readdir)
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1readdir
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2, jlong jarg3, jobject jarg4);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_releasedir
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Release directory (closedir)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1releasedir
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_fsyncdir
 * Signature: (Ljava/lang/String;ILJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Synchronize directory contents (no-op)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1fsyncdir
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jobject jarg3);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_access
 * Signature: (Ljava/lang/String;I)I
 * 
 * Check file access permissions (access)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1access
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_create
 * Signature: (Ljava/lang/String;ILJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Create and open a file (creat)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1create
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jint jarg2, jobject jarg3);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_ftruncate
 * Signature: (Ljava/lang/String;JLJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Change the size of an file (ftruncate)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1ftruncate
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jlong jarg2, jobject jarg3);

/*
 * Class:     JSyndicateFSJNI_JSyndicateFSJNI
 * Method:    jsyndicatefs_fgetattr
 * Signature: (Ljava/lang/String;LJSyndicateFS/struct/JSFSStat;LJSyndicateFS/struct/JSFSFileInfo;)I
 * 
 * Get attributes from an open file (fstat)
 */
JNIEXPORT jint JNICALL Java_JSyndicateFSJNI_JSyndicateFSJNI_jsyndicatefs_1fgetattr
  (JNIEnv *jenv, jclass jcls, jstring jarg1, jobject jarg2, jobject jarg3);


#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFSJNI_H */

