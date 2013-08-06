#include <AG-util.h>

//Global sighand set
struct _sighand_struct sghs;

void clean_dir(const char *dirname) {
    if (dirname == NULL)
	return;
    DIR *dirp = opendir(dirname);
    if (dirp == NULL) {
	perror("opendir");
	return;
    }
    struct dirent *dentry;
    struct dirent *dentry_dp = NULL;
    ssize_t len = offsetof(struct dirent, d_name) +
				pathconf(dirname, _PC_NAME_MAX) + 1;
    dentry = (struct dirent*)malloc(len);
    while (readdir_r(dirp, dentry, &dentry_dp) ==  0 && 
	    dentry_dp != NULL) {
	if (strcmp(dentry->d_name, ".") && 
		strcmp(dentry->d_name, "..")) {
		ssize_t path_len = strlen(dirname) + strlen(dentry->d_name) + 2;
		char *path = (char*)malloc(path_len);
		memset(path, 0, path_len);
		strcpy(path, dirname);
		strcat(path, "/");
		strcat(path, dentry->d_name);
	    if (dentry->d_type == DT_DIR) {
		clean_dir(path);
		if (rmdir(path) < 0)
		    perror("unlink - dir");
	    }
	    else {
		if (unlink(path) < 0) 
		    perror("unlink - file");
	    }
	    free(path);
	    path = NULL;
	}
    }    
    closedir(dirp);
    return;
}


//Register signal handler with sighands
sighandler_t add_signal_handler(int signum, sighandler_t hndl) {
    sighandler_t rc = 0;
    switch (signum) {
	case SIGTERM:
	    sghs.term_handler = hndl;
	    rc = signal(signum, hndl);
	    break;
	case SIGINT:
	    sghs.init_handler = hndl;
	    rc = signal(signum, hndl);
	    break;
    }	
    return rc;
}

//Remove signal handler sighands
sighandler_t remove_signal_handler(int signum) {
    sighandler_t rc = 0;
    switch (signum) {
	case SIGTERM:
	    sghs.term_handler = NULL;
	    rc = signal(signum, NULL);
	    break;
	case SIGINT:
	    sghs.init_handler = NULL;
	    rc = signal(signum, NULL);
	    break;
    }	
    return rc;
}

