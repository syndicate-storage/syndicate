#include <AG-util.h>

struct _driver_events de;

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


void add_driver_event_handler(int event, driver_event_handler deh) {
    if (deh == NULL)
	return;
    switch(event) {
	case DRIVER_TERMINATE:
	    de.term_deh = deh;
	    break;
	case DRIVER_RECONF:
	    de.reconf_deh = deh;
	    break;
	default:
	    break;
    }
}

void remove_driver_event_handler(int event) {
    switch(event) {
	case DRIVER_TERMINATE:
	    de.term_deh = NULL;
	    break;
	case DRIVER_RECONF:
	    de.reconf_deh = NULL;
	    break;
	default:
	    break;
    }
}

void* driver_event_loop(void *) {
    block_all_singals();
    return NULL;
}

void driver_event_start() {
    //Create fifo and update de.fifo_fd.
    pid_t pid = getpid();
    stringstream sstr;
    sstr<<FIFO_PREFIX<<pid;
    char* fifo_path = strdup(sstr.str().c_str());
    int rc = mkfifo(fifo_path, S_IRUSR | S_IWUSR | S_IRGRP);
    if (rc < 0) {
	perror("mkfifo");
	return;
    }
    //de.fifo_fd = 
    rc = pthread_create(NULL, driver_event_loop, NULL);
    if (rc < 0)
	perror("pthread_create");
}

