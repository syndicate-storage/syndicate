/*
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
#include <libgateway.h>

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


void add_driver_event_handler(int event, driver_event_handler deh,
			      void *args) {
    if (deh == NULL)
	return;
    switch(event) {
	case DRIVER_TERMINATE:
	    de.deh[DRIVER_TERMINATE] = deh;
	    de.deh_arg[DRIVER_TERMINATE] = args;
	    break;
	case DRIVER_RECONF:
	    de.deh[DRIVER_RECONF] = deh;
	    de.deh_arg[DRIVER_RECONF] = args;
	    break;
	default:
	    break;
    }
}

void remove_driver_event_handler(int event) {
    switch(event) {
	case DRIVER_TERMINATE:
	    de.deh[DRIVER_TERMINATE] = NULL;
	    break;
	case DRIVER_RECONF:
	    de.deh[DRIVER_RECONF] = NULL;
	    break;
	default:
	    break;
    }
}

void* driver_event_loop(void *) {
    block_all_signals();
    fd_set read_fds;
    int rc = 0;
    char* cmd = (char*)malloc(DRIVER_CMD_LEN);
    memset(cmd, 0, DRIVER_CMD_LEN);
    int read_count = 0;
    while(true) {
	FD_ZERO(&read_fds);
	FD_SET(de.fifo_fd, &read_fds);
	rc = 0;
	read_count = 0;
	memset(cmd, 0, DRIVER_CMD_LEN);
	if (select(de.fifo_fd + 1, &read_fds, NULL, NULL, NULL) < 0 ) {
	    if (errno == EINTR)
		continue;
	    else {
		perror("select");
	    }
	}
	if (FD_ISSET(de.fifo_fd, &read_fds)) {
	    rc = read(de.fifo_fd, cmd + read_count, DRIVER_CMD_LEN);
	    if (rc > 0) {
		read_count += rc;
		if (read_count == DRIVER_CMD_LEN) {
		    //This could be valid command take an action.
		    handle_command(cmd);
		}
	    }
	    else {
		if (errno == EAGAIN || rc == 0)
		    break;
	    }
	}
    }
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
    de.fifo_fd = open(fifo_path, O_RDWR | O_NONBLOCK);
    if (de.fifo_fd < 0) {
	perror("open");
	return;
    }
    rc = pthread_create(&de.tid, NULL, driver_event_loop, NULL);
    if (rc < 0)
	perror("pthread_create");
}

void* handle_command (char *cmd) {
    if (cmd == NULL)
	return NULL;
    if (strncmp(cmd, DRIVER_TERMINATE_STR, DRIVER_CMD_LEN) == 0) {
	if (de.deh[DRIVER_TERMINATE] != NULL) {
	    //Close the fifo and delte the fifo.
	    close(de.fifo_fd);
	    pid_t pid = getpid();
	    stringstream sstr;
	    sstr<<FIFO_PREFIX<<pid;
	    char* fifo_path = strdup(sstr.str().c_str());
	    unlink(fifo_path);
	    return de.deh[DRIVER_TERMINATE](de.deh_arg[DRIVER_TERMINATE]);
	}
    }
    else if (strncmp(cmd, DRIVER_RECONF_STR, DRIVER_CMD_LEN) == 0) {
	if (de.deh[DRIVER_RECONF] != NULL)
	    return de.deh[DRIVER_RECONF](de.deh_arg[DRIVER_RECONF]);
    }
    else {
	return NULL;
    }
    return NULL;
}

int controller_signal_handler(pid_t pid, int flags) {
    //Make sure to handle STOP_CTRL_FLAG after handling all the signal.
    int fifo_fd = 0;
    stringstream sstr;
    sstr<<FIFO_PREFIX<<pid;
    char* fifo_path = strdup(sstr.str().c_str());
    int rc = mkfifo(fifo_path, S_IRUSR | S_IWUSR | S_IRGRP);
    if (rc < 0) {
	if (errno != EEXIST) {
	    perror("mkfifo");
	    return -1;
	}
    }
    fifo_fd = open(fifo_path, O_RDWR | O_NONBLOCK);
    if (fifo_fd < 0) {
	perror("open");
	return -1;
    }

    if ((flags & RMAP_CTRL_FLAG) == RMAP_CTRL_FLAG) {
	if (write(fifo_fd, "RCON", 4) < 0) {
	    perror("write");
	    return -1;
	}
    }
    if ((flags & STOP_CTRL_FLAG) == STOP_CTRL_FLAG) {
	if (write(fifo_fd, "TERM", 4) < 0) {
	    perror("write");
	    return -1;
	}
    }
    return  0;
}
