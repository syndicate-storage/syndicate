/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
   
   Wathsala Vithanage (wathsala@princeton.edu)
*/

#include <proc-handler.h>
#include <libgateway.h>

#define BLK_SIZE (global_conf->blocking_factor)

//Self pipe to signal inotify_even_receiver's select() to retrun immediately...
int self_pipe[2];
set<proc_table_entry*, proc_table_entry_comp> running_proc_set;
map<pid_t, proc_table_entry*> pid_map;
//Lock for pid_map and running_proc_set
pthread_mutex_t pid_map_lock = PTHREAD_MUTEX_INITIALIZER;

void invalidate_entry(void* cls) {
    proc_table_entry *entry = (proc_table_entry*)cls;
    if (entry == NULL)
	return;
    //Acquire write lock on pte
    //pthread_mutex_lock(&entry->pte_lock);
    wrlock_pte(entry);
    if (!(entry->is_read_complete)) {
	if (kill(entry->proc_id, 9) < 0)
	    perror("kill(9)");
    }
    if (unlink(entry->block_file) < 0)
	perror("unlink(block_file)");
    entry->valid = false;
    clean_invalid_proc_entry(entry);
    //pthread_mutex_unlock(&entry->pte_lock);
    unlock_pte(entry);
}

void clean_invalid_proc_entry(proc_table_entry *pte) {
    if (pte) {
	if (pte->block_file)
	    free(pte->block_file);
	pte->block_file_wd = -1;
	pte->is_read_complete = false;
	pte->proc_id = -1;
	pte->current_offset = 0;
	//pte->block_byte_offset = 0;
    }
}

void delete_proc_entry(proc_table_entry *pte) {
    if (pte) {
	if (pte->block_file)
	    free(pte->block_file);
	//pthread_mutex_destroy(&pte->pte_lock);
	pthread_rwlock_destroy(&pte->pte_lock);
	free (pte);
	pte = NULL;
    }
}

void sigchld_handler(int signum) {
    pid_t pid = 0;
    while ((pid = waitpid(-1, NULL, WNOHANG)) > 0) {
	update_death(pid);
    }
}

int  set_sigchld_handler(struct sigaction *action) {
    int err = 0;
    err = install_signal_handler(SIGCHLD, action, sigchld_handler);
    return err;
}

void lock_pid_map() {
    //cout<<"Locking PID_MAP"<<endl;
    pthread_mutex_lock(&pid_map_lock);
}

void unlock_pid_map() {
    //cout<<"Unlocking PID_MAP"<<endl;
    pthread_mutex_unlock(&pid_map_lock);
}

void wrlock_pte(proc_table_entry *pte) {
    //cout<<"Locking PTE"<<endl;
    pthread_rwlock_wrlock(&pte->pte_lock);
}

void rdlock_pte(proc_table_entry *pte) {
    //cout<<"Locking PTE"<<endl;
    pthread_rwlock_rdlock(&pte->pte_lock);
}

void unlock_pte(proc_table_entry *pte) {
    //cout<<"Unlocking PTE"<<endl;
    pthread_rwlock_unlock(&pte->pte_lock);
}

void update_death(pid_t pid) {
    map<pid_t, proc_table_entry*>::iterator it;
    it = pid_map.find(pid);
    proc_table_entry *pte = NULL;
    if (it != pid_map.end()) {
	pte = it->second;
	//Lock pid_map lock
	lock_pid_map();
	//Acquire pte lock
	wrlock_pte(pte);
	pte->is_read_complete = true;
	set<proc_table_entry*, proc_table_entry_comp>::iterator sit;
	sit = running_proc_set.find(pte);
	if (sit != running_proc_set.end()) {
	    running_proc_set.erase(sit);
	}
	pid_map.erase(it);
	unlock_pte(pte);
	unlock_pid_map();
    }
    else {
	return;
    }
}

void* inotify_event_receiver(void *cls) {
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    block_all_signals();
    set_sigchld_handler(&act);
    //Initialize and start process monitoring thread using inotify...
    int ifd = inotify_init(); 
    if (ifd < 0)
	return NULL;
    //Select...
    fd_set read_fds;
    int max_fd = (ifd > self_pipe[0])?ifd:self_pipe[0];
    while(true) {
	FD_ZERO(&read_fds);
	FD_SET(ifd, &read_fds);
	FD_SET(self_pipe[0], &read_fds);
	if (select(max_fd + 1, &read_fds, NULL, NULL, NULL) < 0 ) {
	    if (errno == EINTR)
		continue;
	    else { 
		perror("select");
		exit(-1);
	    }
	}
	//Data is avaialble on an fd...
	if (FD_ISSET(self_pipe[0], &read_fds)) {
	    //We have a new file in file set, watch it in inotify
	    //Read the address of proc_table_entry and add it to the set
	    ulong pte_addr = 0;
	    if ((read(self_pipe[0], &pte_addr, sizeof(ulong)) > 0) && 
		    (pte_addr != 0)) {
		proc_table_entry *pte = (proc_table_entry*)(pte_addr);
		int wd = inotify_add_watch(ifd, pte->block_file, 
				IN_MODIFY | IN_CLOSE_WRITE);
		if (wd > 0) {
		    //Acquire the pte lock
		    wrlock_pte(pte);
		    pte->block_file_wd = wd;
		    //Lock pid_map lock
		    lock_pid_map();
		    running_proc_set.insert(pte);
		    pid_map[pte->proc_id] = pte;
		    unlock_pid_map();
		    unlock_pte(pte);
		}
		else {
		    perror("inotify_add_watch");
		}
	    }
	    else {
		perror("read on inotify self_pipe");
	    }
	}
	if (FD_ISSET(ifd, &read_fds)) {
	    //There are file that have changed since the last update.
	    char ievents[INOTIFY_READ_BUFFER_LEN];
            memset( ievents, 0, INOTIFY_READ_BUFFER_LEN );
	    //cout<<"inotify events available"<<endl;
	    ssize_t read_size = read(ifd, ievents, INOTIFY_READ_BUFFER_LEN);
	    if (read_size <= 0) {
		if (read_size < 0)
		    perror("inotify read");
		continue;
	    }
	    ssize_t byte_count = 0;
	    while (byte_count < read_size) {
		struct inotify_event *ievent = (struct inotify_event*)
						    (&ievents[byte_count]);
		proc_table_entry pte; 		
		pte.block_file_wd = ievent->wd;
		if (ievent->mask & IN_IGNORED) {
		    byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
		    continue;
		}
		if ((ievent->mask & (IN_MODIFY | IN_CLOSE_WRITE)) == 0) {
		    byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
		    continue;
		}
		lock_pid_map();
		set<proc_table_entry*, bool(*)(proc_table_entry*, 
						proc_table_entry*)>::iterator 
		    itr = running_proc_set.find(&pte);
		if (itr != running_proc_set.end()) {
		    //File found in set, update block info...
		    struct stat stat_buff;
		    proc_table_entry *pte = *itr;
		    if (stat(pte->block_file, &stat_buff) < 0 || !(pte->valid)) {
			perror("stat");
			if (inotify_rm_watch(ifd, ievent->wd) < 0)
			    perror("inotify_rm_watch");
			running_proc_set.erase(itr);
			clean_invalid_proc_entry(pte);
			unlock_pid_map();
			//Update the byte_count before we leave
			byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
			continue;
		    }
		    //Acquire the pte lock
		    wrlock_pte(pte);
		    //pte->current_max_block = stat_buff.st_size/BLK_SIZE;
		    //pte->block_byte_offset = stat_buff.st_size - 
		    //		    (pte->current_max_block * BLK_SIZE);
		    pte->current_offset = stat_buff.st_size;
		    unlock_pte(pte);
		}
		else {
		    //File not found in set, remove watch
		    if (inotify_rm_watch(ifd, ievent->wd) < 0)
			perror("inotify_rm_watch");
		    unlock_pid_map();
		    //Update the byte_count before leave
		    byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
		    continue;
		}
		//Update the byte_count before leave
		byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
		unlock_pid_map();
	    }

	}
    }
    return NULL;
}

bool ProcHandler::is_proc_alive(pid_t pid)
{
    if (kill(pid, 0) == 0)
	return true;
    else 
	return false;
}

ProcHandler::ProcHandler()
{
}

ProcHandler::ProcHandler(char *cache_dir_str) 
{
    int rc = 0;
    cache_dir_path = cache_dir_str;
    
    // ensure this path exists
    rc = md_mkdirs( cache_dir_path );
    if( rc != 0 ) {
       errorf("md_mkdirs(%s) rc = %d\n", cache_dir_str, rc );
    }
    
    pthread_mutex_init(&proc_table_lock, NULL);
    if (pipe(self_pipe) < 0) 
	perror("pipe");
    rc = pthread_create(&inotify_event_thread, NULL, inotify_event_receiver, 
		        NULL);
    if (rc < 0)
	perror("pthread_create");
}

ProcHandler&  ProcHandler::get_handle(char *cache_dir_str)
{
    static ProcHandler instance = ProcHandler(cache_dir_str);
    return instance;
}



int ProcHandler::execute_command(const char* proc_name, char *argv[], 
				char *envp[], struct shell_ctx *ctx, 
				proc_table_entry *pte)
{
    if (argv)
	argv[0] = (char*)proc_name;
    char *file_name = get_random_string();
    if (file_name == NULL)
	return -EIO;
    int file_path_len = strlen(cache_dir_path) + strlen(file_name);
    char *file_path = CALLOC_LIST( char, file_path_len + 2 );
    strcpy(file_path,  cache_dir_path);
    strcat(file_path, "/");
    strcat(file_path, file_name);
    free(file_name);
    int old_fd = dup(STDOUT_FILENO);

    int blk_log_fd = open(file_path, O_CREAT | O_RDWR, 
	    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); 
    if (blk_log_fd < 0) {
	cerr<<"Cannot open the file: "<<file_path<<endl;
	return -EIO;
    }

    if (dup2(blk_log_fd, STDOUT_FILENO) < 0) {
	perror("dup2");
	exit(-1);
    }
    pid_t pid = fork();
    if (pid == 0) {
	//Child
	int rc = execve(proc_name, argv, envp);
	if (rc < 0) {
	    perror("exec");
	    exit(-1);
	}
    }
    else if (pid > 0) {
	//Parent
	if (dup2(old_fd, STDOUT_FILENO) < 0) {
	    perror("dup2");
	}
	pte->block_file = file_path;
	pte->block_file_wd = -1;
	//pte->current_max_block = 0;
	pte->proc_id = pid;
	pte->is_read_complete = false;
	pte->valid = true;
	//proc_table[id] = pte;
	//Lock proc_table
	lock_proc_table();
	proc_table[string(ctx->file_path)] = pte;
	//Unlock proc_table
	unlock_proc_table();
	ulong pte_addr = (ulong)pte;
	if (write(self_pipe[1], &pte_addr, sizeof(ulong)) < 0) {
	    perror("write");
	    return -EIO;
	}
	//Read block_sized amount of data...
	close(blk_log_fd);
	return 0;
    }
    else {
	return -EIO;
    }
    return 0;
}

int ProcHandler::execute_command(struct shell_ctx *ctx, 
				char *buffer, ssize_t read_size) 
{
    char const *proc_name = ctx->proc_name;
    char **argv  = ctx->argv;
    char **envp = ctx->envp;
    ssize_t len = 0;
    struct stat st_buf;

    proc_table_entry *pte = proc_table[string(ctx->file_path)];
    if (pte == NULL || (pte != NULL && !(pte->valid))) {
	if (pte == NULL)
	    pte = alloc_proc_table_entry();
	//Lock pte
	wrlock_pte(pte);
	int rc = execute_command(proc_name, argv, 
				envp, ctx, pte);
	if (rc < 0) {
	    unlock_pte(pte);
	    return rc;
	}
	//pte = proc_table[ctx->id];
	pte = proc_table[string(ctx->file_path)];
	//check whether this pte is valid
	if (!pte->valid) {
	    unlock_pte(pte);
	    return -EAGAIN;
	}
	ctx->mi->entry = pte;
	ctx->mi->invalidate_entry = invalidate_entry;
	int fd = open(pte->block_file, O_RDONLY);
	if (fd < 0) {
	    perror("open");
	    unlock_pte(pte);
	    return -EIO;
	}
	ctx->fd = fd;

	if (fstat(fd, &st_buf) < 0) {
	    perror("stat");
	    unlock_pte(pte);
	    return -EIO;
	}
	if (st_buf.st_size < (off_t)(ctx->block_id * ctx->blocking_factor)) {
	    unlock_pte(pte);
	    if (!pte->is_read_complete)
		return -EAGAIN;
	    else
		return 0;
	}
	off_t seek_len = ctx->block_id  * ctx->blocking_factor;
	if (lseek(fd, seek_len, SEEK_SET) < 0) {
	    perror("lseek");
	    unlock_pte(pte);
	    return -EIO;
	}
	uint bk_off_count = 0, bk_off = 0;
	while ((len = read(fd, buffer, read_size)) == 0 &&
		bk_off <= MAX_BACK_OFF_TIME && read_size != 0) {
	    bk_off = pow(2, bk_off_count);
	    sleep(bk_off);
	    bk_off_count++;
	}
	ctx->data_offset += len;
	unlock_pte(pte);
	return len;
    }
    else {
	//If data_offset is equal to block_size then we have read an 
	//entire block. Therefore return 0.
	if (ctx->data_offset >= (ssize_t)ctx->blocking_factor)
	    return 0;
	//get proc_table_entry by id and return the block... 
	//proc_table_entry *pte = proc_table[ctx->id];
	//pte is only read here.
	rdlock_pte(pte);
	//check whether this pte is valid
	if (!pte->valid) {
	    unlock_pte(pte);
	    return -EAGAIN;
	}
	if (pte->current_offset > (ctx->block_id * ctx->blocking_factor) + 
				    ctx->data_offset) {
	    off_t current_read_offset = (ctx->block_id * ctx->blocking_factor) + 
					ctx->data_offset;
	    if (ctx->fd < 0) {
		int fd = open(pte->block_file, O_RDONLY);
		if (fd < 0) {
		    perror("open");
		    unlock_pte(pte);
		    return -EIO;
		}
		ctx->fd = fd;
	    }
	    /*if (((pte->current_max_block == ctx->block_id) && 
		    pte->block_byte_offset > ctx->data_offset) ||
		    pte->current_max_block > ctx->block_id) {*/
	    if (lseek(ctx->fd, current_read_offset, SEEK_SET) < 0) {
		perror("lseek");
		unlock_pte(pte);
		return -EIO;
	    }
	    len = read(ctx->fd, buffer, read_size);
	    if (len < 0) {
		perror("read");
		unlock_pte(pte);
		return -EIO;
	    }
	    else {
		ctx->data_offset += len;
		unlock_pte(pte);
		return len;
	    }
	    /*} 
	      else {
	      unlock_pte(pte);
	      return -EAGAIN;
	    }*/
	}
	else { 
	    unlock_pte(pte);
	    if (pte->is_read_complete) {
		return 0;
	    }
	    else
		return -EAGAIN;
	}
	//unlock_pte(pte);
    }
}

ssize_t ProcHandler::encode_results()
{
    return -ENOSYS;
}

proc_table_entry* ProcHandler::alloc_proc_table_entry()
{
    proc_table_entry *pte = CALLOC_LIST( proc_table_entry, 1 );
    pthread_rwlock_init(&pte->pte_lock, NULL);
    return pte;
}

char* ProcHandler::get_random_string()
{
    const uint nr_chars = MAX_FILE_NAME_LEN;
    char* rand_str = CALLOC_LIST( char, nr_chars + 1 );
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0) {
	free(rand_str);
	return NULL;
    }
    srand(ts.tv_sec + ts.tv_nsec);
    uint count = 0;
    while (count < nr_chars) {
	int r = ( rand()%74 ) + 48;
	if ( (r >= 58 && r <= 64) || 
		(r >= 91 && r <= 96) )
	    continue;
	rand_str[count] = (char)r;
	count++;
    }
    return rand_str;
}

pthread_t ProcHandler::get_thread_id() 
{
    return inotify_event_thread;
} 

block_status ProcHandler::get_block_status(struct shell_ctx *ctx) 
{
    block_status blk_stat;
    blk_stat.in_progress = false;
    blk_stat.no_block = false;
    blk_stat.no_file = false;
    blk_stat.block_available = false;

    /*if (ctx->id >= DEFAULT_INIT_PROC_TBL_LEN) {
	blk_stat.no_block = true;
    }*/
    //proc_table_entry *pte = proc_table[ctx->id];
    proc_table_entry *pte = proc_table[string(ctx->file_path)];
    if (pte == NULL && ctx->file_path == NULL) {
	blk_stat.no_file = true;
    }
    else if (pte == NULL && ctx->file_path != NULL) {
	blk_stat.in_progress = true;
    }
    else {
	if (!(pte->valid)) {
	    blk_stat.in_progress = true;
	}
	else if (((ctx->block_id + 1) * ctx->blocking_factor) > pte->current_offset) {
	    if (pte->is_read_complete) {
		blk_stat.no_block = true;
	    }
	    else {
		blk_stat.in_progress = true;
	    }
	}
	else if (((ctx->block_id + 1) * ctx->blocking_factor) == pte->current_offset) {
	    if (pte->is_read_complete) {
		blk_stat.block_available = true;
	    }
	    else {
		blk_stat.in_progress = true;
	    }
	}
	else {
		blk_stat.block_available = true;
	}
    }
    return blk_stat;
}

void ProcHandler::remove_proc_table_entry(string file_path) {
    //Lock proc_table
    lock_proc_table();
    proc_table.erase(file_path);
    unlock_proc_table();
}

/*
void ProcHandler::operator=(ProcHandler const& x) 
{
    if (init)
	x = odh;

}*/

