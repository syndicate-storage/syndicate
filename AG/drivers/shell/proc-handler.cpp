#include <proc-handler.h>
#include <libgateway.h>

#define BLK_SIZE (global_conf->blocking_factor)

//Self pipe to singla inotify_even_receiver's select() to retrun immediately...
int self_pipe[2];
set<proc_table_entry*, proc_table_entry_comp> running_proc_set;

void* inotify_event_receiver(void *cls) {
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
	if ( select(max_fd + 1, &read_fds, NULL, NULL, NULL) < 0 )
	    break;
	//Data is avaialble on an fd...
	if (FD_ISSET(self_pipe[0], &read_fds)) {
	    //We have a new file in file set, watch it in inotify
	    //Read the address of proc_table_entry and add it to the set
	    ulong pte_addr = 0;
	    if ((read(self_pipe[0], &pte_addr, sizeof(ulong)) > 0) && 
		    (pte_addr != 0)) {
		proc_table_entry *pte = (proc_table_entry*)(pte_addr);
		int wd = inotify_add_watch(ifd, pte->block_file, IN_MODIFY | IN_ATTRIB | IN_ACCESS);
		if (wd > 0) {
		    pte->block_file_wd = wd;
		    running_proc_set.insert(pte);
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
	    //cout<<"inotify events available"<<endl;
	    ssize_t read_size = read(ifd, ievents, INOTIFY_READ_BUFFER_LEN);
	    if (read_size <= 0) {
		if (read_size < 0)
		    perror("inotify read");
		continue;
	    }
	    ssize_t byte_count = 0;
	    while (byte_count < read_size) {
		struct inotify_event *ievent = (struct inotify_event*)(&ievents[byte_count]);
		proc_table_entry pte; 		
		pte.block_file_wd = ievent->wd;
		set<proc_table_entry*, bool(*)(proc_table_entry*, proc_table_entry*)>::iterator 
		    itr = running_proc_set.find(&pte);
		if (itr != running_proc_set.end()) {
		    //File found in set, update block info...
		    struct stat stat_buff;
		    proc_table_entry *pte = *itr;
		    if (stat(pte->block_file, &stat_buff) < 0) {
			perror("stat");
			if (inotify_rm_watch(ifd, ievent->wd) < 0)
			    perror("inotify_rm_watch");
			running_proc_set.erase(itr);
			break;
		    }
		    pte->current_max_block = stat_buff.st_size/BLK_SIZE;
		    pte->block_byte_offset = stat_buff.st_size - 
				    (pte->current_max_block * BLK_SIZE);
		    if (!ProcHandler::is_proc_alive(pte->proc_id)) {
			pte->is_read_complete = true;
			if (inotify_rm_watch(ifd, ievent->wd) < 0)
			    perror("inotify_rm_watch");
			running_proc_set.erase(itr);
			break;
		    }
		    byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
		    //cout<<">> max block: "<<pte->current_max_block<<endl;
		    //cout<<">> byte offset: "<<pte->block_byte_offset<<endl;
		}
		else {
		    //File not found in set, remove warch
		    if (inotify_rm_watch(ifd, ievent->wd) < 0)
			perror("inotify_rm_watch");
		    break;
		}
	    }
	}
    }
    perror("select");
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
    //running_proc_set = new 
	//set<proc_table_entry*, proc_table_entry_comp>();
    cache_dir_path = cache_dir_str;
    proc_table.resize(DEFAULT_INIT_PROC_TBL_LEN);
    if (pipe(self_pipe) < 0) 
	perror("pipe");
    int rc = pthread_create(&inotify_event_thread, NULL, inotify_event_receiver, NULL/*running_proc_set*/);
    if (rc < 0)
	perror("pthreda_create");
}

ProcHandler&  ProcHandler::get_handle(char *cache_dir_str)
{
    static ProcHandler instance = ProcHandler(cache_dir_str);
    return instance;
}



int ProcHandler::execute_command(const char* proc_name, char *argv[], char *envp[], ssize_t block_size, uint id)
{
    if (argv)
	argv[0] = (char*)proc_name;
    char *file_name = get_random_string();
    if (file_name == NULL)
	return -EIO;
    int file_path_len = strlen(cache_dir_path) + strlen(file_name);
    char *file_path = (char*)malloc(file_path_len + 2); 
    memset(file_path, 0, file_path_len + 2);
    strcpy(file_path,  cache_dir_path);
    strcat(file_path, "/");
    strcat(file_path, file_name);
    free(file_name);
    int old_fd = dup(STDOUT_FILENO);

    //cout<<"file://"<<file_path<<endl;
    int blk_log_fd = open(file_path, O_CREAT | O_RDWR, 
	    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); 
    if (blk_log_fd < 0) {
	cout<<"Cannot open the file: "<<file_path<<endl;
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
	proc_table_entry *pte = alloc_proc_table_entry();
	pte->block_file = file_path;
	pte->block_file_wd = -1;
	pte->current_max_block = 0;
	pte->proc_id = pid;
	proc_table[id] = pte;
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

int ProcHandler::execute_command(struct gateway_ctx *ctx, 
				char *buffer, ssize_t read_size, 
				ssize_t block_size) 
{
    char const *proc_name = ctx->proc_name;
    char **argv  = ctx->argv;
    char **envp = ctx->envp;
    ssize_t len = 0;
    struct stat st_buf;

    if (ctx->id >= DEFAULT_INIT_PROC_TBL_LEN)
	return -EIO;
    proc_table_entry *pte = proc_table[ctx->id];
    if (pte == NULL) {
	int rc = execute_command(proc_name, argv, 
				envp, block_size, ctx->id);
	if (rc < 0)
	    return rc;
	pte = proc_table[ctx->id];
	int fd = open(pte->block_file, O_RDONLY);
	if (fd < 0) {
	    perror("open");
	    return -EIO;
	}
	ctx->fd = fd;

	if (fstat(fd, &st_buf) < 0) {
	    perror("stat");
	    return -EIO;
	}
	if (st_buf.st_size < (off_t)(ctx->block_id * BLK_SIZE)) {
	    if (!pte->is_read_complete)
		return -EAGAIN;
	    else
		return 0;
	}
	off_t seek_len = ctx->block_id  * BLK_SIZE;
	if (lseek(fd, seek_len, SEEK_SET) < 0) {
	    perror("lseek");
	    return -EIO;
	}
	uint bk_off_count = 0, bk_off = 0;;
	while ((len = read(fd, buffer, read_size)) == 0 &&
		bk_off <= MAX_BACK_OFF_TIME) {
	    bk_off = pow(2, bk_off_count);
	    sleep(bk_off);
	    bk_off_count++;
	}
	ctx->data_offset += len;
	return len;
    }
    else {
	//If data_offset is equal to block_size then we have read an 
	//entire block. Therefore return 0.
	if (ctx->data_offset >= block_size)
	    return 0;
	//get proc_table_entry by id and return the block... 
	proc_table_entry *pte = proc_table[ctx->id];
	if (pte->current_max_block >= ctx->block_id) {
	    off_t current_offset = (ctx->block_id * block_size) + 
			    ctx->data_offset;
	    if (ctx->fd < 0) {
		int fd = open(pte->block_file, O_RDONLY);
		if (fd < 0) {
		    perror("open");
		    return -EIO;
		}
		ctx->fd = fd;
	    }
	    if (((pte->current_max_block == ctx->block_id) && 
		    pte->block_byte_offset > ctx->data_offset) ||
		    pte->current_max_block > ctx->block_id) {
		if (lseek(ctx->fd, current_offset, SEEK_SET) < 0) {
		    perror("lseek");
		    return -EIO;
		}
		len = read(ctx->fd, buffer, read_size);
		if (len < 0) {
		    perror("read");
		    return -EIO;
		}
		else {
		    ctx->data_offset += len;
		    return len;
		}
	    } 
	    else {
		return -EAGAIN;
	    }
	}
	else { 
	    if (pte->is_read_complete) {
		return 0;
	    }
	    else
		return -EAGAIN;
	}
    }
}

ssize_t ProcHandler::encode_results()
{
    return -ENOSYS;
}

proc_table_entry* ProcHandler::alloc_proc_table_entry()
{
    proc_table_entry *pte = 
		(proc_table_entry*)malloc(sizeof(proc_table_entry));
    return pte;
}

char* ProcHandler::get_random_string()
{
    const uint nr_chars = MAX_FILE_NAME_LEN;
    char* rand_str = (char*)malloc(nr_chars + 1);
    memset(rand_str, 0, nr_chars);
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

block_status ProcHandler::get_block_status(struct gateway_ctx *ctx) 
{
    block_status blk_stat;
    blk_stat.in_progress = false;
    blk_stat.no_block = false;
    blk_stat.no_file = false;
    blk_stat.block_available = false;

    if (ctx->id >= DEFAULT_INIT_PROC_TBL_LEN) {
	blk_stat.no_block = true;
    }
    proc_table_entry *pte = proc_table[ctx->id];
    if (pte == NULL) {
	blk_stat.no_file = true;
    }
    else {
	if (ctx->block_id > pte->current_max_block) {
	    if (pte->is_read_complete) {
		blk_stat.no_block = true;
	    }
	    else {
		blk_stat.in_progress = true;
	    }
	}
	else if (ctx->block_id == pte->current_max_block) {
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
/*
void ProcHandler::operator=(ProcHandler const& x) 
{
    if (init)
	x = odh;

}*/

