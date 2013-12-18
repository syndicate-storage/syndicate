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
proc_table_t proc_table;
int INOTIFY_FD = -1;

//Lock for the above PID metadata
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
	if (pte->block_file) {
	    free(pte->block_file);
            pte->block_file = NULL;
        }
	pte->block_file_wd = -1;
	pte->is_read_complete = false;
	pte->proc_id = -1;
	pte->written_so_far = 0;
	//pte->block_byte_offset = 0;
    }
}

void delete_proc_entry(proc_table_entry *pte) {
    if (pte) {
	if (pte->block_file)
	    free(pte->block_file);
	//pthread_mutex_destroy(&pte->pte_lock);
	pthread_rwlock_destroy(&pte->pte_lock);
        
        memset(pte, 0, sizeof(proc_table_entry) );
	free (pte);
	pte = NULL;
    }
}

void sigchld_handler(int signum) {
    pid_t pid = 0;
    while ((pid = waitpid(-1, NULL, WNOHANG)) > 0) {
        dbprintf("Received SIGCHLD for %d\n", pid);
	update_death(pid);
    }
}

int  set_sigchld_handler(struct sigaction *action) {
    int err = 0;
    dbprintf("Set SIGCHLD handler %p\n", sigchld_handler);
    err = install_signal_handler(SIGCHLD, action, sigchld_handler);
    return err;
}

void lock_pid_map() {
    //dbprintf("%s\n", "locking PID map");
    pthread_mutex_lock(&pid_map_lock);
}

void unlock_pid_map() {
    //dbprintf("%s\n", "Unlocking PID map");
    pthread_mutex_unlock(&pid_map_lock);
}

void wrlock_pte(proc_table_entry *pte) {
    //dbprintf("WrLock PTE %p\n", pte);
    pthread_rwlock_wrlock(&pte->pte_lock);
}

void rdlock_pte(proc_table_entry *pte) {
    //dbprintf("RdLock PTE %p\n", pte);
    pthread_rwlock_rdlock(&pte->pte_lock);
}

void unlock_pte(proc_table_entry *pte) {
    //dbprintf("UnLock PTE %p\n", pte);
    pthread_rwlock_unlock(&pte->pte_lock);
}

void update_death(pid_t pid) {
    map<pid_t, proc_table_entry*>::iterator it;
    
    lock_pid_map();
    
    it = pid_map.find(pid);
    proc_table_entry *pte = NULL;
    if (it != pid_map.end()) {
        dbprintf("finalize %d\n", pid);
	pte = it->second;
	//Lock pid_map lock
	//Acquire pte lock
	wrlock_pte(pte);
	pte->is_read_complete = true;
	set<proc_table_entry*, proc_table_entry_comp>::iterator sit;
	pid_map.erase(it);
	unlock_pte(pte);
    }
    
    unlock_pid_map();
}


int init_event_receiver(void) {
   
    //Initialize and start process monitoring thread using inotify...
    INOTIFY_FD = inotify_init(); 
    if (INOTIFY_FD < 0)
        return -errno;
    
    return 0;
}

void* inotify_event_receiver(void *cls) {
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    block_all_signals();
    set_sigchld_handler(&act);
    
    //Select...
    fd_set read_fds;
    int max_fd = INOTIFY_FD;
    while(true) {
	FD_ZERO(&read_fds);
	FD_SET(INOTIFY_FD, &read_fds);
        
	if (select(max_fd + 1, &read_fds, NULL, NULL, NULL) < 0 ) {
	    if (errno == EINTR)
		continue;
	    else { 
		perror("select");
		exit(-1);
	    }
	}
	
	if (FD_ISSET(INOTIFY_FD, &read_fds)) {
	    //There are file that have changed since the last update.
	    char ievents[INOTIFY_READ_BUFFER_LEN];
            memset( ievents, 0, INOTIFY_READ_BUFFER_LEN );
	    //cout<<"inotify events available"<<endl;
	    ssize_t read_size = read(INOTIFY_FD, ievents, INOTIFY_READ_BUFFER_LEN);
	    if (read_size <= 0) {
		if (read_size < 0)
		    perror("inotify read");
		continue;
	    }
	    ssize_t byte_count = 0;
	    while (byte_count < read_size) {
		struct inotify_event *ievent = (struct inotify_event*)(&ievents[byte_count]);
                
		proc_table_entry pte_by_wd;
                memset( &pte_by_wd, 0, sizeof(pte_by_wd) );
		pte_by_wd.block_file_wd = ievent->wd;
                
		if (ievent->mask & IN_IGNORED) {
		    byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
		    continue;
		}
		if ((ievent->mask & (IN_MODIFY | IN_CLOSE_WRITE)) == 0) {
		    byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
		    continue;
		}
		
		
		lock_pid_map();
                
		set<proc_table_entry*, bool(*)(proc_table_entry*, proc_table_entry*)>::iterator itr = running_proc_set.find(&pte_by_wd);
		if (itr != running_proc_set.end()) {
		    //File found in set, update block info...
		    struct stat stat_buff;
		    proc_table_entry *pte = *itr;
                    
                    wrlock_pte(pte);
                    
		    if (stat(pte->block_file, &stat_buff) < 0 || !(pte->valid)) {
                        int errsv = -errno;
                        
                        if( pte->valid )
                           errorf("stat(%s) errno %d\n", pte->block_file, errsv);
                        
			if (inotify_rm_watch(INOTIFY_FD, ievent->wd) < 0) {
                            int errsv = -errno;
			    errorf("inotify_rm_watch errno %d\n", errsv);
                        }
                        
			running_proc_set.erase(itr);
			clean_invalid_proc_entry(pte);
		    }
		    else {
                        // otherwise, update written size
                        pte->written_so_far = stat_buff.st_size;
                        dbprintf("%s (PID %d) has %zd bytes\n", pte->block_file, pte->proc_id, pte->written_so_far);
                        
                        // if this PTE is dead, then remove it
                        if( pte->is_read_complete ) {
                           running_proc_set.erase(itr);
                        }
                    }
		    unlock_pte(pte);
		}
		else {
		    //File not found in set, remove watch
		    if (inotify_rm_watch(INOTIFY_FD, ievent->wd) < 0) {
                        int errsv = -errno;
			errorf("inotify_rm_watch errno %d\n", errsv);
                    }
		}
	
	
               unlock_pid_map();
               
               //Update the byte_count before leave
               byte_count += INOTIFY_EVENT_SIZE + ievent->len; 
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
    rc = pthread_create(&inotify_event_thread, NULL, inotify_event_receiver, NULL);
    if (rc < 0)
	perror("pthread_create");
}

ProcHandler&  ProcHandler::get_handle(char *cache_dir_str)
{
    static ProcHandler instance = ProcHandler(cache_dir_str);
    return instance;
}


// NOTE: pte must be write-locked
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

   int blk_log_fd = open(file_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); 
   if (blk_log_fd < 0) {
      cerr<<"Cannot open the file: "<<file_path<<endl;
      return -EIO;
   }

   if (dup2(blk_log_fd, STDOUT_FILENO) < 0) {
      perror("dup2");
      exit(-1);
   }
    
    int child_pipe[2];
    int rc = pipe( child_pipe );
    if( rc != 0 ) {
       perror("pipe");
       exit(-1);
    }
    
    pid_t pid = fork();
    if (pid == 0) {
	//Child
       
        // wait until parent records us...
        int go = 0;
        ssize_t len = read( child_pipe[0], &go, sizeof(int) );
        if( len == sizeof(go) ) {
           if( go == 1 ) {
               close(child_pipe[0]);
               close(child_pipe[1]);
               rc = execve(proc_name, argv, envp);
               if (rc < 0) {
                     perror("exec");
                     exit(-1);
               }
           }
           else {
              errorf("Child %s aborted by parent\n", proc_name );
              exit(-1);
           }
        }
        else if( len < 0 ) {
           rc = -errno;
           errorf("read errno %d\n", rc );
           exit(-1);
        }
        exit(-2);
    }
    else if (pid > 0) {
	//Parent
         if (dup2(old_fd, STDOUT_FILENO) < 0) {
               perror("dup2");
         }
         pte->block_file = file_path;
         pte->block_file_wd = -1;
         pte->proc_id = pid;
         pte->is_read_complete = false;
         pte->valid = true;
         
         
         // add this to the proc table 
         lock_proc_table();
         proc_table[string(ctx->file_path)] = pte;
         unlock_proc_table();
         
         // start watching the child 
         lock_pid_map();
         
         running_proc_set.insert(pte);
         
         dbprintf("Start watching child %d\n", pte->proc_id );
   
         pid_map[pte->proc_id] = pte;
         
         int go = 0;
         
         int wd = inotify_add_watch(INOTIFY_FD, pte->block_file, IN_MODIFY | IN_CLOSE_WRITE);
         if( wd >= 0 ) {
            pte->block_file_wd = wd;
            
            // tell the child to go
            go = 1;
         }
         else {
            int errsv = -errno;
            errorf("inofity_add_watch(%s) errno = %d\n", pte->block_file, errsv );
            
            // don't start
            go = 0;
         }
         
         dbprintf("Now watching child %d\n", pte->proc_id );
         unlock_pid_map();
         
         // send the go flag 
         ssize_t len = write( child_pipe[1], &go, sizeof(go) );
         if( len < 0 ) {
            len = -errno;
            errorf("write errno %zd\n", len);
            return -EIO;
         }
         else {
            close( child_pipe[1] );
            close( child_pipe[0] );
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


int ProcHandler::start_command_idempotent( struct shell_ctx *ctx ) {
    dbprintf("Spawn %s\n", ctx->proc_name );
    char const *proc_name = ctx->proc_name;
    char **argv  = ctx->argv;
    char **envp = ctx->envp;
    
    proc_table_entry *pte = NULL;
    
    lock_pid_map();
    
    bool pte_exists = false;
    bool pte_valid = false;
    
    proc_table_t::iterator itr = proc_table.find( string(ctx->file_path) );
    if( itr != proc_table.end() ) {
       pte = itr->second;
       pte_exists = true;
       pte_valid = pte->valid;
       
       if( pte_valid )
          wrlock_pte( pte );
    }
    
    unlock_pid_map();
    
    if( !pte_exists || !pte_valid ) {
        // Process has not yet been started, so kick it off and return EAGAIN
       
        if( !pte_exists ) {
            pte = alloc_proc_table_entry();
            pte_valid = true;
            wrlock_pte(pte);
        }
        
        //check whether this pte is valid
        if(!pte_valid) {
            return -EAGAIN;
        }
        
        // NOTE: pte is write-locked...
        
        // start up the process and insert it into the process table
        int rc = execute_command(proc_name, argv, envp, ctx, pte);
        if (rc < 0) {
            errorf("execute_command(%s) rc = %d\n", proc_name, rc );
            unlock_pte(pte);
            return rc;
        }
        
        dbprintf("Spawned shell child: %d\n", pte->proc_id );
        
        // set up the map_info structure to remember this running process
        ctx->mi->entry = pte;
        ctx->mi->invalidate_entry = invalidate_entry;
        
        unlock_pte(pte);
    }
    else {
       dbprintf("Already running: %s\n", ctx->file_path );
    }
    
    return 0;
}


int ProcHandler::read_command_results(struct shell_ctx *ctx, char *buffer, ssize_t read_size) 
{
    proc_table_entry *pte = NULL;
    
    lock_pid_map();
    
    bool pte_exists = false;
    bool pte_valid = false;
    
    proc_table_t::iterator itr = proc_table.find( string(ctx->file_path) );
    if( itr != proc_table.end() ) {
       pte = itr->second;
       
       pte_exists = true;
       pte_valid = pte->valid;
       
       if( pte_valid ) {
          rdlock_pte( pte );
       }
    }
    
    unlock_pid_map();
    
    if (!pte_exists || !pte_valid) {
        // Process has not yet been started, so kick it off and return EAGAIN
        int rc = start_command_idempotent( ctx );
        if( rc == 0 )
           // tell the reader to try again, now that we're generating data
           return -EAGAIN;
        else 
           return rc;
    }
    else {
        // process is already running.  See if we have data to give back
	
	//check whether this pte is valid
	if (!pte_valid) {
            // start over completely
	    return -ENOTCONN;
	}
	
        // NOTE: pte is read-locked
        
	if (pte->written_so_far > ctx->block_id * ctx->blocking_factor + ctx->data_offset) {
            // have more data
	    off_t current_read_offset = ctx->block_id * ctx->blocking_factor + ctx->data_offset;
            
	    if (ctx->fd < 0) {
		int fd = open(pte->block_file, O_RDONLY);
		if (fd < 0) {
                    int errsv = -errno;
                    errorf("open(%s) errno %d\n", pte->block_file, errsv);
		    unlock_pte(pte);
		    return -EIO;
		}
		ctx->fd = fd;
	    }
	    
	    if (lseek(ctx->fd, current_read_offset, SEEK_SET) < 0) {
		int errsv = -errno;
                errorf("lseek errno %d\n", errsv);
		unlock_pte(pte);
		return -EIO;
	    }
	    // read ALL the data
	    ssize_t num_read = 0;
            
            struct stat sb;
            int rc = fstat( ctx->fd, &sb );
            if( rc < 0 ) {
               rc = -errno;
               errorf("fstat(%s) errno = %d\n", pte->block_file, rc);
            }
            else {
               dbprintf("%s has %zu bytes; we'll read at offset %zd\n", pte->block_file, sb.st_size, current_read_offset );
            }
            
            while( num_read < read_size ) {
               ssize_t nr = read(ctx->fd, buffer + num_read, read_size - num_read);
               if (nr < 0) {
                  int errsv = -errno;
                  errorf("read errno %d\n", errsv);
                  unlock_pte(pte);
                  return -EIO;
               }
               if( nr == 0 ) {
                  // EOF
                  break;
               }
               num_read += nr;
            }
            
            dbprintf("read %zd bytes\n", num_read );
            
            ctx->data_offset += num_read;
            unlock_pte(pte);
            return num_read;
	}
	else { 
           // are we waiting for more data, or are we EOF?
           // if waiting, the EAGAIN.
           if( !pte->is_read_complete ) {
               unlock_pte(pte);
               return -EAGAIN;
           }
           else {
              // on the last block?  if so, then pad
              if( pte->written_so_far > ctx->block_id * ctx->blocking_factor && pte->written_so_far <= (ctx->block_id + 1) * ctx->blocking_factor ) {
                  memset( buffer, 0, read_size );
                  return read_size;
              }
              else {
                  // no data
                 return -ENOENT;
              }
           }
	}
    }
}

ssize_t ProcHandler::encode_results()
{
    return -ENOSYS;
}

proc_table_entry* ProcHandler::alloc_proc_table_entry()
{
    proc_table_entry *pte = CALLOC_LIST( proc_table_entry, 1 );
    pte->valid = true;
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
    blk_stat.no_file = false;
    blk_stat.block_available = false;
    blk_stat.need_padding = false;

    proc_table_entry* pte = NULL;
    
    lock_pid_map();
    
    bool pte_exists = false;
    bool pte_valid = false;
    bool pte_is_read_complete = false;
    
    proc_table_t::iterator itr = proc_table.find( string(ctx->file_path) );
    if( itr != proc_table.end() ) {
       pte = itr->second;
       
       pte_exists = true;
       pte_valid = pte->valid;
       pte_is_read_complete = pte->is_read_complete;
       
       if( pte_valid ) {
          rdlock_pte( pte );
       }
    }
    
    unlock_pid_map();
    
    if (!pte_exists && ctx->file_path == NULL) {
	blk_stat.no_file = true;
    }
    else if (!pte_exists && ctx->file_path != NULL) {
	blk_stat.in_progress = true;
    }
    else {
	if (!pte_valid || !pte_is_read_complete ) {
           // still generating data 
           dbprintf("valid = %d, is_read_complete = %d\n", pte_valid, pte_is_read_complete );
	   blk_stat.in_progress = true;
	}
	
	if( pte_valid ) {
            // NOTE: pte will be read-locked
            
            blk_stat.written_so_far = pte->written_so_far;
           
            // do we have some data?
            if( ctx->block_id * ctx->blocking_factor < pte->written_so_far ) {
               // do we have a complete block?
               if( (ctx->block_id + 1) * ctx->blocking_factor <= pte->written_so_far ) {
                  // whole block
                  blk_stat.block_available = true;
               }
               else {
                  // incomplete block.  If the process is done, then pad the last block 
                  if( pte->is_read_complete ) {
                     blk_stat.need_padding = true;
                     blk_stat.block_available = true;
                  }
                  
                  // otherwise, we're still generating data, but shouldn't serve it yet (since we'll need to encrypt each block)
               }
            }
            
            unlock_pte( pte );
        }
        // otherwise, no data to serve
    }
    return blk_stat;
}

void ProcHandler::remove_proc_table_entry(string file_path) {
    //Lock proc_table
    lock_proc_table();
    proc_table.erase(file_path);
    unlock_proc_table();
}