#ifndef _EXEC_HANDLER_H_
#define _EXEC_HANDLER_H_

#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/inotify.h>
#include <sys/select.h>
#include <sys/time.h>
#include <signal.h>
#include <sys/wait.h>

#include <string.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <set>

#include <gateway-ctx.h>
#include <map-parser.h>

#define DEFAULT_INIT_PROC_TBL_LEN   1024
#define MAX_FILE_NAME_LEN	    32
#define INOTIFY_EVENT_SIZE	    sizeof(struct inotify_event)
#define INOTIFY_READ_BUFFER_LEN	    ((INOTIFY_EVENT_SIZE + \
	    MAX_FILE_NAME_LEN) * 1024)
#define MAX_BACK_OFF_TIME	    64

using namespace std;

struct _proc_table_entry {
    char    *block_file;
    int	    block_file_wd;
    bool    is_read_complete;
    pid_t   proc_id;
    off_t   current_max_block;
    off_t   block_byte_offset;
    bool    valid;
}; 

struct _block_status { 
    bool    in_progress;
    bool    no_block;
    bool    block_available;
    bool    no_file;
};

typedef struct _proc_table_entry    proc_table_entry;
typedef struct _block_status	    block_status;

struct proc_table_entry_comp {
    bool operator()(proc_table_entry *lproc, proc_table_entry *rproc) {
	return (lproc->block_file_wd < rproc->block_file_wd);
    }
};

void invalidate_entry(void* entry);
void* inotify_event_receiver(void *cls);
void update_death(pid_t pid);
static void sigchld_handler(int signum); 
int  set_sigchld_handler();

class ProcHandler 
{
    private:
	char* cache_dir_path;
	vector<proc_table_entry*> proc_table;
	char* get_random_string();
	pthread_t   inotify_event_thread;

	ProcHandler();
	ProcHandler(char* cache_dir_str);
	ProcHandler(ProcHandler const&);

    public:
	static  ProcHandler&  get_handle(char* cache_dir_str);
	int    execute_command(struct gateway_ctx *ctx, char *buffer, ssize_t read_size); 
	int  execute_command(const char* proc_name, char *argv[], char *evp[], uint id, proc_table_entry *pte); 
	ssize_t	encode_results();
	static proc_table_entry* alloc_proc_table_entry();
	static bool is_proc_alive(pid_t);
	block_status get_block_status(struct gateway_ctx *ctx);
	pthread_t get_thread_id();
};


#endif //_EXEC_HANDLER_H_

