#include <pthread.h>

#include <daemon-config.h>
#include <thrift-common.h>
#include <agd-main.h>

#include <set>
#include <sstream>

set<int32_t> live_set;
set<int32_t> dead_set;
set<char**>  cmd_tok_set;

int mask_all_signals() {
    sigset_t set;
    int err;
    if ((err = sigfillset(&set)) < 0)
	return err;
    if ((err = sigprocmask(SIG_BLOCK, &set, NULL)) < 0)
	return err;
    return 0;
}

int unmask_signal(int signum, sighandler_t sighand) {
    int err;
    struct sigaction sa;
    sa.sa_handler = sighand;
    sa.sa_flags = SA_NOCLDSTOP;
    if ((err = sigfillset(&(sa.sa_mask))) <  0 )
	return err;
    if ((err = sigdelset(&(sa.sa_mask), SIGCHLD)) < 0)
	return err;
    sa.sa_restorer = NULL;
    sa.sa_sigaction = NULL;
    if ((err = sigaction(signum, &sa, NULL)) < 0)
	return err;
    return 0;
}

void SIGCHLD_handler(int) {
    cout<<"Child Died!"<<endl;
}

void* generate_pulses(void *cls) {
    if (cls == NULL)
	return NULL;
    pulse_data *pd = (pulse_data*)cls;
    struct timespec request;
    struct timespec remain;
    daemon_config *dc	    = pd->dc;
    thrift_connection *tc   = pd->tc;
    int32_t id		    = pd->id;
    while (1) {
	request.tv_sec = 10;
	request.tv_nsec = 0;
	remain.tv_sec = 0;
	remain.tv_nsec = 0;
	int rc = clock_nanosleep(CLOCK_MONOTONIC, 0,
				    &request, &remain);
	if (rc < 0)
	    perror("clock_nanosleep");
	//Send the pulse...
	tc->client->pulse(id, live_set, dead_set);
    }
    return NULL;
}

char** tokenize_command(char *cmd, char **port) {
    if (cmd == NULL)
	return NULL;
    char *save_ptr = NULL;
    char *token = NULL;
    char **token_array = NULL;
    int  token_count = 0;
    while ((token = strtok_r(cmd, " ", &save_ptr)) != NULL) {
	if (token_count == 0)
	    cmd = NULL;
	token_array = (char**)realloc(token_array, sizeof(char*) * (++token_count));
	size_t tok_len = strlen(token);
	token_array[token_count - 1] = (char*)malloc(tok_len + 1);
        memcpy(token_array[token_count - 1], token, tok_len);
	token_array[token_count - 1][tok_len] = '\0'; 	
	if (!strcmp(token, "-P")) {
	    size_t port_len = strlen(save_ptr);
	    *port = (char*)malloc(port_len + 1);
	    memset(*port, 0, port_len + 1);
	    memcpy(*port, save_ptr, port_len);
	}
    }
    //NULL terminater token array.
    token_array = (char**)realloc(token_array, sizeof(char*) * (++token_count));
    token_array[token_count - 1] = NULL;
    return token_array;
}

int start_ag(char **cmd) {
    pid_t pid = fork();
    if (pid == 0) {
	int rc = execve(cmd[0], cmd, NULL);
	if (rc < 0) {
	    perror("exec");
	    exit(-1);
	}
    }
    else if (pid > 0) {
	return 0;
    }
    else {
	perror("fork");
	return -1;
    }
}

string get_ag_descriptor(char *host, char* port) {
    if (host == NULL || port == NULL)
	return NULL;
    stringstream host_port;
    host_port<<host<<":"<<port;
    string host_port_str = host_port.str();
    return host_port_str;
}

int main(int argc, char* argv[]) {
    //Mask all signals
    mask_all_signals();
    //Read configuration
    daemon_config *dc = get_daemon_config("watchdog.conf", NULL);
    int	    ad_port = dc->ag_daemon_port;
    string  ad_addr = "127.0.0.1";
    int	    wd_port = dc->watchdog_daemon_port;
    string  wd_addr = dc->watchdog_addr;
    AGDaemonID agdid;
    pthread_t tid;

    //Find the host name...
    size_t _host_len = 1000;
    char* _host = (char*)malloc(_host_len);
    memset(_host, 0, _host_len);
    if (gethostname(_host, _host_len) < 0) {
	perror("gethostname");
	exit(-1);
    }

    //Unmask SIGCHLD
    unmask_signal(SIGCHLD, SIGCHLD_handler);

    agdid.addr = ad_addr;
    agdid.port = ad_port;
    agdid.frequency = 60;
    //Pack ag_list 
    int	    ag_list_len = dc->ag_list.size();
    int	    i;
    for (i=0; i<ag_list_len; i++) {
	size_t cmd_len = strlen(dc->ag_list[i].c_str());
	char *cmd = (char*)malloc(cmd_len + 1);
	memset(cmd, 0, cmd_len + 1);
	memcpy(cmd, dc->ag_list[i].c_str(), cmd_len);
	char* _port = NULL;
	char** cmd_toks = tokenize_command(cmd, &_port);
	cmd_tok_set.insert(cmd_toks);
	agdid.ag_map[i] = get_ag_descriptor(_host, _port);
	start_ag(cmd_toks);
	free(_port);
	_port = NULL;
	free(cmd);
	cmd = NULL;
    }
    thrift_connection *tc = thrift_connect(wd_addr, wd_port);
    int32_t id = tc->client->register_agd(agdid);
    pulse_data *pd = new pulse_data;
    pd->dc = dc;
    pd->tc = tc;
    pd->id = id;
    if (pthread_create(&tid, NULL, generate_pulses, pd) < 0)
	perror("pthread_create");
    pthread_join(tid, NULL);
    thrift_disconnect(tc);
    exit(0);
}

