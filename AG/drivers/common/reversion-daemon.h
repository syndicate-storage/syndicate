#ifndef _REVERSION_DAEMON_H_
#define _REVERSION_DAEMON_H_

#define TICK_RATE  3600

#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>

#include <map-parser.h>

void* run_daemon(void *argc);

class ReversionDaemon {
    private:
	bool	    runnable;
	pthread_t   tid;
    public:
	ReversionDaemon();
	void run();
	void stop();
	void add_map_info(struct map_info mi);
};

#endif //_REVERSION_DAEMON_H_
