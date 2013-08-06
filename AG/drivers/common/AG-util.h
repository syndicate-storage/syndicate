#ifndef _UTIL_H_
#define _UTIL_H_

#include <iostream>

#include <sys/types.h>
#include <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

using namespace std;

//sighand set
struct _sighand_struct{
    sighandler_t term_handler;
    sighandler_t init_handler;
    _sighand_struct():term_handler(NULL), init_handler(NULL){}
};


//Delete all the files in a given directory
void clean_dir(const char *dir_name);

//Register signal handler with sighands
sighandler_t add_signal_handler(int signum, sighandler_t handler);

//Remove signal handler sighands
sighandler_t remove_signal_handler(int signum);

#endif //_UTIL_H_

