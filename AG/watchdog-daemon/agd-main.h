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
#ifndef _AG_MAIN_H_
#define _AG_MAIN_H_

#include <daemon-config.h>
#include <thrift-common.h>

#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>

#include <protocol/TBinaryProtocol.h>
//#include <server/TSimpleServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include <AGDaemon.h>
#include <AGDaemon_server.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
using namespace  ::watchdog;

typedef struct _pulse_data {
    daemon_config	*dc;
    thrift_connection	*tc; 
    int32_t		id;
} pulse_data;

int init_signal_mask();

int setup_signal_handler(int signum, sighandler_t sighand);

void SIGCHLD_handler(int);

void* generate_pulses(void*);

char** tokenize_command(char *cmd, char **port);

int start_ag(int32_t i, char **cmd);

string get_ag_descriptor(char *host, char* port);

void* run_daemon(void*);

#endif //_AG_MAIN_H_

