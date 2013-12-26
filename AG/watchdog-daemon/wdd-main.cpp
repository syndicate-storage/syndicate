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
#include <wdd-main.h>
#include <daemon-config.h>

void  init_watchdog_daemon() {
}

void* start_watchdog_daemon(void* cls) {
    daemon_config *dc = (daemon_config*)cls;
    int port = dc->watchdog_daemon_port;
    shared_ptr<WDDaemonHandler> handler(new WDDaemonHandler());
    shared_ptr<TProcessor> processor(new WDDaemonProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    //We are running a thread pool server.
    int nr_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    syslog(LOG_INFO, "Detected %i CPUs\n", nr_cpus);
    int worker_count = nr_cpus;
    syslog(LOG_INFO, "Thread pool size initialized to %i\n", worker_count);
    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(worker_count);
    shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    syslog(LOG_INFO, "Watchdog daemon starts on port %i\n", port);
    server.serve();
    syslog(LOG_INFO, "Watchdog daemon stopped\n");
    return NULL;
}

int main(int argc, char* argv[]) {
    daemon_config *dc = get_daemon_config("watchdog.conf", NULL);
    pthread_t tid;
    if (pthread_create(&tid, NULL, start_watchdog_daemon, dc) < 0) {
	perror("pthread_create");
	exit(-1);
    }
    if (pthread_join(tid, NULL) < 0) {
	perror("pthread_join");
	exit(-1);
    }
    exit(0);
}

