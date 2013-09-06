#ifndef _WDD_MAIN_H_
#define _WDD_MAIN_H_

#include <pthread.h>

#include <protocol/TBinaryProtocol.h>
//#include <server/TSimpleServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include <WDDaemon.h>
#include <WDDaemon_server.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
using namespace  ::watchdog;

void* start_watchdog_daemon(void *cls);

#endif //_WDD_MAIN_H_

