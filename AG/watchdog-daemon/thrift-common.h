#ifndef _THRIFT_COMMON_H_
#define _THRIFT_COMMON_H_

#include <WDDaemon.h>
#include <WDDaemon_server.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>

#include <boost/make_shared.hpp>

#include <string>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;
using namespace  ::watchdog;
using namespace std;

typedef struct _thrift_connection {
    shared_ptr<TSocket>		socket;
    shared_ptr<TTransport>	transport;
    shared_ptr<TProtocol>	protocol;
    WDDaemonClient		*client;
} thrift_connection;

thrift_connection* thrift_connect(string addr, int port);

void thrift_disconnect(thrift_connection *tc);

#endif //_AG_MAIN_H_

