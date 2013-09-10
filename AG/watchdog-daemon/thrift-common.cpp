#include <thrift-common.h>

thrift_connection* thrift_connect(string addr, int port) {
    thrift_connection *tc = new thrift_connection;
    tc->socket = boost::make_shared<TSocket>(addr, port);
    tc->transport = boost::make_shared<TFramedTransport>(tc->socket);
    tc->protocol = boost::make_shared<TBinaryProtocol>(tc->transport);
    tc->client = new WDDaemonClient(tc->protocol);
    tc->transport->open();
    return tc;
}

void thrift_disconnect(thrift_connection *tc) {
    tc->transport->close();
    tc->socket.reset();
    tc->transport.reset();
    tc->protocol.reset();
    delete tc->client;
    delete tc;
}

