#include <thrift-common.h>

thrift_connection* thrift_connect(string addr, int port, bool is_wd) {
    thrift_connection *tc = new thrift_connection;
    tc->socket = boost::make_shared<TSocket>(addr, port);
    tc->transport = boost::make_shared<TFramedTransport>(tc->socket);
    tc->protocol = boost::make_shared<TBinaryProtocol>(tc->transport);
    tc->err = NULL;
    tc->is_connected = true;
    if (is_wd) {
	tc->wd_client = new WDDaemonClient(tc->protocol);
	tc->ag_client = NULL;
    }
    else {
	tc->ag_client = new AGDaemonClient(tc->protocol);
	tc->wd_client = NULL;
    }
    try {
	tc->transport->open();
    }
    catch (TException &tx) {
	tc->err = strdup(tx.what());
	tc->is_connected = false;
    }
    return tc;
}

void thrift_disconnect(thrift_connection *tc) {
    tc->transport->close();
    tc->socket.reset();
    tc->transport.reset();
    tc->protocol.reset();
    if (tc->wd_client)
	delete tc->wd_client;
    if (tc->ag_client)
	delete tc->ag_client;
    if (tc->err)
	free(tc->err);
    delete tc;
}

