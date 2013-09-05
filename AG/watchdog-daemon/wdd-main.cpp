#include <daemon-config.h>

#include <WDDaemon.h>
#include <WDDaemon_server.h>

#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;
using namespace  ::watchdog;

int main(int argc, char* argv[]) {
    daemon_config *dc = get_daemon_config("watchdog.conf", NULL);
    int port = dc->watchdog_daemon_port;
    shared_ptr<WDDaemonHandler> handler(new WDDaemonHandler());
    shared_ptr<TProcessor> processor(new WDDaemonProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    exit(0);
}

