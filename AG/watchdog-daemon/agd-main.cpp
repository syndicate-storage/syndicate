#include <daemon-config.h>

#include <WDDaemon.h>
#include <WDDaemon_server.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;
using namespace  ::watchdog;

int main(int argc, char* argv[]) {
    daemon_config *dc = get_daemon_config("watchdog.conf", NULL);
    int	    ad_port = dc->ag_daemon_port;
    string  ad_addr = "127.0.0.1";
    int	    wd_port = dc->watchdog_daemon_port;
    string  wd_addr = dc->watchdog_addr;
    AGDaemonID agdid;
    shared_ptr<TSocket> socket(new TSocket(wd_addr, wd_port));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WDDaemonClient client(protocol);
    transport->open();
    agdid.addr = ad_addr;
    agdid.port = ad_port;
    agdid.frequency = 60;
    int32_t id = client.register_agd(agdid);
    transport->close();
    exit(0);
}

