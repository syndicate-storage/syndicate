// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "AGDaemon.h"
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

class AGDaemonHandler : virtual public AGDaemonIf {
 public:
  AGDaemonHandler() {
    // Your initialization goes here
  }

  int32_t restart(const int32_t ag_id) {
    // Your implementation goes here
    printf("restart\n");
  }

  void ping() {
    // Your implementation goes here
    printf("ping\n");
  }

};
