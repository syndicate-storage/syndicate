#ifndef _AG_DAEMON_SERVER_H_
#define _AG_DAEMON_SERVER_H_

#include "AGDaemon.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

#include <AGDaemon_service_impl.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::watchdog;

class AGDaemon_service_impl; //Forward declaration

class AGDaemonHandler : virtual public AGDaemonIf {
    private:
	AGDaemon_service_impl *asi;
    
    public:
	AGDaemonHandler();

	int32_t restart(const int32_t ag_id);

	void ping( ::watchdog::PingResponse& _return);

};

#endif //_AG_DAEMON_SERVER_H_

