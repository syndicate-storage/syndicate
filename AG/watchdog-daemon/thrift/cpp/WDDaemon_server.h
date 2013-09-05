#ifndef _WDDAEMON_SERVER_H_
#define _WDDAEMON_SERVER_H_

#include "WDDaemon.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

#include <WDDaemon_service_impl.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::watchdog;

class WDDaemonHandler : virtual public WDDaemonIf {
    private:
	WDDaemon_service_impl *wdsi;

    public:
	WDDaemonHandler();

	void pulse(const int32_t id, const std::set<int32_t> & live_set, const std::set<int32_t> & dead_set);

	int32_t register_agd(const  ::watchdog::AGDaemonID& agdid);

	int32_t unregister_agd(const int32_t id);

};

#endif //_WDDAEMON_SERVER_H_

