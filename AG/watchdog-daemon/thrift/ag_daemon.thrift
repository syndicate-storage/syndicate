include "common.thrift"

namespace cpp watchdog

service AGDaemon {
    i32 restart(1:i32 ag_id),
    common.PingResponse ping(),
}

