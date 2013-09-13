/**
 * AGDeamon identifier. Needs when registering an AGDaemon
 * with the Watchdog daemon.
 */

namespace cpp watchdog

struct AGDaemonID {
    1: string		addr,
    2: i32		port,
    3: i16		frequency,
    4: map<i32,string>	ag_map,
}

struct PingResponse {
    1: set<i32> live_set,
    2: set<i32> dead_set,
    3: i32	id,
}

