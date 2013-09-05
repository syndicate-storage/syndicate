/**
 * AGDeamon identifier. Needs when registering an AGDaemon
 * with the Watchdog daemon.
 */

namespace cpp watchdog

struct AGDaemonID {
    1: string addr,
    2: i32     port,
    3: i16    frequency;
}

