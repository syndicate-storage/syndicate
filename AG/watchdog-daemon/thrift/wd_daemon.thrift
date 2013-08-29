include "common.thrift"

namespace cpp watchdog

/**
 * This service is provided by the watchdog dameon.
 */
service WDDaemon {
    /**
     * After t seconds call this method.
     * live_set: ag_ids of live AGs.
     * dead_set: ag_ids of dead AGs. ag_id in this set get 
     * removed when AG is restarted.
     */
    void pulse(1:set<i32> live_set, 2:set<i32> dead_set), 
    
    /**
     * Register AGDaemon with Watchdog daemon.
     */
    i32 register_agd(1:common.AGDaemonID agdid),
}

