include "common.thrift"

namespace cpp watchdog

/**
 * This service is provided by the watchdog dameon.
 */
service WDDaemon {
    /**
     * After t seconds call this method.
     * id: 32 bit unique identifier of the ag-daemon.
     * live_set: ag_ids of live AGs.
     * dead_set: ag_ids of dead AGs. ag_id in this set get 
     * removed when AG is restarted.
     */
    void pulse(1:i32 id, 2:set<i32> live_set, 3:set<i32> dead_set), 
    
    /**
     * Register AGDaemon with Watchdog daemon.
     * Returns a 32 bit unique id for the ag-daemon.
     */
    i32 register_agd(1:common.AGDaemonID agdid),

    /**
     * Unregister AGDaemon.
     */
    i32 unregister_agd(1:i32 id),
}

