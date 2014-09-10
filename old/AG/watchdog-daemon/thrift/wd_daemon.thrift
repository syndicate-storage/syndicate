/*
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
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

