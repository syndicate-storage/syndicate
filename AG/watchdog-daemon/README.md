AG Watchdog Daemon
==================

Comprises of two sub systems, the AG daemons and a watchdog daemon.

- AG daemon will execute a set of AGs and send heartbeat signals over HTTP to the watchdog daemon until it receives SIGCHLDs. 
- Upon reception of a SIGCHLD AG daemon will notify the event to the watchdog daemon.
- When watchdog daemon is notified about the death of an AG or loses pulses from an AG it will notify the administrator and act as specified in its configuration file (informing AG daemon to start the AG or requesting for human intervention via notification to the administrator).


Cardinality between watchdog daemon and the AG daemon is 1 to n.

			+-----------+ (n)      Apache Thrift (binary)	(1) +-----------------+
			| AG Daemon |<------------------------------------->| Watchdog Daemon |
			+-----------+                                       +-----------------+
			/           \
	       /             \
	      /               \
	+----------+     +------------+
	| AG - SQL |     | AG - shell |
	+----------+     +------------+

NOTE: Both AG and Watchdog daemons should adhere to correct daemon behaviour according to W. Richard Stevens.


