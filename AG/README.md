Acquisition Gateway
==================
Acquisition Gateways (AGs) can map file names to various entities such as SQL queries, shell commands etc. and publish them in a Syndicate volume. Currently AG supports three types of mappings, filename to filename, filename to SQL query and file name to shell command. These functionalities are implemented as separate drivers.

Starting the gateway.
---------------------
The command below starts the AG with a minimum number arguments.

    ./AG -m <MS-URL> -g <AG-Name> -u <MS-username> -p <MS-password> -v <Volume-Name> -f -d </path/to/mapping/xml> -D <AG-driver.so> -P <Port>

Rereading the mapping.
----------------------
After adding/removing/updating mapping XML you can reload without restarting the server.
For more information on mappings read 'Mapping XML' sections in README.mdof drivers.

    ./AG -r <PID> -D <AG-driver.so>

Stopping the gateway.
---------------------
Stop the AG smoothly.

    ./AG -t <PID> -D <AG-driver.so>


