# syndicate-fs-AG-driver

Syndicate-fs-AG-driver
======================

Syndicate-fs-AG-driver provides a generic filesystem interface to underlying filesystems for AG. The Syndicate-fs-AG-driver finds its plugin that implements the filesystem interface from its configuration.

Plugin Configuration
--------------------

The Syndicate-fs-AG-driver reads its plugin configuration from the Syndicate AG driver configuration file, named "config".

Following is an example of a iPlant-DataStore-plugin configuration.
```
{
   "DATASET_DIR":       "/iplant/home/iychoi/syndicate-AG-dataset",
   "EXEC_FMT":          "/usr/bin/python -m syndicate.ag.gateway",
   "DRIVER":            "syndicate.ag.drivers.fs",
   "DRIVER_FS_BACKEND": "iplant_datastore",
   "DRIVER_FS_BACKEND_CONFIG": 
      {
         "irods":
            {
               "host":  "irods-2.iplantcollaborative.org",
               "port":  1247,
               "zone":  "iplant"
            },
         "bms":
            {
               "host":  "irods-2.iplantcollaborative.org",
               "port":  31333,
               "vhost": "/irods/useraccess"
            }
      }
}
```
