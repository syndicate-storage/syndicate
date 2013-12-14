libAGcommon Library
===================

libAGcommon is a library shared by AG drivers.

Functionalities:

  - Provides a generic reversioning daemon that reversions and updates files and file mappings based on timeouts provided by the data curator.
  - Performs diffs on file mappings and volumes and invokes registered callbacks to update, delete and add file mappings and volumes.
  - Provides a controller daemon that listens on control commands. Currently supports TERM and RCON commands to stop and reread file name mappings.

Build Instructions:

    Build:   scons AG-common
    Install: scons AG-common-install

