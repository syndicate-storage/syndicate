= AG Drivers

This directory contains drivers for the AG that allow it to access various dataset sources.

== disk
This driver lets you serve files on local storage.  This is suitable for read-only files.

== disk_polling
This driver lets you serve files on local storage, but will additionally monitor them for changes and keep their size and modification times coherent with Syndicate.

== shell
This driver lets you generate file data by running shell commands and serving back their output.  The driver runs the shell command once, and caches the result until the file is reversioned.

Because the size of the output cannot be known in advance, files exposed by the shell driver will appear to be of maximal size until their data is generated.  Then, once the output's size is
known, the driver keeps it coherent with the rest of Syndicate.

== curl
This driver lets you map file paths to data available by http://, ftp://, and file://.  It uses libcurl to fetch the data (hence the name), and lazily caches block data and metadata to disk while the file is considered fresh by the AG.

This driver is suitable for serving static datasets available via the above protocols.  It is not suitable for data that changes frequently.

== legacy
This directory contains drivers that are currently not supported by the current AG architecture, but have been in the past.
