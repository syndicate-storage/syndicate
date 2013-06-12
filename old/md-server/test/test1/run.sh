#!/bin/sh

MDTOOL=../../mdtool
source ./syndicate-metadata-server.conf

rm -rf $MDROOT

valgrind="valgrind --leak-check=full"

$valgrind $MDTOOL -c syndicate-metadata-server.conf add /path/ http://localhost:8888/path/ 12345 0755 4096 1328176504
$valgrind $MDTOOL -c syndicate-metadata-server.conf add /path/to/ http://localhost:8888/path/to/ 12345 0755 4096 1328176504
$valgrind $MDTOOL -c syndicate-metadata-server.conf add /path/to/my/ http://localhost:8888/path/to/my/ 12345 0755 4096 1328176504
$valgrind $MDTOOL -c syndicate-metadata-server.conf add /path/to/my/file http://localhost:8888/path/to/my/file 12345 0755 4096 1328176504

$valgrind $MDTOOL -c syndicate-metadata-server.conf add /path/w00t http://localhost:8888/path/w00t 12345 0755 0 1328176504

