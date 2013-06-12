#!/bin/sh

MDTOOL=../../mdtool
source ./syndicate-metadata-server.conf

valgrind="valgrind --leak-check=full"

$valgrind $MDTOOL -c syndicate-metadata-server.conf remove /path/w00t
$valgrind $MDTOOL -c syndicate-metadata-server.conf remove /path/to/my/file
$valgrind $MDTOOL -c syndicate-metadata-server.conf remove /path/to/my/
$valgrind $MDTOOL -c syndicate-metadata-server.conf remove /path/to/
$valgrind $MDTOOL -c syndicate-metadata-server.conf remove /path/
