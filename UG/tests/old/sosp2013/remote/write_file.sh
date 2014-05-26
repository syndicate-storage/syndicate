#!/bin/sh

sudo curl http://vcoblitz-cmi.cs.princeton.edu/write_file.conf > /tmp/write_file.conf
sudo curl http://vcoblitz-cmi.cs.princeton.edu/i386/write_file > /tmp/write_file
sudo chmod +x /tmp/write_file
sudo /tmp/write_file -n -v testvolume -s abcdef -u UG-`hostname` -P sniff -m https://syndicate-metadata.appspot.com/ -c /tmp/write_file.conf
