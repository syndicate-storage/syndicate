#!/bin/sh

MS_TEST=../ms-client-test
VOLUME=testvolume
VOLUME_SECRET=abcdef
UG=testgateway
UG_PASS=sniff

$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c mkdir /this http://www.this.com/ 0755 5000 5000
$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c create /is http://www.this.com/is 0755 5000 5000
$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c create /this/the http://www.this.com/the 0755 5000 5000
$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c create /song http://www.this.com/the 0755 5000 5000
$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c mkdir /this/that http://www.this.com/that 0755 5000 5000
$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c mkdir /this/never http://www.this.com/never 0755 5000 5000
$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c create /ends http://www.this.com/isthesongthatneverends 0755 5000 5000

$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c create /jude http://www.cs.princeton.edu/~jcnelson 0755 5000 5000
$MS_TEST -v $VOLUME -s $VOLUME_SECRET -u $UG -p $UG_PASS -c create /this/llp http://www.cs.princeton.edu/~llp 0755 5000 5000

