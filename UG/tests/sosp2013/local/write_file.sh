#!/bin/sh

NUM_TESTS=$1

MS=localhost:8080
VOLUME=testvolume
VOLSECRET=abcdef
USER=UG-localhost
PASS=sniff

WRITE_FILE=../write_file
CONFS=confs/write_file/
CONF_BASE=$CONFS/write_file.conf

OUTPUT=output/

mkdir -p $OUTPUT
rm -rf /tmp/syndicate-test/*

# generate config files
make -C $CONFS clean
make -C $CONFS NUM_CONFS=$NUM_TESTS

for i in $(seq 0 $NUM_TESTS); do
   $WRITE_FILE -N -n -E test-$i -v $VOLUME -s $VOLSECRET -u $USER -P $PASS -m $MS -c $CONF_BASE.$i > $OUTPUT/write_file.out.$i 2>&1 &
done

