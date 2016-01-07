#!/bin/bash

# internal use by Jude.  Don't rely on this for anything.
BIN=../../build/out/bin
SYNCONF=$HOME/.syndicate.local

USER_ID="jcnelson@cs.princeton.edu"
HOSTNAME="localhost"
PASS="asdf"
GATEWAY_PASS="poop"

UG_TYPE=1
RG_TYPE=2
AG_TYPE=3

syntool() {
   $BIN/syntool.py -c $SYNCONF/syndicate.conf -P $PASS $@
}

if [ ! -d $SYNCONF ]; then 
   mkdir $SYNCONF
   mkdir -p $SYNCONF/user_keys/
   cp user_test_key.pem $SYNCONF/user_keys/$USER_ID.pkey

   # TODO: have syntool generate this automatically
   echo "[syndicate]
url=http://localhost:8080
username=$USER_ID
gateway=gateways/
volume=volumes/
user=users/" > $SYNCONF/syndicate.conf

   mkdir -p $SYNCONF/gateway_keys
   mkdir -p $SYNCONF/volume_keys
   mkdir -p $SYNCONF/user_keys
fi

syntool delete_gateway AG-genbank-demo || exit 1
syntool delete_gateway UG-genbank-demo || exit 1
syntool delete_gateway RG-genbank-demo || exit 1
syntool delete_gateway UG-genbank-demo-2 || exit 1
syntool delete_volume genbank-demo || exit 1

rm -f $SYNCONF/gateway_keys/AG-genbank-demo.pkey
rm -f $SYNCONF/gateway_keys/UG-genbank-demo.pkey
rm -f $SYNCONF/gateway_keys/RG-genbank-demo.pkey
rm -f $SYNCONF/gateway_keys/UG-genbank-demo-2.pkey
rm -f $SYNCONF/volume_keys/genbank-demo.pkey

# TODO: keyword arguments; gateway caps
syntool create_volume $USER_ID genbank-demo 'test-genbank' 102400 private=False || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID $UG_TYPE UG-genbank-demo $HOSTNAME 33000 || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID $AG_TYPE AG-genbank-demo $HOSTNAME 34000 || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID $RG_TYPE RG-genbank-demo $HOSTNAME 35000 || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID $UG_TYPE UG-genbank-demo-2 $HOSTNAME 36000 || exit 1
