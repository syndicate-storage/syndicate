#!/bin/bash

# internal use by Jude.  Don't rely on this for anything.

BIN=../../build/out/bin/ms
SYNCONF=$HOME/.syndicate.local

USER_ID="jcnelson@cs.princeton.edu"
HOSTNAME="localhost"
PASS="asdf"
GATEWAY_PASS="poop"

syntool() {
   $BIN/syntool.py -c $SYNCONF/syndicate.conf -P $PASS $@
}

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

syntool create_volume $USER_ID genbank-demo 'test-genbank' 102400 private=False default_gateway_caps=ALL || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID UG UG-genbank-demo $HOSTNAME 33000 || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID AG AG-genbank-demo $HOSTNAME 34000 || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID RG RG-genbank-demo $HOSTNAME 35000 || exit 1
syntool -g $GATEWAY_PASS create_gateway genbank-demo $USER_ID UG UG-genbank-demo-2 $HOSTNAME 36000 || exit 1
