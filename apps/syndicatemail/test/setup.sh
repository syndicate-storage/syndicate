#!/bin/bash

ADMIN_USER_ID="judecn@gmail.com"
BUILD_ROOT=../../../build/out
BIN=$BUILD_ROOT/bin/ms
SYNTOOL=$BIN/syntool.py
SYNCONF=$HOME/.syndicate
HOST=$(hostname)

rm -rf $SYNCONF
mkdir $SYNCONF
mkdir -p $SYNCONF/user_keys/signing/
cp user_test_key.pem $SYNCONF/user_keys/signing/$ADMIN_USER_ID.pkey

echo "[syndicate]
MSAPI=http://localhost:8080/api
user_id=$ADMIN_USER_ID
gateway_keys=gateway_keys/
volume_keys=volume_keys/
user_keys=user_keys/" > $SYNCONF/syndicate.conf

DEFAULT_GATEWAY_CAPS="GATEWAY_CAP_READ_METADATA|GATEWAY_CAP_READ_DATA"

$SYNTOOL -t create_user testuser@gmail.com http://www.vicci.org/id/testuser@gmail.com

# small block size for easy multiblock testing
$SYNTOOL -t -u testuser@gmail.com create_volume testuser@gmail.com mail "SyndicateMail Volume" 122880 metadata_private_key=MAKE_METADATA_KEY private=False default_gateway_caps=$DEFAULT_GATEWAY_CAPS active=True allow_anon=True

#$SYNTOOL -t -u testuser@gmail.com create_gateway mail testuser@gmail.com UG testvolume-UG-1 $HOST 32780
#sleep 1
#$SYNTOOL -t -u testuser@gmail.com set_gateway_caps testvolume-UG-1 ALL

#$SYNTOOL -t -u testuser@gmail.com create_gateway mail testuser@gmail.com UG testvolume-UG-2 $HOST 32781

$SYNTOOL -t -u testuser@gmail.com create_gateway mail testuser@gmail.com RG testvolume-RG-1 $HOST 32800
sleep 1
$SYNTOOL -t -u testuser@gmail.com update_gateway testvolume-RG-1 closure=$BUILD_ROOT/python/syndicate/rg/drivers/disk
