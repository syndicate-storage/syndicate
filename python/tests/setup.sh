#!/bin/bash

BUILD_ROOT=../../build/out
BIN=$BUILD_ROOT/bin/ms
SYNTOOL=$BIN/syntool.py
SYNCONF=$HOME/.syndicate

USER_ID="judecn@gmail.com"

rm -rf $SYNCONF
mkdir $SYNCONF
mkdir -p $SYNCONF/user_keys/signing/
cp user_test_key.pem $SYNCONF/user_keys/signing/$USER_ID.pkey

echo "[syndicate]
MSAPI=http://localhost:8080/api
user_id=judecn@gmail.com
gateway_keys=gateway_keys/
volume_keys=volume_keys/
user_keys=user_keys/" > $SYNCONF/syndicate.conf

DEFAULT_GATEWAY_CAPS="SG_CAP_READ_METADATA|SG_CAP_READ_DATA"

$SYNTOOL -t create_user testuser@gmail.com http://www.vicci.org/id/testuser@gmail.com

# small block size for easy multiblock testing
$SYNTOOL -t -u testuser@gmail.com create_volume testuser@gmail.com testvolume "Test Volume" 10 metadata_private_key=MAKE_METADATA_KEY private=False default_gateway_caps=$DEFAULT_GATEWAY_CAPS active=True allow_anon=True

$SYNTOOL -t -u testuser@gmail.com create_gateway testvolume testuser@gmail.com UG testvolume-UG-1 localhost 32780
$SYNTOOL -t -u testuser@gmail.com set_gateway_caps testvolume-UG-1 ALL

$SYNTOOL -t -u testuser@gmail.com create_gateway testvolume testuser@gmail.com UG testvolume-UG-2 localhost 32781

$SYNTOOL -t -u testuser@gmail.com create_gateway testvolume testuser@gmail.com RG testvolume-RG-1 localhost 32800
