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

DEFAULT_GATEWAY_CAPS="GATEWAY_CAP_READ_METADATA|GATEWAY_CAP_WRITE_METADATA|GATEWAY_CAP_READ_DATA|GATEWAY_CAP_WRITE_DATA|GATEWAY_CAP_COORDINATE"

$SYNTOOL -t create_user testuser@gmail.com http://www.vicci.org/id/testuser@gmail.com

# small block size for easy multiblock testing
$SYNTOOL -t -u testuser@gmail.com create_volume testuser@gmail.com testvolume "Test Volume" 10 metadata_private_key=MAKE_METADATA_KEY private=False default_gateway_caps=$DEFAULT_GATEWAY_CAPS active=True

$SYNTOOL -t -u testuser@gmail.com create_gateway testvolume testuser@gmail.com UG testvolume-UG-1 localhost 32780

$SYNTOOL -t -u testuser@gmail.com create_gateway testvolume testuser@gmail.com RG testvolume-RG-1 localhost 32800
