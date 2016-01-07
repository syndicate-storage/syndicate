#!/bin/bash -e

BIN=../../build/out/bin/
SYNTOOL=$BIN/syntool.py
SYNCONF=$HOME/.syndicate

USER_ID="judecn@gmail.com"
HOSTNAME="t510"

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

$SYNTOOL create_user testuser@gmail.com http://www.vicci.org/id/testuser@gmail.com letmein

$SYNTOOL register_account testuser@gmail.com letmein

$SYNTOOL -u testuser@gmail.com create_volume testuser@gmail.com testvolume "Test Volume" 61440 default_gateway_caps=ALL

$SYNTOOL -u testuser@gmail.com create_gateway testvolume testuser@gmail.com UG testvolume-UG-1 t510 32780 
$SYNTOOL -u testuser@gmail.com create_gateway testvolume testuser@gmail.com UG testvolume-UG-2 t510 32781

$SYNTOOL -u testuser@gmail.com create_gateway testvolume testuser@gmail.com RG testvolume-RG-1 t510 32800
