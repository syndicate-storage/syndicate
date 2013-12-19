#!/bin/bash

BIN=../../bin/ms
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

