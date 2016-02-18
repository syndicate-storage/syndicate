#!/bin/bash

source subr.sh

CONFIG_DIR="$(setup)"

echo "config dir: $CONFIG_DIR"

test -f "$CONFIG_DIR/syndicate.conf" || test_fail "Missing syndicate.conf"
test -d "$CONFIG_DIR/users" || test_fail "Missing users/"
test -d "$CONFIG_DIR/volumes" || test_fail "Missing volumes/"
test -d "$CONFIG_DIR/gateways" || test_fail "Missing gateways/"
test -d "$CONFIG_DIR/syndicate" || test_fail "Missing syndicate/"
test -f "$CONFIG_DIR/users/$SYNDICATE_ADMIN_EMAIL.cert" || test_fail "Missing admin cert"
test -f "$CONFIG_DIR/users/$SYNDICATE_ADMIN_EMAIL.pkey" || test_fail "Missing admin private key"
test -f "$CONFIG_DIR/syndicate/localhost:8080.pub" || test_fail "Missing Syndicate public key"

test_success
exit 0
