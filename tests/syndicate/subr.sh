#!/bin/bash

set -u

MS="./build_root/ms"
BIN="./build_root/bin"

TESTLOGS="/tmp/syndicate-test-logs"
mkdir -p "$TESTLOGS"

SYNDICATE=
SYNDICATE_ADMIN_EMAIL=
SYNDICATE_ADMIN_PRIVKEY=
SYNDICATE_CONFIG=
MS_URL="http://localhost:8080"

TESTNAME="$(basename "$0" | sed 's/\.sh$//g')"
TESTLOG="$TESTLOGS/$TESTNAME.log"

SYNDICATE="$BIN/syndicate"
if ! [ -f "$SYNDICATE" ]; then 
   echo >&2 "No such file or directory: $SYNDICATE"
   exit 1
fi

SYNDICATE_ADMIN_EMAIL_CODE="$(fgrep ADMIN_EMAIL "$MS"/common/admin_info.py)"
SYNDICATE_ADMIN_EMAIL="$(echo "$SYNDICATE_ADMIN_EMAIL_CODE; print ADMIN_EMAIL" | python)"
if [ $? -ne 0 ]; then 
   echo >&2 "Failed to determine admin email"
   exit 1
fi

SYNDICATE_ADMIN_PRIVKEY="$MS"/admin.pem

make_tmp_config_dir() {
   mktemp -d "/tmp/syndicate-test-config.XXXXXX"
}

test_fail() {
   echo "TEST FAILURE: $TESTNAME $@"
   exit 1
}

setup() {
   # does Syndicate's setup with the admin
   # $1: config dir (optional)
   # prints out config dir

   local CONFIG_DIR
   CONFIG_DIR=

   if [ $# -gt 0 ]; then 
       CONFIG_DIR="$1"
    fi

   if [ -z "$CONFIG_DIR" ]; then 
      CONFIG_DIR="$(make_tmp_config_dir)"
   fi

   $SYNDICATE --trust_public_key -c "$CONFIG_DIR/syndicate.conf" --debug setup "$SYNDICATE_ADMIN_EMAIL" "$SYNDICATE_ADMIN_PRIVKEY" "$MS_URL"
   RC=$?

   if [ $RC -ne 0 ]; then 
       test_fail "Failed to set up in $CONFIG_DIR"
   else
       SYNDICATE_CONFIG="$CONFIG_DIR"
       echo "$CONFIG_DIR"
   fi

   return 0
}


test_success() {
   # clean up 
   if [ -n "$SYNDICATE_CONFIG" ] && [ -n "$(echo "$SYNDICATE_CONFIG" | egrep "^/tmp/")" ]; then 
      echo "rm -rf "$SYNDICATE_CONFIG""
   fi

   echo "TEST SUCCESS: $TESTNAME"
   exit 0
}

