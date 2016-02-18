#!/bin/bash

source subr.sh

CONFIG_DIR="$(setup)"
CONFIG_PATH="$CONFIG_DIR/syndicate.conf"
RANDOM_PATH="$(mktemp "/tmp/testvolume-XXXXXX")"
rm "$RANDOM_PATH"

echo "config dir: $CONFIG_DIR"

RANDOM_USER_NAME="$(basename "$RANDOM_PATH")@gmail.com"
RANDOM_ADMIN_NAME="$(basename "$RANDOM_PATH")@admin.com"

# random user
$SYNDICATE -c "$CONFIG_PATH" create_user "$RANDOM_USER_NAME" auto max_volumes=20 max_gateways=21
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to create user"
fi

# random admin
$SYNDICATE -c "$CONFIG_PATH" create_user "$RANDOM_ADMIN_NAME" auto max_volumes=20 max_gateways=21 is_admin=True
RC=$?

# read users
USER_JSON="$($SYNDICATE -c "$CONFIG_PATH" list_users)"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to list users"
fi

if [ -z "$(echo "$USER_JSON" | grep "max_volumes" | grep "20")" ]; then 
   test_fail "Failed to read max volumes"
fi

if [ -z "$(echo "$USER_JSON" | grep "max_gateways" | grep "21")" ]; then 
   test_fail "Failed to read max gateways"
fi

# read admin user 
USER_JSON="$($SYNDICATE -c "$CONFIG_PATH" read_user "$RANDOM_ADMIN_NAME")"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to read admin"
fi

if [ -z "$(echo "$USER_JSON" | grep "max_volumes" | grep "20")" ]; then 
   test_fail "Failed to read max volumes"
fi

if [ -z "$(echo "$USER_JSON" | grep "max_gateways" | grep "21")" ]; then 
   test_fail "Failed to read max gateways"
fi

# read user 
USER_JSON="$($SYNDICATE -c "$CONFIG_PATH" read_user "$RANDOM_USER_NAME")"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to read user"
fi

OLD_USER_PUBKEY="$(echo "$USER_JSON" | grep "public_key")"

openssl genrsa 4096 > "$CONFIG_DIR/new-key@gmail.com.pkey"
openssl rsa -pubout < "$CONFIG_DIR/new-key@gmail.com.pkey" > "$CONFIG_DIR/new-key@gmail.com.pub"

# reset the user 
$SYNDICATE -c "$CONFIG_PATH" reset_user "$RANDOM_USER_NAME" "$CONFIG_DIR/new-key@gmail.com.pub"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to reset user"
fi

# re-read user 
USER_JSON="$($SYNDICATE -c "$CONFIG_PATH" read_user "$RANDOM_USER_NAME")"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to re-read user"
fi

NEW_USER_PUBKEY="$(echo "$USER_JSON" | grep "public_key")"

if [ "$OLD_USER_PUBKEY" = "$NEW_USER_PUBKEY" ]; then 
   test_fail "Failed to reset public key"
fi

# clean up 
$SYNDICATE -c "$CONFIG_PATH" delete_user "$RANDOM_USER_NAME"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Delete should have worked"
fi

$SYNDICATE -c "$CONFIG_PATH" delete_user "$RANDOM_ADMIN_NAME"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Delete admin should have worked"
fi

test_success
exit 0
