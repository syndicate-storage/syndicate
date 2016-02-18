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

if [ $RC -ne 0 ]; then 
   test_fail "Failed to create user"
fi

# should fail (duplicate) 
$SYNDICATE -c "$CONFIG_PATH" create_user "$RANDOM_USER_NAME" auto 
RC=$?

if [ $RC -eq 0 ]; then 
   test_fail "Created duplicate user"
fi

# should fail (invalid name)
$SYNDICATE -c "$CONFIG_PATH" create_user "asdf.not.an.email" auto
RC=$?

if [ $RC -eq 0 ]; then 
   test_fail "Created user with invalid name"
fi

# should succeed
openssl genrsa 4096 > "$CONFIG_DIR/$RANDOM_USER_NAME.au.pkey"

$SYNDICATE -c "$CONFIG_PATH" create_user "$RANDOM_USER_NAME".au "$CONFIG_DIR/$RANDOM_USER_NAME.au.pkey"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to create user from existing private key"
fi

# cleanup 
# save the cert info...
SAVE_DIR="$(mktemp -d)"
pushd "$SAVE_DIR"
cp "$CONFIG_DIR/users/"* .
popd

$SYNDICATE -c "$CONFIG_PATH" delete_user "$RANDOM_USER_NAME".au
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to clean up user $RANDOM_USER_NAME.au"
fi

# is it gone?
USER_JSON="$($SYNDICATE -c "$CONFIG_PATH" list_users)"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to list users"
fi

if [ -n "$(echo "$USER_JSON" | grep "$RANDOM_USER_NAME".au)" ]; then 
   test_fail "Failed to actually delete user $RANDOM_USER_NAME.au"
fi

if [ "$(ls -l "$CONFIG_DIR/users/" | grep "$RANDOM_USER_NAME".au | wc -l)" != "0" ]; then 
   test_fail "user config dir $CONFIG_DIR/users still has info for $RANDOM_USER_NAME.au"
fi

# should succeed--idempotent even if we don't have local state
$SYNDICATE -c "$CONFIG_PATH" delete_user "$RANDOM_USER_NAME".au
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Delete by admin should succeed even when when we don't have the key"
fi

# try again, with keys
# should succeed on the MS 
cp "$SAVE_DIR/"* "$CONFIG_DIR/users"
rm -rf "$SAVE_DIR"

$SYNDICATE -c "$CONFIG_PATH" delete_user "$RANDOM_USER_NAME.au"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Delete should be idempotent on the MS"
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

# should be empty now... 
USER_JSON="$($SYNDICATE -c "$CONFIG_PATH" list_users)"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to list users"
fi

if [ -n "$(echo "$USER_JSON" | grep "$RANDOM_USER_NAME")" ]; then 
   test_fail "Failed to actually delete $RANDOM_USER_NAME"
fi

if [ -n "$(echo "$USER_JSON" | grep "$RANDOM_ADMIN_NAME")" ]; then 
   test_fail "Failed to actually delete $RANDOM_ADMIN_NAME"
fi

if [ "$(ls -l "$CONFIG_DIR/users" | grep "$RANDOM_USER_NAME" | wc -l)" != "0" ]; then 
   test_fail "user config dir $CONFIG_DIR/users still has info for $RANDOM_USER_NAME"
fi

if [ "$(ls -l "$CONFIG_DIR/users" | grep "$RANDOM_ADMIN_NAME")" ]; then 
   test_fail "user config dir $CONFIG_DIR/users still has info for $RANDOM_ADMIN_NAME"
fi

test_success
exit 0
