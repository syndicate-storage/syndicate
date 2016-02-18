#!/bin/bash

source subr.sh

CONFIG_DIR="$(setup)"
CONFIG_PATH="$CONFIG_DIR/syndicate.conf"
RANDOM_PATH="$(mktemp "/tmp/testvolume-XXXXXX")"
rm "$RANDOM_PATH"

echo "config dir: $CONFIG_DIR"

RANDOM_VOLUME_NAME="$(basename "$RANDOM_PATH")"

$SYNDICATE -c "$CONFIG_PATH" create_volume name="$RANDOM_VOLUME_NAME" description="test create_volume" blocksize=4096 email="$SYNDICATE_ADMIN_EMAIL"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to create volume"
fi

$SYNDICATE -c "$CONFIG_PATH" update_volume "$RANDOM_VOLUME_NAME" description="foo" private=False file_quota=123456
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to update volume"
fi

VOLUME_JSON="$($SYNDICATE -c "$CONFIG_PATH" list_volumes)"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to list volumes"
fi

if [ -z "$(echo "$VOLUME_JSON" | grep "description" | grep "foo")" ]; then 
   test_fail "Failed to set volume description"
fi

if [ -z "$(echo "$VOLUME_JSON" | grep "private" | grep "False")" ]; then 
   test_fail "Failed to set volume privacy"
fi

if [ -z "$(echo "$VOLUME_JSON" | grep "file_quota" | grep "123456")" ]; then 
   test_fail "Failed to set volume file quota"
fi

VOLUME_JSON="$($SYNDICATE -c "$CONFIG_PATH" read_volume "$RANDOM_VOLUME_NAME")"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to list volumes"
fi

if [ -z "$(echo "$VOLUME_JSON" | grep "description" | grep "foo")" ]; then 
   test_fail "Failed to set volume description"
fi

if [ -z "$(echo "$VOLUME_JSON" | grep "private" | grep "False")" ]; then 
   test_fail "Failed to set volume privacy"
fi

if [ -z "$(echo "$VOLUME_JSON" | grep "file_quota" | grep "123456")" ]; then 
   test_fail "Failed to set volume file quota"
fi


# cleanup 
$SYNDICATE -c "$CONFIG_PATH" delete_volume "$RANDOM_VOLUME_NAME"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to clean up volume $RANDOM_VOLUME_NAME"
fi

test_success
exit 0
