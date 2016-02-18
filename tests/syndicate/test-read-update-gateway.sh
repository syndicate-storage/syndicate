#!/bin/bash

source subr.sh

CONFIG_DIR="$(setup)"
CONFIG_PATH="$CONFIG_DIR/syndicate.conf"
RANDOM_PATH="$(mktemp "/tmp/testvolume-XXXXXX")"
rm "$RANDOM_PATH"

echo "config dir: $CONFIG_DIR"

RANDOM_VOLUME_NAME="$(basename "$RANDOM_PATH")"
RANDOM_USER_NAME="$(basename "$RANDOM_PATH")"@gmail.com

RANDOM_UG_GATEWAY_NAME="$(basename "$RANDOM_PATH")-UG"
RANDOM_RG_GATEWAY_NAME="$(basename "$RANDOM_PATH")-RG"
RANDOM_AG_GATEWAY_NAME="$(basename "$RANDOM_PATH")-AG"

$SYNDICATE -c "$CONFIG_PATH" create_volume name="$RANDOM_VOLUME_NAME" description="test create_volume" blocksize=4096 email="$SYNDICATE_ADMIN_EMAIL"
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to create volume"
fi

$SYNDICATE -c "$CONFIG_PATH" create_user "$RANDOM_USER_NAME" auto max_volumes=20 max_gateways=21
RC=$?

if [ $RC -ne 0 ]; then 
   test_fail "Failed to create user"
fi

for NAME in $RANDOM_UG_GATEWAY_NAME $RANDOM_RG_GATEWAY_NAME $RANDOM_AG_GATEWAY_NAME; do

   GW_NAME="$NAME-01"
   GW_TYPE="$(echo "$NAME" | sed -r 's/(.*)-([URA]{1}G)$/\2/g')"

   # should succeed
   $SYNDICATE -c "$CONFIG_PATH" create_gateway email="$RANDOM_USER_NAME" volume="$RANDOM_VOLUME_NAME" name="$GW_NAME" private_key=auto type="$GW_TYPE"
   RC=$?

   if [ $RC -ne 0 ]; then 
      test_fail "Failed to create $GW_NAME"
   fi

   # should succeed 
   openssl genrsa 4096 > "$CONFIG_DIR/new_key.pem"
   PUBKEY_STR="$(echo -n "$(cat "$CONFIG_DIR/new_key.pub")" | tr '\n' '@' | sed 's/@/\\\\n/g')"

   $SYNDICATE -c "$CONFIG_PATH" -u "$RANDOM_USER_NAME" update_gateway private_key="$CONFIG_DIR/new_key.pem" host="newhost" port="45678" name="$GW_NAME" 
   RC=$?

   if [ $RC -ne 0 ]; then 
      test_fail "Failed to update $GW_NAME"
   fi

   # check 
   GATEWAY_JSON="$($SYNDICATE -c "$CONFIG_PATH" read_gateway "$GW_NAME")"
   RC=$?

   if [ $RC -ne 0 ]; then 
      test_fail "Failed to read gateway"
   fi

   if [ -z "$(echo "$GATEWAY_JSON" | grep "host" | grep "newhost")" ]; then 
      test_fail "Failed to update host"
   fi

   if [ -z "$(echo "$GATEWAY_JSON" | grep "port" | grep "45678")" ]; then 
      test_fail "Failed to update port"
   fi

   if [ -z "$(echo "$GATEWAY_JSON" | grep "public_key" | grep -- "$PUBKEY_STR")" ]; then 
      test_fail "Failed to update public key"
   fi

done

exit 0
# clean up
for NAME in $RANDOM_UG_GATEWAY_NAME $RANDOM_RG_GATEWAY_NAME $RANDOM_AG_GATEWAY_NAME; do
   
   GW_NAME="$NAME-01"

   # should succeed
   $SYNDICATE -c "$CONFIG_PATH" delete_gateway "$GW_NAME"
   RC=$?

   if [ $RC -ne 0 ]; then 
      test_fail "Failed to delete $GW_NAME"
   fi

   # is it gone?
   GATEWAY_JSON="$($SYNDICATE -c "$CONFIG_PATH" list_gateways)"
   RC=$?

   if [ $RC -ne 0 ]; then 
      test_fail "Failed to list gateways"
   fi

   if [ -n "$(echo "$GATEWAY_JSON" | grep "$GW_NAME")" ]; then 
      test_fail "Failed to actually delete gateway"
   fi
done

# types.conf should be the only file...
if [ "$(ls -l "$CONFIG_DIR/gateways/" | wc -l)" != "2" ]; then
   test_fail "Gateway directory $CONFIG_DIR/gateways not empty"
fi

test_success
exit 0

