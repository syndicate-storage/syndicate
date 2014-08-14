#!/bin/sh

# add /syndicate mountpoint...
mkdir -p /syndicate
chmod 0777 /syndicate

# add run and lock dirs...
mkdir -p /var/log/syndicated

# ensure FUSE filesystems can be publicly accessed 
allow_other=$(cat /etc/fuse.conf | grep "^user_allow_other")

if ! [ "$allow_other" ]; then
   echo "Enabling user_allow_other for FUSE filesystems.  Edit your /etc/fuse.conf to revert, if undesired."
   echo "user_allow_other # added by package syndicated-opencloud" >> /etc/fuse.conf
fi

exit 0
