#!/bin/bash

# bootstrap this node with the syndicatefs control daemon

CLEAN=
REPO=
DEST=

usage() {
   echo "Usage: $0 [-c] [-r REPOSITORY] [-d SERVICE_DIR]"
}

cmd() {
   sh -c "$1"
}

while getopts "cr:d:" OPTION; do
   case $OPTION in
      c)
         CLEAN=1
         ;;
      r)
         REPO=$OPTARG
         ;;
      d)
         DEST=$OPTARG
         ;;
      ?)
         usage
         exit 1
         ;;
   esac
done

if ! [[ $DEST ]]; then
   usage
   exit 1
fi

if [[ $CLEAN ]]; then
   if ! [[ $REPO ]]; then
      usage
      exit 1
   fi

   TMPDIR=/tmp/$(basename $DEST)

   cmd "sudo yum -y install subversion pyOpenSSL"
   cmd "sudo rm -rf $TMPDIR $DEST"
   cmd "sudo rm -rf /etc/syndicate"
   cmd "svn checkout $REPO/md-service/ $TMPDIR"
   cmd "sudo mv $TMPDIR $DEST"
   cmd "sudo mkdir -p /etc/syndicate"
   cmd "sudo cp $DEST/etc/syndicate/*.conf $DEST/etc/syndicate/*.template $DEST/etc/syndicate/*.key $DEST/etc/syndicate/*.cert /etc/syndicate/"
   cmd "sudo cp $DEST/etc/init.d/mdctl /etc/init.d/mdctl"
fi

cmd "sudo /etc/init.d/mdctl restart"
