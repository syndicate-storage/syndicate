#!/bin/sh

if [[ $# < 2 ]]; then
	echo "Usage: $0 site mountpoint"
	exit 1
fi

SITE=$1
MOUNT=$2

syndicatefs -f -c syndicate-Internet2-$SITE.conf $MOUNT > /dev/null 2>/dev/null &

./poll-file.py $SITE.coord $MOUNT/hello & 
