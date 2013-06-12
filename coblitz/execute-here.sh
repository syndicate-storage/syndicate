#!/bin/sh

NODES="nodelist.txt"
OUTDIR=$1
shift 1

for host in $(cat $NODES); do
   > $OUTDIR/$host.txt 2>&1 ssh -t -t -o StrictHostKeyChecking=no -o ConnectTimeout=60 -o RequestTTY=force princeton_syndicate@$host "sudo python install-experimentd.py" &
   sleep 1
done
