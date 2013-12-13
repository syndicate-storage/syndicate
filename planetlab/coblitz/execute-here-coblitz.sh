#!/bin/sh

NODES="coblitz.txt"
OUTDIR=$1
shift 1

for host in $(cat $NODES); do
   > $OUTDIR/$host.txt 2>&1 ssh -t -t -o StrictHostKeyChecking=no -o ConnectTimeout=2 -o RequestTTY=force princeton_vcoblitz@$host "/etc/cron.hourly/squid.cron" &
   sleep 1
done
