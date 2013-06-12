#!/bin/sh

NODES="nodelist.txt"
OUTDIR=$1
shift 1

for host in $(cat $NODES); do
   echo $host
   scp -o StrictHostKeyChecking=no -o ConnectTimeout=30 $1 princeton_syndicate@$host:~/ &
done

