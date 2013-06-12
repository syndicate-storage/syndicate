#!/bin/sh

# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

NODES="nodelist.txt"
OUTDIR=$1
shift 1

for host in $(cat $NODES); do
   echo $host
   scp -o StrictHostKeyChecking=no -o ConnectTimeout=30 $1 princeton_syndicate@$host:~/ 
done

for host in $(cat $NODES); do
   > $OUTDIR/$host.txt 2>&1 ssh -t -t -o StrictHostKeyChecking=no -o ConnectTimeout=30 -o RequestTTY=force princeton_syndicate@$host "python $(basename $1)" &
   sleep 1
done
