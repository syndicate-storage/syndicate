#!/bin/sh

OUTDIR=$1
shift 1

for host in $(cat coblitz.txt); do
   echo $host
   scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 $1 princeton_vcoblitz@$host:/tmp/
done

for host in $(cat coblitz.txt); do
   > $OUTDIR/$host.txt 2>&1 ssh -t -t -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o RequestTTY=force princeton_vcoblitz@$host "python /tmp/$(basename $1)" &
   sleep 1
done
