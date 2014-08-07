#!/bin/bash

ROOT=$HOME/syndicate/syndicate-UG-root
NAME="syndicate-ug"
VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"

DEPS="openssl curl protobuf libsyndicate fuse fuse-libs boost-system boost-thread" 

DEPARGS=""
for pkg in $DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

rm -f $NAME-0*.rpm

fpm --force -s dir -t rpm -a $(uname -p) -v $VERSION -n $NAME $DEPARGS -C $ROOT --license "Apache 2.0" --vendor "Princeton University" --description "Syndicate User Gateway binaries.  Includes syndicatefs, syndicate-httpd, syndicate-ipc." $(ls $ROOT)

