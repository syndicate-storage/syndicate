#!/bin/bash

if ! [ $1 ]; then
   echo "Usage: $0 PACKAGE_ROOT"
   exit 1
fi

# ROOT=$HOME/syndicate/python-syndicate-root
ROOT=$1
NAME="python-syndicate"
VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"

DEPS="libssl1.0.0 libcurl3-gnutls libprotobuf7 libsyndicate libsyndicate-ug python-protobuf python-crypto python-scrypt python-setproctitle python-requests python-psutil python-daemon" 

DEPARGS=""
for pkg in $DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

rm -f $NAME-0*.deb

fpm --force -s dir -t deb -a $(uname -p) -v $VERSION -n $NAME $DEPARGS -C $ROOT --license "Apache 2.0" --vendor "Princeton University" --description "Syndicate Python library." $(ls $ROOT)

