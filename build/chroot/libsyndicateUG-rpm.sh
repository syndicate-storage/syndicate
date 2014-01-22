#!/bin/bash

ROOT=$HOME/syndicate/libsyndicateUG-root
NAME="libsyndicateUG"
VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"

DEPS="openssl curl protobuf libsyndicate" 

DEPARGS=""
for pkg in $DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

fpm --force -s dir -t rpm -a x86_64 -v $VERSION -n $NAME $DEPARGS -C $ROOT --license "Apache 2.0" --vendor "Princeton University" --description "Syndicate client library, for creating embedded User Gateways." $(ls $ROOT)

