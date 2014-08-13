#!/bin/bash

if ! [ $1 ]; then
   echo "Usage: $0 PACKAGE_ROOT"
   exit 1
fi

# ROOT=$HOME/syndicate/syndicate-MS-clients-root
ROOT=$1
NAME="syndicate-ms-clients"
VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"

DEPS="python-syndicate" 

DEPARGS=""
for pkg in $DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

rm -f $NAME-0*.deb

fpm --force -s dir -t deb -a noarch -v $VERSION -n $NAME $DEPARGS -C $ROOT --license "Apache 2.0" --vendor "Princeton University" --maintainer "Jude Nelson <jcnelson@cs.princeton.edu>" --url "https://github.com/jcnelson/syndicate" --description "Syndicate MS clients." $(ls $ROOT)

