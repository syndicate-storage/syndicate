#!/bin/bash

if ! [ $1 ]; then
   echo "Usage: $0 PACKAGE_ROOT"
   exit 1
fi

# ROOT=$HOME/syndicate/python-syndicate-root
ROOT=$1
NAME="python-syndicate"
VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"

DEPS="libssl1.0.0 libcurl3-gnutls libprotobuf7 libsyndicate libsyndicate-ug python-protobuf python-scrypt python-setproctitle python-requests python-psutil python-daemon python-gevent" 

DEPARGS=""
for pkg in $DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

# special version of python-crypto 
#DEPARGS="$DEPARGS -d 'python-crypto >= 2.6'"

source /usr/local/rvm/scripts/rvm

# move site-packages to dist-packages, since this is a .deb 
mv $ROOT/usr/lib/python2.7/site-packages $ROOT/usr/lib/python2.7/dist-packages || exit 1

fpm --force -s dir -t deb -a $(uname -p) -v $VERSION -n $NAME $DEPARGS -d 'python-crypto >= 2.6' -C $ROOT --license "Apache 2.0" --vendor "Princeton University" --maintainer "Jude Nelson <jcnelson@cs.princeton.edu>" --url "https://github.com/jcnelson/syndicate" --description "Syndicate Python library." $(ls $ROOT)

