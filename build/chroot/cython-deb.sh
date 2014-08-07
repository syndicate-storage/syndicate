#!/bin/bash

ROOT=$HOME/cython/cython-root
NAME="cython0.19"
VERSION="0.19.2"

DEPS="python python-support libc6" 

DEPARGS=""
for pkg in $DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

rm -f $NAME-0*.deb

fpm --force -s dir -t deb -a $(uname -p) -v $VERSION -n $NAME $DEPARGS -C $ROOT --license "Apache 2.0" --vendor "Princeton University" --description "Cython 0.19.x for Ubuntu 12.04" $(ls $ROOT)

