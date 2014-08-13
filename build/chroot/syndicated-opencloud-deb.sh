#!/bin/bash

if ! [ $1 ]; then
   echo "Usage: $0 PACKAGE_ROOT PACKAGE_SCRIPTS_DIR"
   exit 1
fi

if ! [ $2 ]; then
   echo "Usage: $0 PACKAGE_ROOT PACKAGE_SCRIPTS_DIR"
   exit 1
fi

ROOT=$1
NAME="syndicated-opencloud"
VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"

PRE_INSTALL_SCRIPT=$2/pkg/pre-inst.sh
POST_INSTALL_SCRIPT=$2/pkg/post-inst.sh
PRE_REMOVE_SCRIPT=$2/pkg/pre-rm.sh

DEPS="python-syndicate" 

DEPARGS=""
for pkg in $DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

rm -f $NAME-0*.deb

fpm --force -s dir -t deb -a noarch -v $VERSION -n $NAME $DEPARGS -C $ROOT --license "Apache 2.0" --vendor "Princeton University" --maintainer "Jude Nelson <jcnelson@cs.princeton.edu>" --url "https://github.com/jcnelson/syndicate" --description "Syndicate automount daemon." --before-install $PRE_INSTALL_SCRIPT --after-install $POST_INSTALL_SCRIPT --before-remove $PRE_REMOVE_SCRIPT  $(ls $ROOT)

