#!/bin/bash

PKG_ROOT=$1
PKG_PKGOUT=$2
PKG_REPO=$3
PKG_NAME="syndicate-UG"
PKG_VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"
PKG_BUILD=$PKG_ROOT/out/syndicate-UG
PKG_INSTALL=$PKG_ROOT/out/syndicate-UG/usr/local

PKG_DEPS="openssl curl uriparser protobuf fuse libsyndicate" 

DEPARGS=""
for pkg in $PKG_DEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

rm -rf $PKG_BUILD
rm -rf $PKG_INSTALL
mkdir -p $PKG_BUILD
mkdir -p $PKG_INSTALL

pushd $PKG_REPO
scons -c UG-install DESTDIR=$PKG_INSTALL
scons -c UG
scons UG
scons UG-install DESTDIR=$PKG_INSTALL
popd

fpm --force -s dir -t rpm -v $PKG_VERSION -n $PKG_NAME $DEPARGS -d "libmicrohttpd > 0.9" -C $PKG_BUILD -p $PKG_PKGOUT --license "Apache 2.0" --vendor "PlanetLab Consortium" --description "Syndicate User Gateways" $(ls $PKG_BUILD)

