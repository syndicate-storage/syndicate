#!/bin/bash

# usage: libmicrohttpd.sh BUILD_ROOT PKG_OUTPUT_DIR GIT_ROOT
# both paths must be absolute paths to directories.

PKG_ROOT=$1
PKG_PKGOUT=$2
PKG_REPO=$3
PKG_NAME="libsyndicate"
PKG_VERSION="0.$(date +%Y\%m\%d\%H\%M\%S)"
PKG_BUILD=$PKG_ROOT/out/libsyndicate
PKG_INSTALL=$PKG_ROOT/out/libsyndicate/usr/

PKG_DEPS="openssl curl uriparser protobuf" 

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
scons -c libsyndicate-install DESTDIR=$PKG_INSTALL
scons -c libsyndicate
scons libsyndicate
scons libsyndicate-install DESTDIR=$PKG_INSTALL
popd

fpm --force -s dir -t rpm -v $PKG_VERSION -n $PKG_NAME $DEPARGS -d "libmicrohttpd > 0.9" -C $PKG_BUILD -p $PKG_PKGOUT --license proprietary --vendor PlanetLab --description "Syndicate common libraries and headers" $(ls $PKG_BUILD)

