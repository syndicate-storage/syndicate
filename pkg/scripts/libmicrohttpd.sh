#!/bin/bash

# usage: libmicrohttpd.sh BUILD_ROOT PKG_OUTPUT_DIR
# both paths must be absolute paths to directories.

PKG_URL=ftp://ftp.gnu.org/gnu/libmicrohttpd/libmicrohttpd-0.9.27.tar.gz
PKG_ROOT=$1
PKG_PKGOUT=$2
PKG_ARCHIVE=libmicrohttpd.tar.gz
PKG_BUILD_ROOT=$PKG_ROOT/$(basename $PKG_URL | sed 's/\.tar\.gz//g')
PKG_VERSION=$(basename $PKG_BUILD_ROOT | sed 's/libmicrohttpd-//g')
PKG_INSTALL=$PKG_ROOT/out/
PKG_DEPS="gnutls libgcrypt libgpg-error libtasn1 libz"

DEPARGS=""
for pkg in $PKGDEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

source /usr/local/rvm/scripts/rvm

mkdir -p $PKG_BUILD_ROOT
mkdir -p $PKG_INSTALL

pushd $PKG_ROOT
wget $PKG_URL -O $PKG_ARCHIVE || exit 1
tar xvf $PKG_ARCHIVE
popd

pushd $PKG_BUILD_ROOT
./configure --prefix=$PKG_INSTALL/usr/local || exit 1
make || exit 1
make install || exit 1
popd


fpm -s dir -t rpm -v $PKG_VERSION -n libmicrohttpd $DEPARGS -C $PKG_INSTALL -p $PKG_PKGOUT --license LGPL --vendor PlanetLab --description "libmicrohttpd build compatible with Syndicate" $(ls $PKG_INSTALL)
