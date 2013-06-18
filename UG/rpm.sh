#!/bin/bash

PKGDIR=/tmp/syndicate-rpm
PKGNAME="syndicate-UG"
PKGVERSION="0.01"
PKGDEPS="fuse curl libxml2 gnutls openssl boost libattr libgcrypt uriparser protobuf libsyndicate"
PKGEXTRA=./planetlab/
PKGARCH=all

source /usr/local/rvm/scripts/rvm

pushd .
cd $PKGEXTRA
PKGFILES=$(find . -type f | grep -v "\.svn")
popd

rm -rf $PKGDIR

DEPARGS=""
for pkg in $PKGDEPS; do
   DEPARGS="$DEPARGS -d $pkg"
done

make install DESTDIR=$PKGDIR

for pkgfile in $PKGFILES; do
   DIR=$PKGDIR/$(dirname $pkgfile)
   mkdir -p $DIR
   cp -a $PKGEXTRA/$pkgfile $DIR/
done

fpm -s dir -t rpm -v $PKGVERSION -n $PKGNAME -a $PKGARCH $DEPARGS -C $PKGDIR $(ls $PKGDIR) 

rm -rf $PKGDIR
